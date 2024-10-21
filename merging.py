import pandas as pd
import re

def mergeLoyality(df_exchange :pd.DataFrame, df_forum:tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame], df_airlines:pd.DataFrame):
    """
    Returns: 
        (tuple): три таблицы: кто и куда летел, кто использовал чужую программу лояльности, все люди которые есть в базе и их индификаторы b и бонусные программы
    """
    #синтезируем датафреим состоящий из uid(уникальный индификатор человека), FFKey(код и номер бонусной программы), NickName(ник пользвателя на сайте)
    a = df_exchange['FFKey'].drop_duplicates()
    c = df_airlines[['card_number', 'uid']].drop_duplicates()
    a_c = pd.merge(a,c, left_on='FFKey', right_on='card_number', how='outer', copy=False)
    a_c['FFKey'] = a_c['FFKey'].fillna(a_c['card_number'])
    idFKey_df = a_c[['uid', 'FFKey']].copy()
    #ищем людей владеющих одной и той же бонусной программой
    bad = idFKey_df[idFKey_df.duplicated(subset='FFKey')]
    idFKey_df.drop_duplicates(subset='FFKey', inplace=True)
    user_id = df_forum[2][['NickName', 'programm', 'Number']]
    user_id['programm'] = user_id['programm']+' '+user_id['Number'].apply(str)
    user_id = user_id[['NickName', 'programm']]
    idFKeyNick_df = pd.merge(idFKey_df, user_id, left_on='FFKey', right_on='programm', how = 'outer')
    idFKeyNick_df['FFKey']= idFKeyNick_df['FFKey'].fillna(idFKeyNick_df['programm'])
    idFKeyNick_df = idFKeyNick_df[['uid','FFKey','NickName']]
    #удаляем все записи о людях ползующихся одной программой оставляя только одного владельца для дублирующейся бонусной программы
    idFKeyNick_df = idFKeyNick_df[~idFKeyNick_df['uid'].isin(bad['uid'])]
    #начинаем создавать таблицу всех перелетов для всех людей по-очередно для каждого файла сохраняя эти таблицы в am_df, bm_df, cm_df соотвественно
    am_df = pd.merge(idFKeyNick_df, df_exchange, on='FFKey', how='outer')
    df_airlines.rename(columns={'card_number':'FFKey'}, inplace=True)
    cm_df = pd.merge(idFKeyNick_df, df_airlines, on = ['uid','FFKey'], how='outer')
    uidNick = idFKeyNick_df[['uid','NickName']].drop_duplicates()
    bm_df = pd.merge(uidNick, df_forum[1], on='NickName', how='outer')
    #удаляем из каждой таблицы все тех людей о которых мы не получили данные о перелетах
    am_df.dropna(subset='Fare', inplace=True)
    bm_df.dropna(subset='Flight', inplace=True)
    cm_df.dropna(subset='fare', inplace=True) 
    #приводим все таблицы к единому формату
    am_df = am_df[['uid', 'FFKey','NickName', 'Date', 'FlightNumber', 'From', 'To', 'Fare']]
    am_df.rename(columns={'FlightNumber':'Flight'}, inplace=True)
    bm_df = bm_df[['uid', 'NickName', 'Date', 'Flight', 'Departure.Airport','Arrival.Airport']].copy()
    bm_df.rename(columns={'Arrival.Airport':'To', 'Departure.Airport':'From'}, inplace=True)
    cm_df = cm_df[['uid', 'FFKey', 'NickName', 'date', 'code', 'departure', 'arrival', 'fare']]
    cm_df.rename(columns={'date':'Date', 'code':'Flight', 'departure':'From', 'arrival':'To', 'fare':'Fare'}, inplace=True)
    #объеденяем все таблицы в одну - итоговую
    res = pd.concat([am_df, bm_df, cm_df], axis=0)
    bm_df[bm_df['NickName'] == 'FrequentFlyer98708'].sort_values('Date')
    res.drop_duplicates(subset=['uid','FFKey', 'Date',	'Flight',	'From',	'To',	'Fare'], inplace= True)
    return (res, bad, idFKeyNick_df)

def mergeDataPasports(df_sirena :pd.DataFrame , df_boarding: pd.DataFrame):
    """
    Объединение базовых данных по паспорту, дате, номеру рейса
    Args:
        df_sirena (pd.DataFrame): DaraFrame из Sirena-export-fixed.tab
        df_boarding (pd.DataFrame): DaraFrame из BoardingData.csv
        timetable (pd.DataFrame): DaraFrame из Skyteam_Timetable.pdf
    Returns:
        (pd.DataFrame, pd.DataFrame): Данные: ['PassengerDocument, 'FlightNumber', 'FlightDate','FlightTime','From','Dest', 'TicketNumber'] и таблица соответствий загранников и обычных паспортов
    """
    # Выделяем нужные данные
    df_sirena_d = df_sirena[['TravelDoc', 'e-Ticket','Flight','DepartDate', 'DepartTime', 'From', 'Dest', 'PaxName']]
    df_sirena_d.rename(columns={'TravelDoc':'PassengerDocument', 'e-Ticket':'TicketNumber', 'Flight' : 'FlightNumber', 'DepartDate':'FlightDate',  'DepartTime':'FlightTime'}, inplace=True)
    df_sirena_d['TicketNumber'] = df_sirena_d['TicketNumber'].apply(str)
    
    # Удаление подозрительных полётов
    def filter_suspicious(group):
        most_common_from = group['From'].mode()[0]
        most_common_dest = group['Dest'].mode()[0]
        
        # Находим "частые" и "подозрительные" записи
        frequent_records = group[(group['From'] == most_common_from) & (group['Dest'] == most_common_dest)]
        suspicious_records = group[(group['From'] != most_common_from) | (group['Dest'] != most_common_dest)]
        
        # Условие: частых записей должно быть как минимум в 5 раз больше, чем подозрительных
        if len(frequent_records) >= 1 * len(suspicious_records):
            # Возвращаем только частые записи, подозрительные удаляем
            return frequent_records
        else:
            # Возвращаем всю группу, если условие не выполнено (не удаляем ничего)
            return group

    # Группируем данные по DepartDate, DepartTime и Flight и применяем фильтрацию
    filtered_df = df_sirena_d.groupby(['FlightDate', 'FlightTime', 'FlightNumber']).apply(filter_suspicious)
    # Убираем лишние индексы после группировки
    filtered_df = filtered_df.reset_index(drop=True)
    df_sirena_d = filtered_df

    df_boarding_d = df_boarding[['PassengerDocument','FlightNumber', 'FlightDate', 'FlightTime', 'TicketNumber']]
    df_boarding_d['TicketNumber'] = df_boarding_d['TicketNumber'].apply(lambda col: None if col == 'Not presented' else col)

    # находим все случаи "двойных" документов
    double_docs = pd.merge(df_sirena_d[['TicketNumber','PassengerDocument']], df_boarding_d[['TicketNumber','PassengerDocument']], on=['TicketNumber'], how = 'outer')
    double_docs = double_docs.loc[~(double_docs['PassengerDocument_x'] == double_docs['PassengerDocument_y'])].dropna()[['PassengerDocument_x','PassengerDocument_y']]

    double_docs.rename(columns={'PassengerDocument_x':'ForeignPassport', 'PassengerDocument_y': 'PassengerDocument'}, inplace=True)
    double_docs = double_docs.drop_duplicates()
    double_docs.rename(columns={'ForeignPassport':'pass'}, inplace=True)

    # мерджим по номеру загранника
    pasports_copy = df_sirena_d[['PassengerDocument']].copy()
    pasports_copy['pass'] = pasports_copy['PassengerDocument'].copy()
    pass_to_pass = pd.merge(pasports_copy, double_docs, how='left', on='pass')

    del pasports_copy
    # заменяем заграничные паспорта на обычные где можем
    df_sirena_d['PassengerDocument'] = pass_to_pass['PassengerDocument_y'].fillna(pass_to_pass['PassengerDocument_x'])

    df_boarding_d = pd.merge(df_boarding_d, filtered_df[['FlightDate','FlightTime','FlightNumber', 'From', 'Dest']].drop_duplicates(), on=['FlightDate','FlightTime','FlightNumber'], how = 'left')
    value_counts = df_boarding_d['TicketNumber'].value_counts()
    # Фильтрация значений, встречающихся более 1 раза
    frequent_values = value_counts[value_counts > 1].index.tolist()
    df_boarding_d = df_boarding_d[~df_boarding_d['TicketNumber'].isin(frequent_values)]

    # объединение полётов и чистка дубликатов
    df = pd.concat([df_sirena_d[['PassengerDocument','FlightNumber', 'FlightDate', 'FlightTime', 'From', 'Dest','TicketNumber']], df_boarding_d[['PassengerDocument','FlightNumber', 'FlightDate', 'FlightTime','From', 'Dest', 'TicketNumber']]]).dropna().drop_duplicates()
    
    df_sirena = pd.merge(df_sirena, double_docs, left_on='TravelDoc', right_on='pass', how='left')
    df_sirena['TravelDoc'] = df_sirena['PassengerDocument'].fillna(df_sirena['TravelDoc'])
    df_sirena = df_sirena.drop('PassengerDocument',axis=1)
    return df, double_docs

def mergeLoyalityIdNickPasports(df_sirena, df_loyality_merged):
    def f(col):
        m =re.search('FF#\w\w\s\d+', str(col))
        if m:
            return m.group().replace('FF#','')
        return None
    df_sirena['FFKey'] = df_sirena['PaxAdditionalInfo'].apply(f)
    df_sirena_lp = df_sirena[['TravelDoc', 'FFKey']].dropna().drop_duplicates()
    b = pd.merge(df_loyality_merged, df_sirena_lp, on='FFKey', how='outer')
    b['ID'] = "pass_"+b['TravelDoc']
    b['ID'] = b['ID'].fillna('id_'+b['uid'])
    b['ID'] = b['ID'].fillna("nick_"+b['NickName'])
    return b