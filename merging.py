import pandas as pd

def mergeToDrawFlights(df_sirena :pd.DataFrame , df_boarding: pd.DataFrame):
    """
    Объединение базовых данных по паспорту, дате, номеру рейса
    Args:
        df_sirena (pd.DataFrame): DaraFrame из Sirena-export-fixed.tab
        df_boarding (pd.DataFrame): DaraFrame из BoardingData.csv
        timetable (pd.DataFrame): DaraFrame из Skyteam_Timetable.pdf
    Returns:
        pd.DataFrame: Данные: ['PassengerDocument, 'FlightNumber', 'FlightDate','FlightTime','From','Dest', 'TicketNumber']
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
    df_boarding_d['PassangerName'] = df_boarding['PassengerLastName'] +' ' +  df_boarding['PassengerFirstName'] + ' ' + df_boarding['PassengerSecondName'].apply(lambda col: col[0])

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
    return df