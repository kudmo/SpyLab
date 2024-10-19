import pandas as pd

def mergeBaseInfo(df_sirena :pd.DataFrame , df_boarding: pd.DataFrame):
    """
    Объединение базовых данных по паспорту, дате, номеру рейса
    Args:
        df_sirena (pd.DataFrame): DaraFrame из Sirena-export-fixed.tab
        df_boarding (pd.DataFrame): DaraFrame из BoardingData.csv
    Returns:
        pd.DataFrame: Данные: ['PassengerDocument, 'FlightNumber', 'FlightDate','FlightTime','From','Dest', 'TicketNumber']
    """
    # Выделяем нужные данные
    df_sirena_d = df_sirena[['TravelDoc', 'e-Ticket','Flight','DepartDate', 'DepartTime', 'From', 'Dest', 'PaxName']]
    df_sirena_d.rename(columns={'TravelDoc':'PassengerDocument', 'e-Ticket':'TicketNumber', 'Flight' : 'FlightNumber', 'DepartDate':'FlightDate',  'DepartTime':'FlightTime'}, inplace=True)
    df_sirena_d['TicketNumber'] = df_sirena_d['TicketNumber'].apply(str)

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

    # объединение полётов и чистка дубликатов
    df = pd.concat([df_sirena_d[['PassengerDocument','FlightNumber', 'FlightDate', 'FlightTime','TicketNumber']], df_boarding_d[['PassengerDocument','FlightNumber', 'FlightDate', 'FlightTime', 'TicketNumber']]]).drop_duplicates()
    df = pd.merge(df, df_sirena_d[['TicketNumber', 'From' ,'Dest']], on='TicketNumber', how ='outer')
    return df