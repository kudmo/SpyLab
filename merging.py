import pandas as pd

def mergeBaseInfo(df_sirena :pd.DataFrame , df_boarding: pd.DataFrame):
    """
    Объединение базовых данных по паспорту, дате, номеру рейса
    Args:
        df_sirena (pd.DataFrame): DaraFrame из Sirena-export-fixed.tab
        df_boarding (pd.DataFrame): DaraFrame из BoardingData.csv
    Returns:
        pd.DataFrame: Данные: ['PassengerDocument', 'PaxName', 'FlightNumber', 'FlightDate','FlightTime','From','Dest', 'TicketNumber']
    """
    
    df_sirena_d = df_sirena[['TravelDoc', 'e-Ticket','Flight','DepartDate', 'DepartTime', 'From', 'Dest', 'PaxName']]
    df_sirena_d.rename(columns={'TravelDoc':'PassengerDocument', 'e-Ticket':'TicketNumber', 'Flight' : 'FlightNumber', 'DepartDate':'FlightDate',  'DepartTime':'FlightTime'}, inplace=True)
    df_sirena_d['TicketNumber'] = df_sirena_d['TicketNumber'].apply(str)


    df_boarding_d = df_boarding[['PassengerDocument','FlightNumber', 'FlightDate', 'FlightTime', 'TicketNumber']]
    df_boarding_d['TicketNumber'] = df_boarding_d['TicketNumber'].apply(lambda col: None if col == 'Not presented' else col)
    df_boarding_d['PassangerName'] = df_boarding['PassengerLastName'] +' ' +  df_boarding['PassengerFirstName'] + ' ' + df_boarding['PassengerSecondName'].apply(lambda col: col[0])
    
    df = pd.merge(df_sirena_d, df_boarding_d, on=['PassengerDocument','FlightNumber', 'FlightDate', 'FlightTime', 'TicketNumber'], how = 'outer')[['PassengerDocument','PassangerName', 'PaxName', 'FlightNumber', 'FlightDate','FlightTime','From','Dest', 'TicketNumber']]
    
    # df['FlightDate'] = df['FlightDate'].apply(pd.to_datetime)
    # df['FlightTime'] = df['FlightTime'].apply(pd.to_datetime)
    return df