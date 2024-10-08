"""Модуль функций для извлечения данных из файлов в DataFrame"""
import pandas as pd
import json
import xml.etree.ElementTree as ET

import zipfile
import os

def extractBoardingData(path: str) -> pd.DataFrame:
    return pd.read_csv(path, sep=';')

def extractAirlinesData(path: str)  -> pd.DataFrame:
    tree = ET.parse(path)
    root = tree.getroot()

    # Извлечение данных о пользователях, картах и активностях
    data = []
    for user in root.findall('user'):
        uid = user.get('uid')
        first_name = user.find('name').get('first')
        last_name = user.find('name').get('last')
        
        for card in user.find('cards').findall('card'):
            card_number = card.get('number')
            bonus_program = card.find('bonusprogramm').text
            
            for activity in card.find('activities').findall('activity'):
                activity_type = activity.get('type')
                code = activity.find('Code').text
                date = activity.find('Date').text
                departure = activity.find('Departure').text
                arrival = activity.find('Arrival').text
                fare = activity.find('Fare').text
                
                data.append({
                    'uid': uid,
                    'first_name': first_name,
                    'last_name': last_name,
                    'card_number': card_number,
                    'bonus_program': bonus_program,
                    'activity_type': activity_type,
                    'code': code,
                    'date': date,
                    'departure': departure,
                    'arrival': arrival,
                    'fare': fare
                })

    return pd.DataFrame(data)

def extractSirenaExportFixed(path: str) -> pd.DataFrame:
    colspecs = [
        (0, 60),    # PaxName
        (60, 72),   # PaxBirthDate
        (72, 84),   # DepartDate
        (84, 96),   # DepartTime
        (96, 108),  # ArrivalDate
        (108, 120), # ArrivalTime
        (120, 132), # FlightCodeSh
        (132, 138), # From
        (138, 144), # Dest
        (144, 150), # Code
        (150, 168), # e-Ticket
        (168, 180), # TravelDoc
        (180, 186), # Seat
        (186, 192), # Meal
        (192, 198), # TrvCls
        (198, 204), # Fare
        (204, 216), # Baggage
        (216, 276), # PaxAdditionalInfo
        (276, 357)  # AgentInfo
    ]
    return pd.read_fwf(path, colspecs=colspecs)



def extractFrequentFlyerForumProfiles(path: str):
    """
    Функция для извлечения данных из FrequentFlyerForum-Profiles.json;

    Извлекает как систему связанных ником таблиц;

    Returns:
        (pd.DataFrame, pd.DataFrame, pd.DataFrame):  Таблица общей информации, Таблица о полётах, Таблица о программах лояльности
    """

    with open(path, 'r') as f:
        data = json.load(f)

    def extractSubtable(name: str) -> pd.DataFrame:
        """Функция для объединения записей json разных пользователей в одну таблицу
            Args:
                name (str): Название поля
        """
        
        df = [pd.json_normalize(data['Forum Profiles'][0][name])]
        
        for i in range(1, len(data['Forum Profiles'])):
            df_ = pd.json_normalize(data['Forum Profiles'][i][name])
            df_["NickName"] = data['Forum Profiles'][i]['NickName']

            df.append(df_)
        return pd.concat(df)
    
    flights_table = extractSubtable("Registered Flights")
    loyality_table = extractSubtable("Loyality Programm")
    # documents_table = concat("Travel Documents") # Все - NaN
    names_table = extractSubtable("Real Name")
 
    return names_table, flights_table, loyality_table,


def extract_data_from_file(filepath):
    '''
    Функция для извлечения данныx одного файла xlsx из zip
    '''
    df = pd.read_excel(filepath, header=None)
    sequence = df.iloc[0, 7]
    gender = df.iloc[2, 0]
    passenger_name = df.iloc[2, 1] if not pd.isna(df.iloc[2, 1]) else "Unknown"
    flight_number = df.iloc[4, 0] if not pd.isna(df.iloc[4, 0]) else "Unknown"
    departure_city = df.iloc[4, 3]
    arrival_city = df.iloc[4, 7]
    aeroport1 = df.iloc[6, 3]
    aeroport2 = df.iloc[6, 7]
    flight_date = df.iloc[8, 0]
    departure_time = df.iloc[8, 2]
    pnr = df.iloc[12, 1]
    ticket_number = df.iloc[12, 4]
    seat = df.iloc[10, 7]
    gate = df.iloc[6, 1]
    y = df.iloc[2, 7]
    return {
        'sequence': sequence,
        'gender': gender,
        'passenger_name': passenger_name,
        'flight_number': flight_number,
        'departure_city': departure_city,
        'arrival_city': arrival_city,
        'aeroport1': aeroport1,
        'aeroport2': aeroport2,
        'flight_date': flight_date,
        'departure_time': departure_time,
        'pnr': pnr,
        'ticket_number': ticket_number,
        'seat': seat,
        'gate': gate,
        'trvCls': y
    }

def process_zip_archive_to_df(zip_filepath):
    """
    Функция для обработки всех файлов в архиве

    Для запуска: 
        # путь к архиву zip
    zip_filepath = '.../Airlines-All/Airlines/YourBoardingPassDotAero.zip'
    df = process_zip_archive_to_df(zip_filepath) 

    """
    extract_dir = "temp_extract"
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    all_data = []
    for filename in os.listdir(extract_dir):
        if filename.endswith(".xlsx"):
            filepath = os.path.join(extract_dir, filename)
            data = extract_data_from_file(filepath)
            if data:
                all_data.append(data)

    df = pd.DataFrame(all_data)
    #os.rmdir(extract_dir)
    return df
    