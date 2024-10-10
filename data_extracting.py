"""Модуль функций для извлечения данных из файлов в DataFrame"""
import pandas as pd
import json
import xml.etree.ElementTree as ET
import re

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

def extractSkyTeamExchange(path: str) -> pd.DataFrame:
    with open(path, 'r') as yaml_file:
        data = {'Date': [], 'FlightNumber': [], 'FFKey': [], 'Class': [], 'Fare': [], 'From': [], 'Status': [], 'To' : []}
        current_date = None
        flight_number = None
        flight_from = None
        flight_status = None
        flight_to = None
        ff = []

        for line in yaml_file:
            line = line.strip().strip(':')
            # Проверяем, является ли строка датой
            if re.match(r"^\s*\'(\d{4}-\d{2}-\d{2})\'$", line):
                current_date = line.strip('\'').strip()
                continue

            # Проверяем, является ли строка номером рейса
            if re.match(r"^\s*(\w{2}\d{4})$", line):
                flight_number = line.strip()
                continue

            match = re.match(r"^\s*(\w{2}\s\d+):\s\{CLASS:\s(\w),\sFARE:\s(\w{6})\}$", line)
            if match:
                ff_key, class_value, fare_value = match.groups()
                ff.append((ff_key, class_value, fare_value))
                continue

            if re.match(r"^\s*FROM:\s\w{3}$", line):
                flight_from = line.split(': ')[1].strip()
                continue
            
            if re.match(r"^\s*STATUS:\s\w+$", line):
                flight_status = line.split(': ')[1].strip()
                continue
            
            if re.match(r"^\s*TO:\s\w{3}$", line):
                flight_to = line.split(': ')[1].strip()
                for i in ff:
                    data['Date'].append(current_date)
                    data['FlightNumber'].append(flight_number)
                    data['FFKey'].append(i[0])
                    data['Class'].append(i[1])
                    data['Fare'].append(i[2])
                    data['From'].append(flight_from)
                    data['Status'].append(flight_status)
                    data['To'].append(flight_to)
                ff.clear()
    return pd.DataFrame(data)

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