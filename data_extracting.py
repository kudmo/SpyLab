"""Модуль функций для извлечения данных из файлов в DataFrame"""
import pandas as pd
import json
import xml.etree.ElementTree as ET
import pdfplumber
import re
import pdfplumber

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


def extractSkyteamTimetable(path: str) ->pd.DataFrame:
    with pdfplumber.open(path) as pdf:
        tables = []
        info1 = {'From':None, 'From_Code':None, 'To':None,'To_Code':None}
        info2 = {'From':None, 'From_Code':None, 'To':None,'To_Code':None}
        
        for page_number in range(4, len(pdf.pages)):
            data = []
            page = pdf.pages[page_number]
            # выделение таблицы
            table_objects = page.extract_tables()

            data_start = 0
            # Заголовки таблицы
            if ('FROM:' in table_objects[0][0]):
                info1['From'] = table_objects[0][0][1]
                info1['From_Code'] = table_objects[0][0][7]

                info2['From'] = table_objects[0][0][11:][1]
                info2['From_Code'] = table_objects[0][0][11:][7]

                info1['To'] = table_objects[0][1][1]
                info1['To_Code'] = table_objects[0][1][7]

                info2['To'] = table_objects[0][1][11:][1]
                info2['To_Code'] = table_objects[0][1][11:][7]
                
                data_start = 3

            # Пропускаем пустые таблицы
            if ('Consult your travel agent for details' in table_objects[0][data_start]):
                continue
        
            # Проход по таблице
            for table_object in table_objects[0]:
                # очистка от пропусков и от лишних данных
                cleared = list(filter(lambda x: x is not None and x != '' and 'Operated by' not in x, table_object))
                if (len(cleared) < 7):
                    continue
                have_left = True
                if (table_object[0] == None or table_object[0] == ''):
                    have_left = False

                data1 = [info1['From'],info1['From_Code'], info1['To'], info1['To_Code']]
                data2 = [info2['From'],info2['From_Code'], info2['To'], info2['To_Code']]

                if (len(cleared) == 14):
                    data1.extend(cleared[:7])
                    data.append(data1)

                    data2.extend(cleared[7:])
                    data.append(data2)
                elif have_left:
                    data1.extend(cleared)
                    data.append(data1)
                else:
                    data2.extend(cleared)
                    data.append(data2)
                    

            tables.append(pd.DataFrame(data, columns=['From','From_Code','To','To_Code','Validity','Days','Dep_Time','Arr_Time','Flight','Aircraft','Travel_Time']))
            page.flush_cache()

        return pd.concat(tables)
