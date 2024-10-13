"""Модуль функций для извлечения данных из файлов в DataFrame"""
import pandas as pd
import json
import xml.etree.ElementTree as ET
import pdfplumber
import re
import PyPDF2, os

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


def convertPdfToDataframe(path: str):
    with open(path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)

        num_pages = len(reader.pages)

        # Loop through all the pages and extract text
        for i in range(num_pages):
            page = reader.pages[i]
            text = page.extract_text()
            print(f"Page {i+1}:\n{text}\n")


    # Открываем PDF-файл
    pdf = pdfplumber.open(path)
    extracted_data = []
    last_from = None
    last_to = None

        # Регулярные выражения для извлечения данных From и To
    from_pattern = re.compile(r'FROM:\s+(.+?),\s+(.+?)\s+(\w{3})')
    to_pattern = re.compile(r'TO:\s+(.+?),\s+(.+?)\s+(\w{3})')

        # Регулярное выражение для извлечения строк таблицы (дни: от 0 до 7 цифр)
    pattern = re.compile(r'(\d{2} \w{3}  -  \d{2} \w{3})\s+(\d{0,7})\s+(\d{2}:\d{2})\s+(\d{2}:\d{2})\s+(\w+)\s+(\w+)\s+(\d{1}H\d{2}M)')

    for i in range(num_pages):
        if i % 100 == 0:
            print(f"Iteration No.{i}")
        page = reader.pages[i]
        text = page.extract_text()

            # Попытка извлечь пункты отправления и назначения
        from_match = from_pattern.search(text)
        to_match = to_pattern.search(text)

            # Если найден FROM и TO, обновляем последние значения
        if from_match and to_match:
            last_from = f"{from_match.group(1)}, {from_match.group(2)} ({from_match.group(3)})"
            last_to = f"{to_match.group(1)}, {to_match.group(2)} ({to_match.group(3)})"

            # Если на странице нет FROM и TO, остаются последние известные значения
        if last_from and last_to:
                # Извлекаем строки таблицы
            matches = pattern.findall(text)

                # Добавляем найденные строки с полями From и To
            for match in matches:
                extracted_data.append(list(match) + [last_from, last_to])

    # Создаем DataFrame
    columns = ['Validity', 'Days', 'Dep Time', 'Arr Time', 'Flight', 'Aircraft', 'Travel Time', 'From', 'To']
    df = pd.DataFrame(extracted_data, columns=columns)

    # Вывод DataFrame
    print(df)

    # Сохранение в CSV (или другой формат)
    df.to_csv('flights_data.csv', index=False)
    return df