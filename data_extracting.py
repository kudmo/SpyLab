import pandas as pd
import json
import xml.etree.ElementTree as ET
def extractBoardingData(path: str):
    return pd.read_csv(path, sep=';')

def extractAirlinesData(path: str):
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

    # Создание DataFrame
    return pd.DataFrame(data)
def extractSirenaExportFixed(path: str):
    colspecs = [
        (0, 60),    # PaxName
        (60, 72),   # PaxBirthDate
        (72, 82),   # DepartDate
        (82, 92),   # DepartTime
        (92, 102),  # ArrivalDate
        (102, 112), # ArrivalTime
        (112, 122), # FlightCode
        (122, 128), # ShFrom
        (128, 134), # Dest
        (134, 156), # Code
        (156, 176), # e-Ticket
        (176, 187), # TravelDoc
        (187, 197), # Seat
        (197, 208), # Meal
        (208, 219), # TrvClsFare
        (219, 229), # Baggage
        (229, 287), # PaxAdditionalInfo
        (287, 357)  # AgentInfo
    ]
    return pd.read_fwf(path, colspecs=colspecs)


