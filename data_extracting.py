"""Модуль функций для извлечения данных из файлов в DataFrame"""
import pandas as pd
import json

def extractBoardingData(path: str) -> pd.DataFrame:
    return pd.read_csv(path, sep=';')

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
