import pandas as pd
import json

def extractBoardingData(path: str):
    return pd.read_csv(path, sep=';')