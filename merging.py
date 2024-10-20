import pandas as pd
import data_extracting
def merge_yjx(yaml_path, json_path, xml_path):
    a_df = data_extracting.extractSkyTeamExchange(yaml_path)
    b_df = data_extracting.extractFrequentFlyerForumProfiles(json_path)
    c_df = data_extracting.extractAirlinesData(xml_path) 
    a = a_df['FFKey'].drop_duplicates()
    c = c_df[['card_number', 'uid']].drop_duplicates()
    a_c = pd.merge(a,c, left_on='FFKey', right_on='card_number', how='outer', copy=False)
    a_c['FFKey'] = a_c['FFKey'].fillna(a_c['card_number'])
    idFKey_df = a_c[['uid', 'FFKey']].copy()
    fackers = idFKey_df[idFKey_df.duplicated(subset='FFKey')]
    idFKey_df.drop_duplicates(subset='FFKey', inplace=True)
    user_id = b_df[2]
    user_id['programm'] = user_id['programm']+user_id['Number']
    user_id = user_id[['NickName', 'programm']]
    idFKeyNick_df = pd.merge(idFKey_df, user_id, left_on='FFKey', right_on='programm', how = 'outer')
    idFKeyNick_df['FFKey']= idFKeyNick_df['FFKey'].fillna(idFKeyNick_df['programm'])
    idFKeyNick_df = idFKeyNick_df[['uid','FFKey','NickName']]
    idFKeyNick_df = idFKeyNick_df[~idFKeyNick_df['uid'].isin(fackers['uid'])]
    am_df = pd.merge(idFKeyNick_df, a_df, on='FFKey', how='outer')
    c_df.rename(columns={'card_number':'FFKey'}, inplace=True)
    cm_df = pd.merge(idFKeyNick_df, c_df, on = ['uid','FFKey'], how='outer')
    uidNick = idFKeyNick_df[['uid','NickName']].drop_duplicates()
    bm_df = pd.merge(uidNick, b_df[1], on='NickName', how='outer')
    am_df.dropna(subset='Fare', inplace=True)
    bm_df.dropna(subset='Flight', inplace=True)
    cm_df.dropna(subset='fare', inplace=True) 
    am_df = am_df[['uid', 'FFKey','NickName', 'Date', 'FlightNumber', 'From', 'To', 'Fare']]
    am_df.rename(columns={'FlightNumber':'Flight'}, inplace=True)
    bm_df = bm_df[['uid', 'NickName', 'Date', 'Flight', 'Departure.Airport','Arrival.Airport']].copy()
    bm_df.rename(columns={'Arrival.Airport':'To', 'Departure.Airport':'From'}, inplace=True)
    cm_df = cm_df[['uid', 'FFKey', 'NickName', 'date', 'code', 'departure', 'arrival', 'fare']]
    cm_df.rename(columns={'date':'Date', 'code':'Flight', 'departure':'From', 'arrival':'To', 'fare':'Fare'}, inplace=True)
    res = pd.concat([am_df, bm_df, cm_df], axis=0)
    bm_df[bm_df['NickName'] == 'FrequentFlyer98708'].sort_values('Date')
    res.drop_duplicates(subset=['uid','FFKey', 'Date',	'Flight',	'From',	'To',	'Fare'], inplace= True)
    return (res, fackers, idFKeyNick_df)
