import pandas as pd
import numpy as np
import pymysql.cursors

db = pymysql.connect(
        host = 'localhost',
        user = 'root',
        password = '',
        db = 'suumo_monthly_rent_development',
        charset = 'utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)

df = pd.read_sql('select * from appartments', con=db)
db.close()

#立地を「路線, 駅, 徒歩〜分」に分割
df = df.drop(df.index[(df['station1'].str.count("/") > 1) | (df['station2'].str.count("/") > 1) | (df['station3'].str.count("/") > 1)])
splitted00 = df['station1'].str.split(' 歩', expand=True)
splitted00.columns = ['location1', 'walk_min1']
splitted10 = splitted00['location1'].str.split('/', expand=True)
splitted10.columns = ['line1', 'station_name1']

splitted01 = df['station2'].str.split(' 歩', expand=True)
splitted01.columns = ['location2', 'walk_min2']
splitted11 = splitted01['location2'].str.split('/', expand=True)
splitted11.columns = ['line2', 'station_name2']

splitted02 = df['station3'].str.split(' 歩', expand=True)
splitted02.columns = ['location3', 'walk_min3']
splitted12 = splitted02['location3'].str.split('/', expand=True)
splitted12.columns = ['line3', 'station_name3']

splitted2 = df['initial_cost'].str.split('/', expand=True)
splitted2.columns = ['shikikin', 'reikin', 'hoshokin', 'shokyaku']
#分割したカラムを結合
df = pd.concat([df, splitted00, splitted10, splitted01, splitted11, splitted02, \
                splitted12, splitted2], axis=1)

df['rent'] = df['rent'].str.replace(u'万円', u'')
df['shikikin'] = df['shikikin'].str.replace(u'万円', u'')
df['reikin'] = df['reikin'].str.replace(u'万円', u'')
df['hoshokin'] = df['hoshokin'].str.replace(u'万円', u'')
df['shokyaku'] = df['shokyaku'].str.replace(u'万円', u'')
df['admin_fee'] = df['admin_fee'].str.replace(u'円', u'')
df['age'] = df['age'].str.replace(u'新築', u'0') #新築は築年数0年とする
df['age'] = df['age'].str.replace(u'築', u'')
df['age'] = df['age'].str.replace(u'年', u'')
df['surface'] = df['surface'].str.replace(u'm', u'')
df['walk_min1'] = df['walk_min1'].str.replace(u'分', u'')

#「-」を0に変換
df['admin_fee'] = df['admin_fee'].replace('-',0)
df['shikikin'] = df['shikikin'].replace('-',0)
df['reikin'] = df['reikin'].replace('-',0)
df['hoshokin'] = df['hoshokin'].replace('-',0)
df['shokyaku'] = df['shokyaku'].replace('-',0)
df['shokyaku'] = df['shokyaku'].replace('実費',0)

#文字列から数値に変換
df['rent'] = pd.to_numeric(df['rent'])
df['admin_fee'] = pd.to_numeric(df['admin_fee'])
df['shikikin'] = pd.to_numeric(df['shikikin'])
df['reikin'] = pd.to_numeric(df['reikin'])
df['hoshokin'] = pd.to_numeric(df['hoshokin'])
df['shokyaku'] = pd.to_numeric(df['shokyaku'])
df['age'] = pd.to_numeric(df['age'])
df['surface'] = pd.to_numeric(df['surface'])
df['walk_min1'] = pd.to_numeric(df['walk_min1'])

#単位を合わせるために、admin_fee以外を10000倍。
df['rent'] = df['rent'].astype(int) * 10000
df['shikikin'] = df['shikikin'].astype(int) * 10000
df['reikin'] = df['reikin'].astype(int) * 10000
df['hoshokin'] = df['hoshokin'].astype(int) * 10000
df['shokyaku'] = df['shokyaku'].astype(int) * 10000

#賃貸の計算
df['monthly_rent'] = df['rent'] + df['admin_fee']
df['total_initial_cost'] = df['shikikin'] + df['reikin'] + df['hoshokin'] + df['shokyaku']
df['total_annual_cost'] = df['monthly_rent'] * 12 + df['total_initial_cost']

df = df.dropna(subset=['rent'])
df = df.dropna(subset=['walk_min1'])

#区の切り出し
splitted3 = df['address'].str.split('区', expand=True)
splitted3.columns = ['ward', 'address']
splitted3['ward'] = splitted3['ward'].str.replace('東京都', '')
df = pd.concat([df, splitted3['ward']], axis=1)

# 階の計算
splitted4 = df['floor'].str.split('-', expand=True)
splitted4.columns = ['floor0', 'floor1']
splitted4['floor0'] = splitted4['floor0'].str.replace(u'B', u'-')
splitted4['floor0'] = splitted4['floor0'].str.replace(u'階', u'')
df['floor'] = splitted4['floor0']

# 建物高さの計算
height = df['story'].str.split('地上', expand=True)
height.columns = ['story1', 'story2']
#height['underground'] = height['underground'].str.replace(u'地下', u'')
height['story1'] = height['story1'].str.replace(u'階建', u'')
height['story1'] = height['story1'].str.replace(u'地下', u'')
height['story1'] = height['story1'].str.replace(u'平屋', u'2')
height['story2'] = height['story2'].str.replace(u'階建', u'')
height = height.fillna(0)
df['story'] = pd.to_numeric(height['story2']) + pd.to_numeric(height['story1'])

#indexを振り直す（これをしないと、以下の処理でエラーが出る）
df = df.reset_index(drop=True)

#間取りを分割
df['plan_DK'] = 0
df['plan_L'] = 0
df['plan_K'] = 0
df['plan_S'] = 0
df['floor_plan'] = df['floor_plan'].str.replace(u'ワンルーム', u'1') #ワンルームを1に変換

# df index or df ix ->
df['plan_L'].iloc[df.index[df['floor_plan'].str.count('L') > 0]] = 1
df['floor_plan'] = df['floor_plan'].str.replace(u'L', u'')

df['plan_DK'].iloc[df.index[df['floor_plan'].str.count('DK') > 0]] = 1
df['floor_plan'] = df['floor_plan'].str.replace(u'DK', u'')

df['plan_K'].iloc[df.index[df['floor_plan'].str.count('K') > 0]] = 1
df['floor_plan'] = df['floor_plan'].str.replace(u'K', u'')

df['plan_S'].iloc[df.index[df['floor_plan'].str.count('S') > 0]] = 1
df['floor_plan'] = df['floor_plan'].str.replace(u'S', u'')


df = df[['name','ward','floor_plan','plan_DK','plan_L','plan_K','plan_S','age','story', \
    'floor','surface','walk_min1','line1', 'station_name1', 'walk_min2','line2', \
    'station_name2', 'walk_min3','line3', 'station_name3', 'rent','admin_fee', \
    'shikikin', 'reikin','hoshokin','shokyaku', 'monthly_rent', 'total_initial_cost', \
    'total_annual_cost', 'url']]

df.to_csv('suumo_treated_DB.csv', sep = '\t',encoding='utf-8')
