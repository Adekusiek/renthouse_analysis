import pymysql.cursors
import pandas as pd
from sklearn.externals import joblib

# Connect to DB
db = pymysql.connect(
        host = 'localhost',
        user = 'root',
        password = '',
        db = 'all_monthly_rent_development',
        charset = 'utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)

# get data with not prediction data
df = pd.read_sql('select * from appartments where pre_monthly_rent is NULL;', con=db)

#立地を「路線, 駅, 徒歩〜分」に分割
df = df.drop(df.index[(df['station1'].str.count("/") > 1) | (df['station2'].str.count("/") > 1) | (df['station3'].str.count("/") > 1)])
df = df.drop(df.index[df['initial_cost'].str.count("/") > 3])

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

# df['rent'] = df['rent'].str.replace(u'万円', u'')
df['shikikin'] = df['shikikin'].str.replace(u'万円', u'')
df['reikin'] = df['reikin'].str.replace(u'万円', u'')
df['hoshokin'] = df['hoshokin'].str.replace(u'万円', u'')
df['shokyaku'] = df['shokyaku'].str.replace(u'万円', u'')
df['walk_min1'] = df['walk_min1'].str.replace(u'分', u'')

#「-」を0に変換
# df['admin_fee'] = df['admin_fee'].replace('-',0)
df['shikikin'] = df['shikikin'].replace('-',0)
df['reikin'] = df['reikin'].replace('-',0)
df['hoshokin'] = df['hoshokin'].replace('-',0)
df['shokyaku'] = df['shokyaku'].replace('-',0)
df['shikikin'] = df['shikikin'].replace('--',0)
df['reikin'] = df['reikin'].replace('--',0)
df['hoshokin'] = df['hoshokin'].replace('--',0)
df['shokyaku'] = df['shokyaku'].replace('--',0)
df['shokyaku'] = df['shokyaku'].replace('実費',0)

#文字列から数値に変換
# df['rent'] = pd.to_numeric(df['rent'])
# df['admin_fee'] = pd.to_numeric(df['admin_fee'])
df['shikikin'] = pd.to_numeric(df['shikikin'])
df['reikin'] = pd.to_numeric(df['reikin'])
df['hoshokin'] = pd.to_numeric(df['hoshokin'])
df['shokyaku'] = pd.to_numeric(df['shokyaku'])
# df['age'] = pd.to_numeric(df['age'])
# df['surface'] = pd.to_numeric(df['surface'])
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

df['floor_plan'] = df['floor_plan'].str.replace(u'R', u'')
df['floor_plan'] = df['floor_plan'].str.replace(u'+', u'')
df['floor_plan'] = pd.to_numeric(df['floor_plan'])


df = df[['id', 'name','ward','floor_plan','plan_DK','plan_L','plan_K','plan_S','age','story', \
    'floor','surface','walk_min1','line1', 'station_name1', 'walk_min2','line2', \
    'station_name2', 'walk_min3','line3', 'station_name3', 'rent','admin_fee', \
    'shikikin', 'reikin','hoshokin','shokyaku', 'monthly_rent', 'total_initial_cost', \
    'total_annual_cost']]

# prepare df_o for prediction
ward = pd.get_dummies(df['ward'])

df_o = df[['floor_plan', 'plan_DK','plan_L', 'plan_K','plan_S','age','story', 'floor','surface','walk_min1', 'monthly_rent']]
df_o = pd.merge(df_o, ward, left_index=True, right_index=True)
df_o = df_o.dropna()

y_true = df_o['monthly_rent']
df_o.drop(['monthly_rent'], axis=1, inplace=True)
data = df_o

# import training result
clf = joblib.load('all_rent_monthly_rent.pkl')
pre_monthly_rent = clf.predict(data)
delta = (pre_monthly_rent - y_true)
pre_monthly_rent = pd.DataFrame(pre_monthly_rent)
pre_monthly_rent.columns = ['pre_monthly_rent']
delta.name = 'delta_monthly_rent'
df = pd.concat([df, pre_monthly_rent, delta], axis=1)
df['pre_monthly_rent'] = df['pre_monthly_rent'].astype(int)
df['delta_monthly_rent'] = df['delta_monthly_rent'].astype(int)

#Update database for pre_monthly_rent and delta_monthly_rent
for i in range(len(df)):
    c = db.cursor()
    sql_update = "update appartments set pre_monthly_rent = '%s', delta_monthly_rent = '%s' where id = %s;" \
                    % (df['pre_monthly_rent'][i], df['delta_monthly_rent'][i],  df['id'][i])
    c.execute(sql_update)
    db.commit()
    c.close()
