import pandas as pd
import numpy as np

import pandas as pd
import numpy as np

df_chiyoda  = pd.read_csv("./csv_suumo/chiyoda.csv", sep='\t', encoding='utf-16')
df_chuo     = pd.read_csv("./csv_suumo/chuo.csv", sep='\t', encoding='utf-16')
df_minato   = pd.read_csv("./csv_suumo/minato.csv", sep='\t', encoding='utf-16')
df_shinjuku = pd.read_csv("./csv_suumo/shinjuku.csv", sep='\t', encoding='utf-16')
df_bunkyo   = pd.read_csv("./csv_suumo/bunkyo.csv", sep='\t', encoding='utf-16')
df_shibuya  = pd.read_csv("./csv_suumo/shibuya.csv", sep='\t', encoding='utf-16')
df_taito    = pd.read_csv("./csv_suumo/taito.csv", sep='\t', encoding='utf-16')
df_sumida   = pd.read_csv("./csv_suumo/sumida.csv", sep='\t', encoding='utf-16')
df_koto     = pd.read_csv("./csv_suumo/koto.csv", sep='\t', encoding='utf-16')
df_shinagawa = pd.read_csv("./csv_suumo/shinagawa.csv", sep='\t', encoding='utf-16')
df_meguro   = pd.read_csv("./csv_suumo/meguro.csv", sep='\t', encoding='utf-16')
df_ota      = pd.read_csv("./csv_suumo/ota.csv", sep='\t', encoding='utf-16')
df_setagaya = pd.read_csv("./csv_suumo/setagaya.csv", sep='\t', encoding='utf-16')
df_nakano   = pd.read_csv("./csv_suumo/nakano.csv", sep='\t', encoding='utf-16')
df_suginami = pd.read_csv("./csv_suumo/suginami.csv", sep='\t', encoding='utf-16')
df_toshima  = pd.read_csv("./csv_suumo/toshima.csv", sep='\t', encoding='utf-16')
df_kita     = pd.read_csv("./csv_suumo/kita.csv", sep='\t', encoding='utf-16')
df_arakawa  = pd.read_csv("./csv_suumo/arakawa.csv", sep='\t', encoding='utf-16')
df_itabashi = pd.read_csv("./csv_suumo/itabashi.csv", sep='\t', encoding='utf-16')
df_nerima   = pd.read_csv("./csv_suumo/nerima.csv", sep='\t', encoding='utf-16')
df_adachi   = pd.read_csv("./csv_suumo/adachi.csv", sep='\t', encoding='utf-16')
df_katushika = pd.read_csv("./csv_suumo/katushika.csv", sep='\t', encoding='utf-16')
df_edogawa  = pd.read_csv("./csv_suumo/edogawa.csv", sep='\t', encoding='utf-16')

df = pd.concat([df_chiyoda, df_chuo, df_minato, df_shinjuku, df_bunkyo, df_shibuya, df_taito,
                df_sumida, df_koto, df_shinagawa, df_meguro, df_ota, df_setagaya, df_nakano,
                df_suginami, df_toshima, df_kita, df_arakawa, df_itabashi, df_nerima, df_adachi,
                df_katushika, df_edogawa], axis=0, ignore_index=True)

df.drop(['Unnamed: 0'], axis=1, inplace=True)

#立地を「路線, 駅, 徒歩〜分」に分割
splitted0 = df['立地'].str.split(' 歩', expand=True)
splitted0.columns = ['立地1', '歩分']
splitted1 = splitted0['立地1'].str.split('/', expand=True)
splitted1.columns = ['路線', '駅']
df['歩分'] = splitted0['歩分']

splitted2 = df['敷/礼/保証/敷引,償却'].str.split('/', expand=True)
splitted2.columns = ['敷金', '礼金', '保証金', '敷引,償却']
#分割したカラムを結合
df = pd.concat([df, splitted1, splitted2], axis=1)

df.drop(['立地','敷/礼/保証/敷引,償却'], axis=1, inplace=True)

df = df.dropna(subset=['賃料'])

df['賃料'] = df['賃料'].str.replace(u'万円', u'')
df['敷金'] = df['敷金'].str.replace(u'万円', u'')
df['礼金'] = df['礼金'].str.replace(u'万円', u'')
df['保証金'] = df['保証金'].str.replace(u'万円', u'')
df['敷引,償却'] = df['敷引,償却'].str.replace(u'万円', u'')
df['管理費'] = df['管理費'].str.replace(u'円', u'')
df['築年数'] = df['築年数'].str.replace(u'新築', u'0') #新築は築年数0年とする
df['築年数'] = df['築年数'].str.replace(u'築', u'')
df['築年数'] = df['築年数'].str.replace(u'年', u'')
df['専有面積'] = df['専有面積'].str.replace(u'm', u'')
df['歩分'] = df['歩分'].str.replace(u'分', u'')

#「-」を0に変換
df['管理費'] = df['管理費'].replace('-',0)
df['敷金'] = df['敷金'].replace('-',0)
df['礼金'] = df['礼金'].replace('-',0)
df['保証金'] = df['保証金'].replace('-',0)
df['敷引,償却'] = df['敷引,償却'].replace('-',0)
df['敷引,償却'] = df['敷引,償却'].replace('実費',0)

#文字列から数値に変換
df['賃料'] = pd.to_numeric(df['賃料'])
df['管理費'] = pd.to_numeric(df['管理費'])
df['敷金'] = pd.to_numeric(df['敷金'])
df['礼金'] = pd.to_numeric(df['礼金'])
df['保証金'] = pd.to_numeric(df['保証金'])
df['敷引,償却'] = pd.to_numeric(df['敷引,償却'])
df['築年数'] = pd.to_numeric(df['築年数'])
df['専有面積'] = pd.to_numeric(df['専有面積'])
df['歩分'] = pd.to_numeric(df['歩分'])

#単位を合わせるために、管理費以外を10000倍。
df['賃料'] = df['賃料'].astype(int) * 10000
df['敷金'] = df['敷金'].astype(int) * 10000
df['礼金'] = df['礼金'].astype(int) * 10000
df['保証金'] = df['保証金'].astype(int) * 10000
df['敷引,償却'] = df['敷引,償却'].astype(int) * 10000

#賃貸の計算
df['賃料月額'] = df['賃料'] + df['管理費']
df['初期費用'] = df['敷金'] + df['礼金'] + df['保証金'] + df['敷引,償却']
df['年間費用'] = df['賃料月額'] *12 + df['初期費用']

#区の切り出し
splitted3 = df['住所'].str.split('区', expand=True)
splitted3.columns = ['区', '住所']
splitted3['区'] = splitted3['区'].str.replace('東京都', '')
df = pd.concat([df, splitted3['区']], axis=1)

# 階の計算
splitted4 = df['階'].str.split('-', expand=True)
splitted4.columns = ['階0', '階1']
splitted4['階0'] = splitted4['階0'].str.replace(u'B', u'-')
splitted4['階0'] = splitted4['階0'].str.replace(u'階', u'')
df['階'] = splitted4['階0']

# 建物高さの計算
height = df['建物高さ'].str.split('地上', expand=True)
height.columns = ['height1', 'height2']
#height['underground'] = height['underground'].str.replace(u'地下', u'')
height['height1'] = height['height1'].str.replace(u'階建', u'')
height['height1'] = height['height1'].str.replace(u'地下', u'')
height['height1'] = height['height1'].str.replace(u'平屋', u'2')
height['height2'] = height['height2'].str.replace(u'階建', u'')
height = height.fillna(0)
df['建物高さ'] = pd.to_numeric(height['height2']) + pd.to_numeric(height['height1'])

#indexを振り直す（これをしないと、以下の処理でエラーが出る）
df = df.reset_index(drop=True)

#間取りを分割
df['間取りDK_LDK'] = 0
df['間取りK'] = 0
df['間取りS'] = 0
df['間取り'] = df['間取り'].str.replace(u'ワンルーム', u'1') #ワンルームを1に変換

for x in range(len(df)):
    if 'DK' in df['間取り'][x]:
        df['間取りDK_LDK'][x] = 1
    elif 'K' in df['間取り'][x]:
        df['間取りK'][x] = 1
    elif 'S' in df['間取り'][x]:
        df['間取りS'][x] = 1

df['間取り'] = df['間取り'].str.replace(u'DK', u'')
df['間取り'] = df['間取り'].str.replace(u'K', u'')
df['間取り'] = df['間取り'].str.replace(u'L', u'')
df['間取り'] = df['間取り'].str.replace(u'S', u'')

df = df[['マンション名','区','間取り','間取りDK_LDK','間取りK','間取りS','築年数','建物高さ','階','専有面積',
    '歩分', '路線', '駅', '賃料','管理費','敷金','礼金','保証金','敷引,償却', '賃料月額', '初期費用', '年間費用']]

df.to_csv('suumo_treated.csv', sep = '\t',encoding='utf-16')
