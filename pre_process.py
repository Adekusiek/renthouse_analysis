import pandas as pd
import numpy as np

df_chiyoda  = pd.read_csv("chiyoda.csv", sep='\t', encoding='utf-16')
df_chuo     = pd.read_csv("chuo.csv", sep='\t', encoding='utf-16')
df_minato   = pd.read_csv("minato.csv", sep='\t', encoding='utf-16')
df_shinjuku = pd.read_csv("shinjuku.csv", sep='\t', encoding='utf-16')
df_bunkyo   = pd.read_csv("bunkyo.csv", sep='\t', encoding='utf-16')
df_shibuya  = pd.read_csv("shibuya.csv", sep='\t', encoding='utf-16')
df_taito    = pd.read_csv("taito.csv", sep='\t', encoding='utf-16')
df_sumida   = pd.read_csv("sumida.csv", sep='\t', encoding='utf-16')
df_koto     = pd.read_csv("koto.csv", sep='\t', encoding='utf-16')
df_shinagawa = pd.read_csv("shinagawa.csv", sep='\t', encoding='utf-16')
df_meguro   = pd.read_csv("meguro.csv", sep='\t', encoding='utf-16')
df_ota      = pd.read_csv("ota.csv", sep='\t', encoding='utf-16')
df_setagaya = pd.read_csv("setagaya.csv", sep='\t', encoding='utf-16')
df_nakano   = pd.read_csv("nakano.csv", sep='\t', encoding='utf-16')
df_suginami = pd.read_csv("suginami.csv", sep='\t', encoding='utf-16')
df_toshima  = pd.read_csv("toshima.csv", sep='\t', encoding='utf-16')
df_kita     = pd.read_csv("kita.csv", sep='\t', encoding='utf-16')
df_arakawa  = pd.read_csv("arakawa.csv", sep='\t', encoding='utf-16')
df_itabashi = pd.read_csv("itabashi.csv", sep='\t', encoding='utf-16')
df_nerima   = pd.read_csv("nerima.csv", sep='\t', encoding='utf-16')
df_adachi   = pd.read_csv("adachi.csv", sep='\t', encoding='utf-16')
df_katushika = pd.read_csv("katushika.csv", sep='\t', encoding='utf-16')
df_edogawa  = pd.read_csv("edogawa.csv", sep='\t', encoding='utf-16')

df = pd.concat([df_chiyoda, df_chuo, df_minato, df_shinjuku, df_bunkyo, df_shibuya, df_taito,
                df_sumida, df_koto, df_shinagawa, df_meguro, df_ota, df_setagaya, df_nakano,
                df_suginami, df_toshima, df_kita, df_arakawa, df_itabashi, df_nerima, df_adachi,
                df_katushika, df_edogawa], axis=0, ignore_index=True)

df.drop(['Unnamed: 0'], axis=1, inplace=True)

#立地を「路線+駅」と「徒歩〜分」に分割
splitted1 = df['立地'].str.split(' 歩', expand=True)
splitted1.columns = ['立地1', '立地2']

#その他費用を、「敷金」「礼金」「保証金」「敷引,償却」に分割
splitted2 = df['敷/礼/保証/敷引,償却'].str.split('/', expand=True)
splitted2.columns = ['敷金', '礼金', '保証金', '敷引,償却']


#「敷引,償却」をさらに「敷引」「償却」に分割
#splitted3 = df['敷引,償却'].str.split(',', expand=True)
#splitted3.columns = ['敷引', '償却']

#分割したカラムを結合
df = pd.concat([df, splitted1, splitted2], axis=1)

df.drop(['立地','敷/礼/保証/敷引,償却'], axis=1, inplace=True)

#「賃料」がNAの行を削除
df = df.dropna(subset=['賃料'])

#エンコードをcp932に変更しておく（これをしないと、replaceできない）
df['賃料'].str.encode('cp932')
df['敷金'].str.encode('cp932')
df['礼金'].str.encode('cp932')
df['保証金'].str.encode('cp932')
df['敷引,償却'].str.encode('cp932')
df['管理費'].str.encode('cp932')
df['築年数'].str.encode('cp932')
df['専有面積'].str.encode('cp932')
df['立地2'].str.encode('cp932')

#数値として扱いたいので、不要な文字列を削除
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
df['立地2'] = df['立地2'].str.replace(u'分', u'')

#「-」を0に変換
df['管理費'] = df['管理費'].replace('-',0)
df['敷金'] = df['敷金'].replace('-',0)
df['礼金'] = df['礼金'].replace('-',0)
df['保証金'] = df['保証金'].replace('-',0)
df['敷引,償却'] = df['敷引,償却'].replace('-',0)
#df['敷引'] = df['敷引'].replace('実費',0) #「実費」と文字列が入っている場合がある
#df['償却'] = df['償却'].replace('-',0)

#文字列から数値に変換
df['賃料'] = pd.to_numeric(df['賃料'])
df['管理費'] = pd.to_numeric(df['管理費'])
df['敷金'] = pd.to_numeric(df['敷金'])
df['礼金'] = pd.to_numeric(df['礼金'])
df['保証金'] = pd.to_numeric(df['保証金'])
df['敷引,償却'] = pd.to_numeric(df['敷引,償却'])
df['築年数'] = pd.to_numeric(df['築年数'])
df['専有面積'] = pd.to_numeric(df['専有面積'])
df['立地2'] = pd.to_numeric(df['立地2'])

#単位を合わせるために、管理費以外を10000倍。
df['賃料'] = df['賃料'] * 10000
df['敷金'] = df['敷金'] * 10000
df['礼金'] = df['礼金'] * 10000
df['保証金'] = df['保証金'] * 10000
df['敷引,償却'] = df['敷引,償却'] * 10000
