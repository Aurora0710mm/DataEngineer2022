from cgi import print_arguments
from cmath import nan
import csv
from enum import Flag
import pandas as pd
import numpy as np
df = pd.read_csv('books.csv')

#Flitering
df_ES = df.drop(columns=['Edition Statement']);
# print(df_ES)

df_CA = df_ES.drop(columns=['Corporate Author']);
#print(df_CA)

df_CC = df_CA.drop(columns=['Corporate Contributors']);
df_FO = df_CC.drop(columns=['Former owner']);
df_Engraver = df_FO.drop(columns=['Engraver']);
df_IT = df_Engraver.drop(columns=['Issuance type']);
df_Shelfmarks = df_IT.drop(columns=['Shelfmarks']);

# print(df_Shelfmarks)

df_usecols = pd.read_csv('books.csv', usecols=['Identifier', 'Place of Publication', 'Date of Publication','Publisher', 'Title', 'Author', 'Contributors', 'Flickr URL'])

# print(df_usecols)

# print(df_Shelfmarks.columns)

#Tidying up the data
import re

# remove & convert date
df['Date of Publication'] = df['Date of Publication'].str.strip('[')
df_clean_2 = df['Date of Publication'].str.extract(r'(^\d{4})')
# print(df_clean_2.head(40))

#Tidying with applymap()
text = 'uniplaces.txt'

# df_time = pd.read_csv('')
# from collections import namedtuple
# Item = namedtuple('Item', 'state area')

df1 = pd.read_csv('uniplaces.txt',sep=';',names=['text'])

df1['State'] = df1.loc[df1.text.str.contains('[edit]', regex=False), 'text'].str.extract(r'(.*?)\[edit\]', expand=False)

df1['Region Name'] = df1.loc[df1.State.isnull(), 'text'].str.extract(r'(.*?)\s*[\(\[]+.*[\n]*', expand=False)

df1['Region e'] = df1.loc[df1.State.isnull(), 'text'].str.extract(r'((?<=\()[^\(\)]*(?=\)))', expand=False)

# df1['un_new'] = df1.loc[df1.State.isnull(),'text'].str.extract(r'(?<=\()[^\(\)]*(?=\))',expand=False)

df1['State'] = df1['State'].ffill()

# print(df1)


# Decoding
df_trimet = pd.read_csv('trimet_03411_2.csv')

# for idx, row in df_trimet.iterrows():
#     df_test = row[]
#     print(df_test)
total = 0

def get_num(test):
    global total
    if total < test:
        total = test
df_trimet['OCCURRENCES'].apply(get_num)
df_trimet_temp = df_trimet

for i in range(2,total):
    # is_bigger = df_trimet['OCCURRENCES'] 
    df_try = df_trimet[df_trimet['OCCURRENCES'] == i]
    df_trimet_temp = df_trimet_temp.append(df_try*i)

# print(df_trimet_temp.tail(5))

# Filling
df_trimet2 = df_trimet
df_trimet2['VALID_FLAG'] = df_trimet2['VALID_FLAG'].ffill()

# Interpolating
df_trimet3 = df_trimet
df_trimet3['ARRIVE_TIME'] = df_trimet3['ARRIVE_TIME'].interpolate(method='linear')

# More Transformations
df_temp = pd.melt(df_trimet,id_vars='VEHICLE_BREADCRUMB_ID')

# print(df_temp)

# H Transformation Visualizations
csv = '''
VEHICLE_BREADCRUMB_ID,VEHICLE_NUMBER,EQUIPMENT_CLASS,ARRIVE_DATETIME,SERVICE_DATE,ARRIVE_TIME,LONGITUDE,LATITUDE,DWELL,DISTANCE,SPEED, SATELLITES,HDOP,OCCURRENCES,VALID_FLAG,LAST_USER,LAST_TIMESTAMP

4313659883,3411,B,29OCT2021:07:23:25,29OCT2021:00:00:00,26605,-122.769718,45.516525,0,133221.79,21,10,0.9,1,Y,TRANS,31OCT2021:06:06:40
4313660036,3411,B,29OCT2021:07:59:12,29OCT2021:00:00:00,28752,-122.806785,45.532305,0,156112.21,29,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
4313660037,3411,B,29OCT2021:07:59:17,29OCT2021:00:00:00,28757,-122.80699,45.532863,0,156318.9,28,11,0.9,1,Y,TRANS,31OCT2021:06:06:40
4313660038,3411,B,29OCT2021:07:59:22,29OCT2021:00:00:00,28762,-122.807395,45.533377,0,156532.16,29,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
4313660039,3411,B,29OCT2021:07:59:27,29OCT2021:00:00:00,28767,-122.807992,45.533808,0,156748.69,30,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
4313660040,3411,B,29OCT2021:07:59:32,29OCT2021:00:00:00,28772,-122.808765,45.53413,0,156978.35,31,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
4313660041,3411,B,29OCT2021:07:59:37,29OCT2021:00:00:00,28777,-122.809647,45.534373,0,157221.13,33,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
4313660042,3411,B,29OCT2021:07:59:42,29OCT2021:00:00:00,28782,-122.810518,45.534652,0,157460.63,33,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
4313660043,3411,B,29OCT2021:07:59:47,29OCT2021:00:00:00,28787,-122.811272,45.535102,0,157713.26,34,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
4313660044,3411,B,29OCT2021:07:59:52,29OCT2021:00:00:00,28792,-122.811823,45.535717,0,157975.73,36,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
4313660045,3411,B,29OCT2021:07:59:57,29OCT2021:00:00:00,28797,-122.812058,45.536453,0,158251.32,38,12,0.9,1,Y,TRANS,31OCT2021:06:06:40
4313660046,3411,B,29OCT2021:08:00:02,29OCT2021:00:00:00,28802,-122.81204,45.537278,0,158549.87,41,12,0.9,1,Y,TRANS,31OCT2021:06:06:40
4313660047,3411,B,29OCT2021:08:00:07,29OCT2021:00:00:00,28807,-122.811832,45.538117,0,158854.99,42,12,0.8,1,Y,TRANS,31OCT2021:06:06:40
4313660048,3411,B,29OCT2021:08:00:12,29OCT2021:00:00:00,28812,-122.811262,45.53876,0,159127.3,37,12,0.8,1,Y,TRANS,31OCT2021:06:06:40
4313660049,3411,B,29OCT2021:08:00:17,29OCT2021:00:00:00,28817,-122.810712,45.539317,0,159370.08,33,11,0.8,1,Y,TRANS,31OCT2021:06:06:40
4313660050,3411,B,29OCT2021:08:00:22,29OCT2021:00:00:00,28822,-122.810432,45.539925,0,159599.74,31,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
4313660051,3411,B,29OCT2021:08:00:27,29OCT2021:00:00:00,28827,-122.8104,45.54054,0,159822.84,30,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
4313660052,3411,B,29OCT2021:08:00:32,29OCT2021:00:00:00,28832,-122.810405,45.541132,0,160036.09,29,12,0.8,1,Y,TRANS,31OCT2021:06:06:40
4313660053,3411,B,29OCT2021:08:00:37,29OCT2021:00:00:00,28837,-122.810412,45.54176,0,160265.75,31,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
4313660054,3411,B,29OCT2021:08:00:42,29OCT2021:00:00:00,28842,-122.810432,45.542432,0,160505.25,33,11,0.8,1,Y,TRANS,31OCT2021:06:06:40
4313660055,3411,B,29OCT2021:08:00:47,29OCT2021:00:00:00,28847,-122.810457,45.543055,0,160728.35,30,11,1.2,1,Y,TRANS,31OCT2021:06:06:40
4313660056,3411,B,29OCT2021:08:00:52,29OCT2021:00:00:00,28852,-122.810473,45.543675,0,160954.73,31,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
4313660057,3411,B,29OCT2021:08:00:57,29OCT2021:00:00:00,28857,-122.810498,45.544348,0,161197.51,33,12,0.7,1,Y,TRANS,31OCT2021:06:06:40
'''
import io
df_final = pd.read_csv(io.StringIO(csv))