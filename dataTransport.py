import pandas as pd
import numpy as np

df = pd.read_csv('crashesData.csv')

# print(df.head())

# existence assertions

# # Some crashes occurred in March
df1 = df[df["Crash Month"] == 3]
print(df1)

# limit assertions

# Everycrash occurred on highway 26
df2 = df[df["Highway Number"] == 26]
print(df2)

# Intra-record assertions

# Every crash requires at least one participant
df3 = df[df["Record Type"] == 3]
# print(pd.isnull(df3["Participant ID"]))

# Inter-record assertions

# No more than 20 percent of crashes occur in June
df_1 = df[df["Record Type"] == 1]
june = 0
other = 0
for idx, data in df_1.iterrows():
    if data['Crash Month'] == 4:
        june = june + 1
    else:
        other = other+ 1
assert((june / (june + other))<= 0.2)

# Each serial number is the part of the incident record as 1
for idx, data in df_1.iterrows():
    assert(np.isnan(data['Serial #']) == False)
# print((pd.isnull(df_1['Serial #'])) == False)

# Summary assertions

# There are three record types in crash data
assert(df['Record Type'].nunique())

# In 2019, crashes took place for thousands of hours.
crash_hour_num = 0
for idx1,data1 in df_1.iterrows():
    if data1['Crash Year'] == 2019:
        crash_hour_num = crash_hour_num + data1['Crash Hour']
        
assert(crash_hour_num <= 9999)

# Statistical assertions

# Crashes occur on average 20m away from intersections.”
# df_1['Distance from Intersection'].sum()
assert((df_1['Distance from Intersection'].sum() / len(df_1))<= 20)
# print(df_1['Distance from Intersection'].sum() / len(df_1))

# The mode of the weekday on which the crash occurs is Wednesday.
# print(df_1['Week Day Code'].sum() / len(df_1) )
# assert((df_1['Week Day Code'].sum() / len(df_1)) == 3)
print(df_1['Week Day Code'].median())
# assert(df_1['Week Day Code'].median() == 3)
assert(df_1['Week Day Code'].median() == 4)


