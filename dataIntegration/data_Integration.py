from cgitb import reset
from operator import ge
from re import X
from shelve import DbfilenameShelf
from sre_parse import State
from numpy import count_nonzero
import pandas as pd
from pytz import country_names
import matplotlib.pyplot as plt

df_county = pd.read_csv("acs2017_census_tract_data.csv")

df_covid = pd.read_csv("COVID_county_data.csv")

df_county['PovertyPopulation'] = df_county['TotalPop']*(df_county['Poverty']/100)
df_county['IncomePerCap'] = df_county['IncomePerCap']*df_county['TotalPop']
County_info = df_county.groupby(['State','County']).agg({'TotalPop':'sum','PovertyPopulation':'sum','IncomePerCap':'sum'}).reset_index()

County_info['Poverty'] = (County_info['PovertyPopulation']/County_info['TotalPop']) * 100

County_info['PerCapitalIncome'] = (County_info['IncomePerCap'])/County_info['TotalPop']

# County_info_new = County_info.drop(columns=['PovertyPopulation'])
County_info_new = County_info.drop(['PovertyPopulation', 'IncomePerCap'], axis=1)
# group_1.index.name = 'ID'

County_info_new.rename(columns={'TotalPop' : 'Population'}, inplace = True)
# print(County_info_new)

print(County_info_new.query('County=="Loudoun County" & State=="Virginia"'))
print(County_info_new.query('County=="Washington County" & State=="Oregon"'))
print(County_info_new.query('County=="Harlan County" & State=="Kentucky"'))
print(County_info_new.query('County=="Malheur County" & State=="Oregon"'))

# The most populous county in the USA
print(County_info_new['Population'].idxmax())
print(County_info_new.loc[204])

# The least populous county in the USA
print(County_info_new['Population'].idxmin())
print(County_info_new.loc[2751])





df_covid['date'] = pd.to_datetime(df_covid['date'])
df_covid['date']=pd.DatetimeIndex(df_covid['date']).to_period('M')
Covid_monthly = df_covid.groupby(['date','state','county']).agg({'cases':'sum','deaths':'sum'}).reset_index()


# print(Covid_info)

print(Covid_monthly[(Covid_monthly['county'] == 'Malheur') & (Covid_monthly['state'] == 'Oregon') & (Covid_monthly['date'] == '2020-08')])

print(Covid_monthly[(Covid_monthly['county'] == 'Malheur') & (Covid_monthly['state'] == 'Oregon') & (Covid_monthly['date'] == '2021-01')])

print(Covid_monthly[(Covid_monthly['county'] == 'Malheur') & (Covid_monthly['state'] == 'Oregon') & (Covid_monthly['date'] == '2021-02')])


# Covid_monthly   
# County_info_new


# print(Covid_monthly)
# print(County_info_new)

County_info_new['County'] = County_info_new['County'].str.replace('\s[^\s]*$','',regex=True)
County_info_new['State_County'] = County_info_new['State'] + County_info_new['County']

# print(County_info_new)

total_df = Covid_monthly.groupby(['state','county']).agg({'cases':'sum','deaths':'sum'}).reset_index()
total_df['State_County'] = total_df['state'] + total_df['county']

county_new = County_info_new.join(total_df.set_index('State_County'),on='State_County')

county_new = county_new.drop(['State_County', 'state','county'], axis=1)

county_new['TotalCasesPer100k'] = county_new['cases'] / (county_new['Population'] / 100000)
county_new['TOtalDeathsPer100k'] = county_new['deaths'] / (county_new['Population'] / 100000)


county_new.rename(columns={'cases' : 'TotalCases' , 'deaths':'TotalDeaths'}, inplace = True)


# print(county_new)

print(county_new[(county_new['County'] == 'Washington') & (county_new['State'] == 'Oregon') ])

print(county_new[(county_new['County'] == 'Malheur') & (county_new['State'] == 'Oregon') ])

print(county_new[(county_new['County'] == 'Loudoun') & (county_new['State'] == 'Virginia') ])

print(county_new[(county_new['County'] == 'Harlan') & (county_new['State'] == 'Kentucky') ])



r1 = county_new['TotalCases'].corr(county_new['Poverty'])

r = county_new['TotalCasesPer100k'].corr(county_new['Poverty'])

r = county_new['TotalCasesPer100k'].corr(county_new['Poverty'])

r = county_new['TotalCasesPer100k'].corr(county_new['Poverty'])



df_oregon = county_new[county_new['State'] == 'Oregon']

question1 = df_oregon['Poverty'].corr(df_oregon['TotalCases'])

question2 = county_new['Population'].corr(county_new['TotalCases'])
ax = county_new.plot.scatter(x="Population", y="TotalCases")

question3 = df_oregon['PerCapitalIncome'].corr(df_oregon['TotalDeaths'])

question4 = county_new['PerCapitalIncome'].corr(county_new['TotalCases'])

question5 = county_new['TotalCases'].corr(county_new['TotalCasesPer100k'])
# print(question1, question2, question3, question4)
# print(plt.show())
# print(question5)

# print(county_new)







