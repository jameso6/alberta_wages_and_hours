import numpy as np
import pandas as pd
import plotly.express as px
import re
from stats_can import StatsCan

sc = StatsCan()
table_id_wages = '14100064' # hourly wages
table_id_hours = '14100037' # weekly hours # 
wages = sc.table_to_df(table_id_wages)
hours = sc.table_to_df(table_id_hours)

wages.drop(columns=['DGUID','UOM_ID','SCALAR_FACTOR','SCALAR_ID','VECTOR','COORDINATE','STATUS','SYMBOL','TERMINATED','DECIMALS'], inplace=True)
hours.drop(columns=['DGUID','UOM_ID','SCALAR_FACTOR','SCALAR_ID','VECTOR','COORDINATE','STATUS','SYMBOL','TERMINATED','DECIMALS'], inplace=True)

wages.drop(wages.loc[wages['UOM']=='Persons'].index, inplace=True)
hours.drop(hours.loc[hours['UOM']=='Persons'].index, inplace=True)

wages = wages[(wages['GEO'] == 'Alberta') & (wages['Wages'] == 'Average hourly wage rate') & (wages['Type of work'] == 'Both full- and part-time employees')]
hours = hours[(hours['GEO'] == 'Alberta') & (hours['Actual hours worked'] == 'Average actual hours (worked in reference week, main job)')]

wages.rename(columns={'UOM':'UOM_wages','VALUE':'Hourly Wage'}, inplace = True)
hours.rename(columns={'UOM':'UOM_hours','VALUE':'Weekly Hours'}, inplace = True)

hours.replace('Wholesale and retail trade\t\t [41, 44-45]','Wholesale and retail trade [41, 44-45]', inplace=True)

data = pd.merge(wages, hours, on = ['REF_DATE','GEO','North American Industry Classification System (NAICS)','Sex'], how = 'outer')

# Cleaning sector names and adding a year column

def remove_square_brackets(text):
    # Define a regular expression pattern to match text within square brackets
    pattern = r'\[.*?\]'
    # Use the sub() function from the re module to replace matches with an empty string
    result = re.sub(pattern, '', text)
    return result

def clean_sector_names():
    # Replacing all sector names with versions without the bracketed text
    for NAIC in data['North American Industry Classification System (NAICS)'].unique():
        data.replace(NAIC, remove_square_brackets(NAIC), inplace=True)

def clean_ref_date():
    # Replacing date format from timestamps to dates
    # data['REF_DATE'] = pd.to_datetime(data['REF_DATE']).astype('int64')
    # data['REF_DATE'] = pd.to_datetime(data['REF_DATE'], format="%Y-%m-%d")
    # for year in data['REF_DATE'].unique():
    data['YEAR'] = data['REF_DATE'].array.year

clean_sector_names()
clean_ref_date()

# Inserting living wage and minimum wage data
living_wages_data = { # from https://www.livingwage.ca/rates
    'YEAR': [2014, 2014, 2014, 2015, 2015, 2015, 2015, 2016, 2016, 2016, 2016, 2016,
             2017, 2017, 2017, 2017, 2017, 2018, 2018, 2018, 2018, 2018, 2019, 2019,
             2019, 2019, 2019, 2020, 2020, 2020, 2020, 2020, 2021, 2021, 2021, 2021,
             2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2022,
             2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022,
             2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023,
             2023, 2023, 2023],
    'LIVING_WAGE': [13, 15.5, 13, 17.29, 15.55, 13, 13.11, 18.15, 17.36, 17.35, 13,
                     13.11, 18.15, 16.69, 17.35, 13, 13.81, 18.15, 16.31, 17.31, 13.65,
                     13.81, 18.15, 16.31, 17.35, 13.65, 13.81, 18.15, 16.31, 17.35, 13.65,
                     13.81, 20.69, 22.65, 16.51, 17.35, 13.65, 19.43, 16.37, 15.41, 17.2,
                     17.42, 16.62, 17.59, 17.74, 20.25, 18.48, 22.4, 32.75, 22.35, 21.2,
                     21.4, 22.5, 20.3, 19.65, 21.85, 20.4, 20.7, 22.4, 19.05, 18.9, 23.7,
                     38.8, 19.55, 21.7, 24.9, 21.6, 22.25, 24.5, 17.35, 20.6, 18.75, 21.1,
                     21, 23.8]
}

living_wages = pd.DataFrame(living_wages_data)
living_wages = living_wages.groupby(['YEAR']).mean().reset_index()

minimum_wages_data = { # from: https://open.alberta.ca/dataset/0b2e7658-eef7-4ea4-b8f4-76d4238d4669/resource/6d241936-f628-4cc1-b60d-f50ca813105f/download/2015-albertas-minimum-wage-graph-2015-06.pdf
                       # and https://www.alberta.ca/minimum-wage-expert-panel
    'YEAR':[2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023],
    'MINIMUM_WAGE':[8.4, 8.8, 8.8, 9.4, 9.75, 9.95, 10.20, 11.2, 12.20, 13.60, 15, 15, 15, 15, 15, 15]
} 

minimum_wages = pd.DataFrame(minimum_wages_data)

def insert_wages():
    for year in data['YEAR'].unique():
        if minimum_wages[minimum_wages['YEAR'] == year]['MINIMUM_WAGE'].array.size > 0:
            data.loc[data['YEAR'] == year, 'MINIMUM_WAGE'] = minimum_wages[minimum_wages['YEAR'] == year]['MINIMUM_WAGE'].values[0]
        if living_wages[living_wages['YEAR'] == year]['LIVING_WAGE'].array.size > 0:
            data.loc[data['YEAR'] == year, 'LIVING_WAGE'] = living_wages[living_wages['YEAR'] == year]['LIVING_WAGE'].values[0]

insert_wages()

# print(data)

# Sectors into csvs

data.to_csv('.\Datasets\Weekly_wages_and_hours_by_sector.csv')

for NAIC in data['North American Industry Classification System (NAICS)'].unique():
    data[data['North American Industry Classification System (NAICS)'] == NAIC].reset_index().to_csv('.\\Datasets\\'+NAIC+'.csv')


