import numpy as np
import os
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
from stats_can import StatsCan

# Load Data from Stats Canada
sc = StatsCan()
table_id_wages = '14100064' # hourly wages
table_id_hours = '14100037' # weekly hours
wages = sc.table_to_df(table_id_wages)
hours = sc.table_to_df(table_id_hours)

# Remove unnecessary columns
wages.drop(columns=['DGUID','UOM_ID','SCALAR_FACTOR','SCALAR_ID','VECTOR','COORDINATE','STATUS','SYMBOL','TERMINATED','DECIMALS'], inplace=True)
hours.drop(columns=['DGUID','UOM_ID','SCALAR_FACTOR','SCALAR_ID','VECTOR','COORDINATE','STATUS','SYMBOL','TERMINATED','DECIMALS'], inplace=True)

# Remove person counts
wages.drop(wages.loc[wages['UOM']=='Persons'].index, inplace=True)
hours.drop(hours.loc[hours['UOM']=='Persons'].index, inplace=True)

# Filter data to only AB, hourly, and weekly wage, and both full- and part-time
hourly_wages = wages[(wages['GEO'] == 'Alberta') & (wages['Wages'] == 'Average hourly wage rate') & (wages['Type of work'] == 'Both full- and part-time employees')]
weekly_wages = wages[(wages['GEO'] == 'Alberta') & (wages['Wages'] == 'Average weekly wage rate') & (wages['Type of work'] == 'Both full- and part-time employees')]

# Combine the above filtered datasets so that hourly and weekly wages are their own column
wages = pd.merge(hourly_wages, weekly_wages, on = ['REF_DATE','GEO','Type of work','North American Industry Classification System (NAICS)','Sex', 'Age group'], how = 'outer')
wages.drop(columns=['Wages_x', 'Wages_y', 'UOM_y'], inplace=True)

# Filter for working hours data
hours = hours[(hours['GEO'] == 'Alberta') & (hours['Actual hours worked'] == 'Average actual hours (worked in reference week, main job)')]

# Clean column names post merge
wages.rename(columns={'UOM_x':'UOM_wages','VALUE_x':'Hourly Wage','VALUE_y':'Weekly Wage'}, inplace = True)
hours.rename(columns={'UOM':'UOM_hours','VALUE':'Weekly Hours'}, inplace = True)
hours.replace('Wholesale and retail trade\t\t [41, 44-45]','Wholesale and retail trade [41, 44-45]', inplace=True)

# Merge all wage and working hours dataset into one
df = pd.merge(wages, hours, on = ['REF_DATE','GEO','North American Industry Classification System (NAICS)','Sex'], how = 'outer')

# Clean sector names and adding a year column
def remove_square_brackets(text):
    # Define a regular expression pattern to match text within square brackets
    pattern = r'\[.*?\]'
    # Use the sub() function from the re module to replace matches with an empty string
    result = re.sub(pattern, '', text)
    return result

def clean_sector_names():
    # Replacing all sector names with versions without the bracketed text
    for NAIC in df['North American Industry Classification System (NAICS)'].unique():
        df.replace(NAIC, remove_square_brackets(NAIC), inplace=True)

def clean_ref_date():
    df['YEAR'] = df['REF_DATE'].array.year

clean_sector_names()
clean_ref_date()

# Insert living wage and minimum wage data by first storing data into a dictionary
living_wages_data = { # from https://www.livingwage.ca/rates and https://datawrapper.dwcdn.net/FXpvY/27/
    'YEAR': [2014, 2014, 2014, 
             2015, 2015, 2015, 2015, 
             2016, 2016, 2016, 2016, 2016,
             2017, 2017, 2017, 2017, 2017, 
             2018, 2018, 2018, 2018, 2018, 
             2019, 2019, 2019, 2019, 2019, 
             2020, 2020, 2020, 2020, 2020, 
             2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 
             2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022, 2022,
             2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023, 2023],

    'CITY': ['Calgary', 'Grand Prairie', 'Medicine Hat', 
             'Calgary', 'Grand Prairie', 'Medicine Hat', 'Red Deer',
             'Calgary', 'Edmonton', 'Grand Prairie', 'Medicine Hat', 'Red Deer',
             'Calgary', 'Edmonton', 'Grand Prairie', 'Medicine Hat', 'Red Deer',
             'Calgary', 'Edmonton', 'Grand Prairie', 'Medicine Hat', 'Red Deer',
             'Calgary', 'Edmonton', 'Grand Prairie', 'Medicine Hat', 'Red Deer',
             'Calgary', 'Edmonton', 'Grand Prairie', 'Medicine Hat', 'Red Deer',
             'Calgary', 'Canmore', 'Chestermere', 'Cochrane', 'Drumheller', 'Edmonton',
             'Fort McMurray', 'Lethbridge', 'Red Deer', 'Rocky Mountain House', 'Stony Plain', 'Strathcona County',
             'Calgary', 'Canmore', 'Cochrane', 'Drumheller', 'Edmonton', 'Fort McMurray', 'Lethbridge', 'Red Deer',
             'Rocky Mountain House', 'Stony Plain', 'Spruce Grove', 'St. Albert',
             'Brooks', 'Grand Prairie', 'Calgary', 'Canmore', 'Drayton Valley', 'High River', 'Jasper', 'Lac La Biche County',
             'Edmonton', 'Fort McMurray', 'Medicine Hat', 'Lethbridge', 'Red Deer', 'Stony Plain', 'Spruce Grove', 'St. Albert'
             ],

    'PROVINCE': ['Alberta'] * 72,

    'LIVING_WAGE': [13, 15.5, 13, 
                    17.29, 15.55, 13, 13.11, 
                    18.15, 17.36, 17.35, 13, 13.11,
                    18.15, 16.69, 17.35, 13, 13.81, 
                    18.15, 16.31, 17.31, 13.65, 13.81, 
                    18.15, 16.31, 17.35, 13.65, 13.81, 
                    18.15, 16.31, 17.35, 13.65, 13.81, 
                    18.60, 37.40, 18.60, 22.60, 19.70, 18.10, 27.35, 19.00, 17.15, 18.05, 17.20, 16.80,
                    22.4, 32.75, 22.35, 21.2, 21.4, 22.5, 20.3, 19.65, 21.85, 20.4, 20.7, 22.4, 
                    19.05, 18.9, 23.7, 38.8, 19.55, 21.7, 24.9, 21.6, 22.25, 24.5, 17.35, 20.6, 18.75, 21.1, 21, 23.8]
}



# Create a dictionary of minimum wage data
minimum_wages_data = { # from: https://open.alberta.ca/dataset/0b2e7658-eef7-4ea4-b8f4-76d4238d4669/resource/6d241936-f628-4cc1-b60d-f50ca813105f/download/2015-albertas-minimum-wage-graph-2015-06.pdf
                       # and https://www.alberta.ca/minimum-wage-expert-panel
    'YEAR':[2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023],
    'MINIMUM_WAGE':[8.4, 8.8, 8.8, 9.4, 9.75, 9.95, 10.20, 11.2, 12.20, 13.60, 15, 15, 15, 15, 15, 15]
} 

# Create dataframe using dictionaries
living_wages_df = pd.DataFrame(living_wages_data)
living_wages = living_wages_df[['YEAR','LIVING_WAGE']].groupby(['YEAR']).mean().reset_index()
minimum_wages = pd.DataFrame(minimum_wages_data)

# Insert living and minimum wages into the main dataframe
def insert_wages():
    for year in df['YEAR'].unique():
        if minimum_wages[minimum_wages['YEAR'] == year]['MINIMUM_WAGE'].array.size > 0:
            df.loc[df['YEAR'] == year, 'MINIMUM_WAGE'] = minimum_wages[minimum_wages['YEAR'] == year]['MINIMUM_WAGE'].values[0]
        if living_wages[living_wages['YEAR'] == year]['LIVING_WAGE'].array.size > 0:
            df.loc[df['YEAR'] == year, 'LIVING_WAGE'] = living_wages[living_wages['YEAR'] == year]['LIVING_WAGE'].values[0]

insert_wages()

# print(df)
df.to_csv('Complete_Wages_and_Hours_by_sector.csv', index=False, encoding='utf-8')
living_wages_df.to_csv('Living_Wages_Map.csv', index=False, encoding='utf-8')

# Download CSV into a folder
# # Setting filepath
# # Get the current working directory (where your script is located)
# current_dir = os.path.dirname(os.path.abspath(__file__))

# folder_name = 'Datasets'

# # Create the full path of the output folder
# sub_folder = os.path.join(current_dir, folder_name)

# # Create a new directory if it doesn't exist
# if not os.path.exists(sub_folder):
#     os.makedirs(sub_folder)

# # Define the filename for CSV file
# csv_filename = 'Complete_Weekly_wages_and_hours_by_sector.csv'

# # Combine the current directory path and the filename
# # csv_path = os.path.join(current_dir, csv_filename)
# csv_path = os.path.join(sub_folder, csv_filename)

# # Save the DataFrame to CSV
# df.to_csv(csv_path, index=False, encoding='utf-8')

# # Redo for living wages map
# csv_filename = 'Living_Wages_Map.csv'

# # Combine the current directory path and the filename
# # csv_path = os.path.join(current_dir, csv_filename)
# csv_path = os.path.join(sub_folder, csv_filename)

# living_wages_df.to_csv(csv_path,  index=False, encoding='utf-8')

# # print(f"CSV file saved to: {csv_path}")

# # Download a separate CSV for each sector
# # Uncomment below if a CSV for each sector is needed
# # for NAIC in df['North American Industry Classification System (NAICS)'].unique():
# #     csv_filename = NAIC+'.csv'
# #     csv_path = os.path.join(sub_folder, csv_filename)
# #     df[df['North American Industry Classification System (NAICS)'] == NAIC].reset_index().to_csv(csv_path, index=False, encoding='utf-8')


###### Job Occupations script

# Connect to Job Bank website and retrieve HTML
page = requests.get('https://www.jobbank.gc.ca/wagereport/location/ab')
soup = BeautifulSoup(page.content)

# print(soup.prettify())

# Find all rows (tr) in the table body (tbody)
table = soup.find('tbody')
rows = table.find_all('tr')

# Initialize lists to store data
data = []
columns = ['Occupation', 'Low Wage', 'Median Wage', 'High Wage', 'Source']

# Iterate through rows, extract and append data to lists
for row in rows:
    cols = row.find_all('td')
    occupation = cols[0].text.strip()
    low_wage = cols[1].text.strip()
    median_wage = cols[2].text.strip()
    high_wage = cols[3].text.strip()
    source = cols[4].find('a')['href']  # assuming the source is in a link
    
    data.append([occupation, low_wage, median_wage, high_wage, source])

# Store data into a dataframe
df = pd.DataFrame(data, columns=columns)

# Clean text for job occupations
def clean_text(text):
    # Remove excess whitespace, newlines, and tabs
    cleaned_text = re.sub(r'\s+', ' ', text.strip())
    
    # Extract occupation and code using regex
    match = re.match(r'^(.*?)\s*\((\d+)\)$', cleaned_text)
    if match:
        occupation = match.group(1)
        code = match.group(2)
        return occupation, code
    else:
        return None, None
    
# Create two new columns to split and clean the 'Occupation column'    
df['Occupation Title'] = pd.Series()
df['NOC'] = pd.Series()

# Clean Occupations text
for row in df.index:
    # print(df.iloc[row]['Occupation'])
    df.loc[row, 'Occupation Title'], df.loc[row, 'NOC'] = clean_text(df.loc[row, 'Occupation'])

# Clean numerical texts
for column in ['Low Wage', 'Median Wage', 'High Wage']:
    for row in range(len(df[column])):
        df.loc[row, column] = df.loc[row, column].replace(',','')

# Remove missing values
df['Low Wage'] = [None if x == "N/A" else x for x in df['Low Wage']]
df['Median Wage'] = [None if x == "N/A" else x for x in df['Median Wage']]
df['High Wage'] = [None if x == "N/A" else x for x in df['High Wage']]

# Cast them as floats
df['Low Wage'] = df['Low Wage'].astype('float')
df['Median Wage'] = df['Median Wage'].astype('float')
df['High Wage'] = df['High Wage'].astype('float')

# Map Occupations to Industry Sectors using the below dictionary

occupation_sector_mapping = {'Legislators': 'Public administration [91]',
 'Senior government managers and officials': 'Public administration [91]',
 'Senior managers - financial, communications and other business services': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Senior managers - trade, broadcasting and other services': 'Services-producing sector',
 'Senior managers - construction, transportation, production and utilities': 'Construction [23]',
 'Financial managers': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Human resources managers': 'Professional, scientific and technical services [54]',
 'Purchasing managers': 'Professional, scientific and technical services [54]',
 'Other administrative services managers': 'Public administration [91]',
 'Insurance, real estate and financial brokerage managers': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Banking, credit and other investment managers': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Advertising, marketing and public relations managers': 'Professional, scientific and technical services [54]',
 'Other business services managers': 'Business, building and other support services [55-56]',
 'Telecommunication carriers managers': 'Professional, scientific and technical services [54]',
 'Financial auditors and accountants': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Financial and investment analysts': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Financial advisors': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Securities agents, investment dealers and brokers': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Other financial officers': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Human resources professionals': 'Professional, scientific and technical services [54]',
 'Professional occupations in business management consulting': 'Professional, scientific and technical services [54]',
 'Professional occupations in advertising, marketing and public relations': 'Professional, scientific and technical services [54]',
 'Supervisors, general office and administrative support workers': 'Business, building and other support services [55-56]',
 'Supervisors, finance and insurance office workers': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Supervisors, library, correspondence and related information workers': 'Public administration [91]',
 'Supervisors, supply chain, tracking and scheduling co-ordination occupations': 'Transportation and warehousing [48-49]',
 'Executive assistants': 'Business, building and other support services [55-56]',
 'Human resources and recruitment officers': 'Professional, scientific and technical services [54]',
 'Procurement and purchasing agents and officers': 'Professional, scientific and technical services [54]',
 'Conference and event planners': 'Professional, scientific and technical services [54]',
 'Employment insurance and revenue officers': 'Public administration [91]',
 'Court reporters, medical transcriptionists and related occupations': 'Other services (except public administration) [81]',
 'Health information management occupations': 'Health care and social assistance [62]',
 'Records management technicians': 'Public administration [91]',
 'Statistical officers and related research support occupations': 'Public administration [91]',
 'Accounting technicians and bookkeepers': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Insurance adjusters and claims examiners': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Insurance underwriters': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Assessors, business valuators and appraisers': 'Professional, scientific and technical services [54]',
 'Administrative officers': 'Public administration [91]',
 'Property administrators': 'Public administration [91]',
 'Payroll administrators': 'Professional, scientific and technical services [54]',
 'Administrative assistants': 'Business, building and other support services [55-56]',
 'Legal administrative assistants': 'Professional, scientific and technical services [54]',
 'Medical administrative assistants': 'Health care and social assistance [62]',
 'Customs, ship and other brokers': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Production and transportation logistics coordinators': 'Transportation and warehousing [48-49]',
 'General office support workers': 'Business, building and other support services [55-56]',
 'Receptionists': 'Business, building and other support services [55-56]',
 'Personnel clerks': 'Professional, scientific and technical services [54]',
 'Court clerks and related court services occupations': 'Public administration [91]',
 'Survey interviewers and statistical clerks': 'Public administration [91]',
 'Data entry clerks': 'Professional, scientific and technical services [54]',
 'Accounting and related clerks': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Banking, insurance and other financial clerks': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Collection clerks': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Library assistants and clerks': 'Public administration [91]',
 'Correspondence, publication and regulatory clerks': 'Public administration [91]',
 'Shippers and receivers': 'Transportation and warehousing [48-49]',
 'Storekeepers and partspersons': 'Wholesale and retail trade [41, 44-45]',
 'Production logistics workers': 'Manufacturing [31-33]',
 'Purchasing and inventory control workers': 'Professional, scientific and technical services [54]',
 'Dispatchers': 'Transportation and warehousing [48-49]',
 'Transportation route and crew schedulers': 'Transportation and warehousing [48-49]',
 'Engineering managers': 'Professional, scientific and technical services [54]',
 'Architecture and science managers': 'Professional, scientific and technical services [54]',
 'Computer and information systems managers': 'Professional, scientific and technical services [54]',
 'Physicists and astronomers': 'Professional, scientific and technical services [54]',
 'Chemists': 'Professional, scientific and technical services [54]',
 'Geoscientists and oceanographers': 'Professional, scientific and technical services [54]',
 'Meteorologists and climatologists': 'Professional, scientific and technical services [54]',
 'Other professional occupations in physical sciences': 'Professional, scientific and technical services [54]',
 'Biologists and related scientists': 'Professional, scientific and technical services [54]',
 'Forestry professionals': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Agricultural representatives, consultants and specialists': 'Agriculture [111-112, 1100, 1151-1152]',
 'Public and environmental health and safety professionals': 'Public administration [91]',
 'Architects': 'Professional, scientific and technical services [54]',
 'Landscape architects': 'Business, building and other support services [55-56]',
 'Urban and land use planners': 'Public administration [91]',
 'Land surveyors': 'Professional, scientific and technical services [54]',
 'Mathematicians, statisticians and actuaries': 'Professional, scientific and technical services [54]',
 'Data scientists': 'Professional, scientific and technical services [54]',
 'Cybersecurity specialists': 'Professional, scientific and technical services [54]',
 'Business systems specialists': 'Professional, scientific and technical services [54]',
 'Information systems specialists': 'Professional, scientific and technical services [54]',
 'Database analysts and data administrators': 'Professional, scientific and technical services [54]',
 'Computer systems developers and programmers': 'Professional, scientific and technical services [54]',
 'Software engineers and designers': 'Professional, scientific and technical services [54]',
 'Software developers and programmers': 'Professional, scientific and technical services [54]',
 'Web designers': 'Information, culture and recreation [51, 71]',
 'Web developers and programmers': 'Professional, scientific and technical services [54]',
 'Civil engineers': 'Construction [23]',
 'Mechanical engineers': 'Manufacturing [31-33]',
 'Electrical and electronics engineers': 'Manufacturing [31-33]',
 'Computer engineers (except software engineers and designers)': 'Professional, scientific and technical services [54]',
 'Chemical engineers': 'Professional, scientific and technical services [54]',
 'Industrial and manufacturing engineers': 'Manufacturing [31-33]',
 'Metallurgical and materials engineers': 'Manufacturing [31-33]',
 'Mining engineers': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Geological engineers': 'Professional, scientific and technical services [54]',
 'Petroleum engineers': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Other professional engineers': 'Professional, scientific and technical services [54]',
 'Chemical technologists and technicians': 'Manufacturing [31-33]',
 'Geological and mineral technologists and technicians': 'Professional, scientific and technical services [54]',
 'Biological technologists and technicians': 'Professional, scientific and technical services [54]',
 'Agricultural and fish products inspectors': 'Agriculture [111-112, 1100, 1151-1152]',
 'Forestry technologists and technicians': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Conservation and fishery officers': 'Agriculture [111-112, 1100, 1151-1152]',
 'Landscape and horticulture technicians and specialists': 'Business, building and other support services [55-56]',
 'Architectural technologists and technicians': 'Professional, scientific and technical services [54]',
 'Industrial designers': 'Manufacturing [31-33]',
 'Drafting technologists and technicians': 'Professional, scientific and technical services [54]',
 'Land survey technologists and technicians': 'Professional, scientific and technical services [54]',
 'Technical occupations in geomatics and meteorology': 'Professional, scientific and technical services [54]',
 'Computer network and web technicians': 'Professional, scientific and technical services [54]',
 'User support technicians': 'Professional, scientific and technical services [54]',
 'Information systems testing technicians': 'Professional, scientific and technical services [54]',
 'Non-destructive testers and inspectors': 'Manufacturing [31-33]',
 'Engineering inspectors and regulatory officers': 'Professional, scientific and technical services [54]',
 'Occupational health and safety specialists': 'Public administration [91]',
 'Construction inspectors': 'Construction [23]',
 'Civil engineering technologists and technicians': 'Professional, scientific and technical services [54]',
 'Mechanical engineering technologists and technicians': 'Manufacturing [31-33]',
 'Industrial engineering and manufacturing technologists and technicians': 'Manufacturing [31-33]',
 'Construction estimators': 'Construction [23]',
 'Electrical and electronics engineering technologists and technicians': 'Professional, scientific and technical services [54]',
 'Electronic service technicians (household and business equipment)': 'Services-producing sector',
 'Industrial instrument technicians and mechanics': 'Manufacturing [31-33]',
 'Aircraft instrument, electrical and avionics mechanics, technicians and inspectors': 'Manufacturing [31-33]',
 'Managers in health care': 'Health care and social assistance [62]',
 'Specialists in clinical and laboratory medicine': 'Health care and social assistance [62]',
 'Specialists in surgery': 'Health care and social assistance [62]',
 'General practitioners and family physicians': 'Health care and social assistance [62]',
 'Veterinarians': 'Health care and social assistance [62]',
 'Dentists': 'Health care and social assistance [62]',
 'Optometrists': 'Health care and social assistance [62]',
 'Audiologists and speech-language pathologists': 'Health care and social assistance [62]',
 'Pharmacists': 'Health care and social assistance [62]',
 'Dietitians and nutritionists': 'Health care and social assistance [62]',
 'Psychologists': 'Health care and social assistance [62]',
 'Chiropractors': 'Health care and social assistance [62]',
 'Physiotherapists': 'Health care and social assistance [62]',
 'Occupational therapists': 'Health care and social assistance [62]',
 'Kinesiologists and other professional occupations in therapy and assessment': 'Health care and social assistance [62]',
 'Other professional occupations in health diagnosing and treating': 'Health care and social assistance [62]',
 'Nursing coordinators and supervisors': 'Health care and social assistance [62]',
 'Registered nurses and registered psychiatric nurses': 'Health care and social assistance [62]',
 'Nurse practitioners': 'Health care and social assistance [62]',
 'Physician assistants, midwives and allied health professionals': 'Health care and social assistance [62]',
 'Opticians': 'Health care and social assistance [62]',
 'Licensed practical nurses': 'Health care and social assistance [62]',
 'Paramedical occupations': 'Health care and social assistance [62]',
 'Respiratory therapists, clinical perfusionists and cardiopulmonary technologists': 'Health care and social assistance [62]',
 'Animal health technologists and veterinary technicians': 'Health care and social assistance [62]',
 'Other technical occupations in therapy and assessment': 'Health care and social assistance [62]',
 'Denturists': 'Health care and social assistance [62]',
 'Dental hygienists and dental therapists': 'Health care and social assistance [62]',
 'Dental technologists and technicians': 'Health care and social assistance [62]',
 'Medical laboratory technologists': 'Health care and social assistance [62]',
 'Medical radiation technologists': 'Health care and social assistance [62]',
 'Medical sonographers': 'Health care and social assistance [62]',
 'Cardiology technologists and electrophysiological diagnostic technologists': 'Health care and social assistance [62]',
 'Pharmacy technicians': 'Health care and social assistance [62]',
 'Other medical technologists and technicians': 'Health care and social assistance [62]',
 'Traditional Chinese medicine practitioners and acupuncturists': 'Health care and social assistance [62]',
 'Massage therapists': 'Health care and social assistance [62]',
 'Other practitioners of natural healing': 'Health care and social assistance [62]',
 'Dental assistants and dental laboratory assistants': 'Health care and social assistance [62]',
 'Medical laboratory assistants and related technical occupations': 'Health care and social assistance [62]',
 'Nurse aides, orderlies and patient service associates': 'Health care and social assistance [62]',
 'Pharmacy technical assistants and pharmacy assistants': 'Health care and social assistance [62]',
 'Other assisting occupations in support of health services': 'Health care and social assistance [62]',
 'Government managers - health and social policy development and program administration': 'Public administration [91]',
 'Government managers - economic analysis, policy development and program administration': 'Public administration [91]',
 'Government managers - education policy development and program administration': 'Public administration [91]',
 'Other managers in public administration': 'Public administration [91]',
 'Administrators - post-secondary education and vocational training': 'Educational services [61]',
 'School principals and administrators of elementary and secondary education': 'Educational services [61]',
 'Managers in social, community and correctional services': 'Public administration [91]',
 'Commissioned police officers and related occupations in public protection services': 'Public administration [91]',
 'Fire chiefs and senior firefighting officers': 'Public administration [91]',
 'Commissioned officers of the Canadian Armed Forces': 'Public administration [91]',
 'Judges': 'Public administration [91]',
 'Lawyers and Quebec notaries': 'Professional, scientific and technical services [54]',
 'University professors and lecturers': 'Educational services [61]',
 'Post-secondary teaching and research assistants': 'Educational services [61]',
 'College and other vocational instructors': 'Educational services [61]',
 'Secondary school teachers': 'Educational services [61]',
 'Elementary school and kindergarten teachers': 'Educational services [61]',
 'Social workers': 'Health care and social assistance [62]',
 'Therapists in counselling and related specialized therapies': 'Health care and social assistance [62]',
 'Religious leaders': 'Other services (except public administration) [81]',
 'Police investigators and other investigative occupations': 'Public administration [91]',
 'Probation and parole officers': 'Public administration [91]',
 'Educational counsellors': 'Educational services [61]',
 'Career development practitioners and career counsellors (except education)': 'Other services (except public administration) [81]',
 'Natural and applied science policy researchers, consultants and program officers': 'Professional, scientific and technical services [54]',
 'Economists and economic policy researchers and analysts': 'Professional, scientific and technical services [54]',
 'Business development officers and market researchers and analysts': 'Professional, scientific and technical services [54]',
 'Social policy researchers, consultants and program officers': 'Professional, scientific and technical services [54]',
 'Health policy researchers, consultants and program officers': 'Health care and social assistance [62]',
 'Education policy researchers, consultants and program officers': 'Educational services [61]',
 'Recreation, sports and fitness policy researchers, consultants and program officers': 'Information, culture and recreation [51, 71]',
 'Program officers unique to government': 'Public administration [91]',
 'Other professional occupations in social science': 'Professional, scientific and technical services [54]',
 'Police officers (except commissioned)': 'Public administration [91]',
 'Firefighters': 'Public administration [91]',
 'Specialized members of the Canadian Armed Forces': 'Public administration [91]',
 'Paralegals and related occupations': 'Professional, scientific and technical services [54]',
 'Social and community service workers': 'Health care and social assistance [62]',
 'Early childhood educators and assistants': 'Health care and social assistance [62]',
 'Instructors of persons with disabilities': 'Educational services [61]',
 'Religion workers': 'Other services (except public administration) [81]',
 'Elementary and secondary school teacher assistants': 'Educational services [61]',
 'Other instructors': 'Educational services [61]',
 'Sheriffs and bailiffs': 'Public administration [91]',
 'Correctional service officers': 'Public administration [91]',
 'By-law enforcement and other regulatory officers': 'Public administration [91]',
 'Border services, customs, and immigration officers': 'Public administration [91]',
 'Operations Members of the Canadian Armed Forces': 'Public administration [91]',
 'Home child care providers': 'Health care and social assistance [62]',
 'Home support workers, caregivers and related occupations': 'Health care and social assistance [62]',
 'Primary combat members of the Canadian Armed Forces': 'Public administration [91]',
 'Student monitors, crossing guards and related occupations': 'Public administration [91]',
 'Library, archive, museum and art gallery managers': 'Public administration [91]',
 'Managers - publishing, motion pictures, broadcasting and performing arts': 'Information, culture and recreation [51, 71]',
 'Recreation, sports and fitness program and service directors': 'Information, culture and recreation [51, 71]',
 'Librarians': 'Public administration [91]',
 'Conservators and curators': 'Information, culture and recreation [51, 71]',
 'Archivists': 'Information, culture and recreation [51, 71]',
 'Editors': 'Information, culture and recreation [51, 71]',
 'Authors and writers (except technical)': 'Information, culture and recreation [51, 71]',
 'Technical writers': 'Professional, scientific and technical services [54]',
 'Journalists': 'Information, culture and recreation [51, 71]',
 'Translators, terminologists and interpreters': 'Professional, scientific and technical services [54]',
 'Producers, directors, choreographers and related occupations': 'Information, culture and recreation [51, 71]',
 'Conductors, composers and arrangers': 'Information, culture and recreation [51, 71]',
 'Musicians and singers': 'Information, culture and recreation [51, 71]',
 'Library and public archive technicians': 'Public administration [91]',
 'Film and video camera operators': 'Information, culture and recreation [51, 71]',
 'Graphic arts technicians': 'Manufacturing [31-33]',
 'Broadcast technicians': 'Information, culture and recreation [51, 71]',
 'Audio and video recording technicians': 'Information, culture and recreation [51, 71]',
 'Announcers and other broadcasters': 'Information, culture and recreation [51, 71]',
 'Other technical and coordinating occupations in motion pictures, broadcasting and the performing arts': 'Information, culture and recreation [51, 71]',
 'Graphic designers and illustrators': 'Information, culture and recreation [51, 71]',
 'Interior designers and interior decorators': 'Professional, scientific and technical services [54]',
 'Registrars, restorers, interpreters and other occupations related to museum and art galleries': 'Information, culture and recreation [51, 71]',
 'Photographers': 'Information, culture and recreation [51, 71]',
 'Motion pictures, broadcasting, photography and performing arts assistants and operators': 'Information, culture and recreation [51, 71]',
 'Dancers': 'Information, culture and recreation [51, 71]',
 'Painters, sculptors and other visual artists': 'Information, culture and recreation [51, 71]',
 'Theatre, fashion, exhibit and other creative designers': 'Information, culture and recreation [51, 71]',
 'Artisans and craftspersons': 'Goods-producing sector',
 'Athletes': 'Information, culture and recreation [51, 71]',
 'Coaches': 'Information, culture and recreation [51, 71]',
 'Program leaders and instructors in recreation, sport and fitness': 'Educational services [61]',
 'Corporate sales managers': 'Professional, scientific and technical services [54]',
 'Retail and wholesale trade managers': 'Wholesale and retail trade [41, 44-45]',
 'Restaurant and food service managers': 'Accommodation and food services [72]',
 'Accommodation service managers': 'Accommodation and food services [72]',
 'Managers in customer and personal services': 'Other services (except public administration) [81]',
 'Retail sales supervisors': 'Wholesale and retail trade [41, 44-45]',
 'Food service supervisors': 'Accommodation and food services [72]',
 'Executive housekeepers': 'Accommodation and food services [72]',
 'Accommodation, travel, tourism and related services supervisors': 'Accommodation and food services [72]',
 'Customer and information services supervisors': 'Business, building and other support services [55-56]',
 'Cleaning supervisors': 'Business, building and other support services [55-56]',
 'Other services supervisors': 'Business, building and other support services [55-56]',
 'Technical sales specialists - wholesale trade': 'Wholesale and retail trade [41, 44-45]',
 'Retail and wholesale buyers': 'Wholesale and retail trade [41, 44-45]',
 'Chefs': 'Accommodation and food services [72]',
 'Funeral directors and embalmers': 'Other services (except public administration) [81]',
 'Insurance agents and brokers': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Real estate agents and salespersons': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Financial sales representatives': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Cooks': 'Accommodation and food services [72]',
 'Butchers - retail and wholesale': 'Wholesale and retail trade [41, 44-45]',
 'Bakers': 'Accommodation and food services [72]',
 'Hairstylists and barbers': 'Services-producing sector',
 'Estheticians, electrologists and related occupations': 'Services-producing sector',
 'Upholsterers': 'Manufacturing [31-33]',
 'Retail salespersons and visual merchandisers': 'Wholesale and retail trade [41, 44-45]',
 'Sales and account representatives - wholesale trade (non-technical)': 'Wholesale and retail trade [41, 44-45]',
 'Tailors, dressmakers, furriers and milliners': 'Manufacturing [31-33]',
 "Maîtres d'hôtel and hosts/hostesses": 'Accommodation and food services [72]',
 'Bartenders': 'Accommodation and food services [72]',
 'Travel counsellors': 'Wholesale and retail trade [41, 44-45]',
 'Pursers and flight attendants': 'Transportation and warehousing [48-49]',
 'Airline ticket and service agents': 'Transportation and warehousing [48-49]',
 'Ground and water transport ticket agents, cargo service representatives and related clerks': 'Transportation and warehousing [48-49]',
 'Hotel front desk clerks': 'Accommodation and food services [72]',
 'Tour and travel guides': 'Educational services [61]',
 'Casino workers': 'Accommodation and food services [72]',
 'Outdoor sport and recreational guides': 'Information, culture and recreation [51, 71]',
 'Customer services representatives - financial institutions': 'Finance, insurance, real estate, rental and leasing [52-53]',
 'Postal services representatives': 'Transportation and warehousing [48-49]',
 'Other customer and information services representatives': 'Professional, scientific and technical services [54]',
 'Security guards and related security service occupations': 'Business, building and other support services [55-56]',
 'Cashiers': 'Wholesale and retail trade [41, 44-45]',
 'Service station attendants': 'Services-producing sector',
 'Store shelf stockers, clerks and order fillers': 'Wholesale and retail trade [41, 44-45]',
 'Other sales related occupations': 'Wholesale and retail trade [41, 44-45]',
 'Food and beverage servers': 'Accommodation and food services [72]',
 'Food counter attendants, kitchen helpers and related support occupations': 'Accommodation and food services [72]',
 'Meat cutters and fishmongers – retail and wholesale': 'Wholesale and retail trade [41, 44-45]',
 'Support occupations in accommodation, travel and facilities set-up services': 'Accommodation and food services [72]',
 'Operators and attendants in amusement, recreation and sport': 'Information, culture and recreation [51, 71]',
 'Pet groomers and animal care workers': 'Services-producing sector',
 'Other support occupations in personal services': 'Other services (except public administration) [81]',
 'Light duty cleaners': 'Business, building and other support services [55-56]',
 'Specialized cleaners': 'Services-producing sector',
 'Janitors, caretakers and heavy-duty cleaners': 'Services-producing sector',
 'Dry cleaning, laundry and related occupations': 'Services-producing sector',
 'Other service support occupations': 'Other services (except public administration) [81]',
 'Construction managers': 'Construction [23]',
 'Home building and renovation managers': 'Construction [23]',
 'Facility operation and maintenance managers': 'Business, building and other support services [55-56]',
 'Managers in transportation': 'Transportation and warehousing [48-49]',
 'Postal and courier services managers': 'Transportation and warehousing [48-49]',
 'Contractors and supervisors, machining, metal forming, shaping and erecting trades and related occupations': 'Construction [23]',
 'Contractors and supervisors, electrical trades and telecommunications occupations': 'Construction [23]',
 'Contractors and supervisors, pipefitting trades': 'Construction [23]',
 'Contractors and supervisors, carpentry trades': 'Construction [23]',
 'Contractors and supervisors, other construction trades, installers, repairers and servicers': 'Construction [23]',
 'Contractors and supervisors, mechanic trades': 'Construction [23]',
 'Contractors and supervisors, heavy equipment operator crews': 'Construction [23]',
 'Supervisors, printing and related occupations': 'Manufacturing [31-33]',
 'Supervisors, railway transport operations': 'Transportation and warehousing [48-49]',
 'Supervisors, motor transport and other ground transit operators': 'Transportation and warehousing [48-49]',
 'Supervisors, mail and message distribution occupations': 'Business, building and other support services [55-56]',
 'Machinists and machining and tooling inspectors': 'Manufacturing [31-33]',
 'Tool and die makers': 'Manufacturing [31-33]',
 'Sheet metal workers': 'Construction [23]',
 'Boilermakers': 'Manufacturing [31-33]',
 'Structural metal and platework fabricators and fitters': 'Manufacturing [31-33]',
 'Ironworkers': 'Construction [23]',
 'Welders and related machine operators': 'Manufacturing [31-33]',
 'Electricians (except industrial and power system)': 'Construction [23]',
 'Industrial electricians': 'Construction [23]',
 'Power system electricians': 'Utilities [22]',
 'Electrical power line and cable workers': 'Utilities [22]',
 'Telecommunications line and cable installers and repairers': 'Information, culture and recreation [51, 71]',
 'Telecommunications equipment installation and cable television service technicians': 'Information, culture and recreation [51, 71]',
 'Plumbers': 'Construction [23]',
 'Steamfitters, pipefitters and sprinkler system installers': 'Construction [23]',
 'Gas fitters': 'Utilities [22]',
 'Carpenters': 'Construction [23]',
 'Cabinetmakers': 'Manufacturing [31-33]',
 'Bricklayers': 'Construction [23]',
 'Insulators': 'Construction [23]',
 'Construction millwrights and industrial mechanics': 'Construction [23]',
 'Heavy-duty equipment mechanics': 'Construction [23]',
 'Heating, refrigeration and air conditioning mechanics': 'Construction [23]',
 'Railway carmen/women': 'Transportation and warehousing [48-49]',
 'Aircraft mechanics and aircraft inspectors': 'Manufacturing [31-33]',
 'Elevator constructors and mechanics': 'Construction [23]',
 'Automotive service technicians, truck and bus mechanics and mechanical repairers': 'Wholesale and retail trade [41, 44-45]',
 'Auto body collision, refinishing and glass technicians and damage repair estimators': 'Services-producing sector',
 'Oil and solid fuel heating mechanics': 'Construction [23]',
 'Appliance servicers and repairers': 'Services-producing sector',
 'Electrical mechanics': 'Utilities [22]',
 'Motorcycle, all-terrain vehicle and other related mechanics': 'Services-producing sector',
 'Other small engine and small equipment repairers': 'Services-producing sector',
 'Crane operators': 'Construction [23]',
 'Air pilots, flight engineers and flying instructors': 'Transportation and warehousing [48-49]',
 'Air traffic controllers and related occupations': 'Transportation and warehousing [48-49]',
 'Railway traffic controllers and marine traffic regulators': 'Transportation and warehousing [48-49]',
 'Other technical trades and related occupations': 'Services-producing sector',
 'Concrete finishers': 'Construction [23]',
 'Tilesetters': 'Construction [23]',
 'Plasterers, drywall installers and finishers and lathers': 'Construction [23]',
 'Roofers and shinglers': 'Construction [23]',
 'Glaziers': 'Construction [23]',
 'Painters and decorators (except interior decorators)': 'Construction [23]',
 'Floor covering installers': 'Construction [23]',
 'Residential and commercial installers and servicers': 'Construction [23]',
 'General building maintenance workers and building superintendents': 'Construction [23]',
 'Pest controllers and fumigators': 'Services-producing sector',
 'Other repairers and servicers': 'Services-producing sector',
 'Transport truck drivers': 'Transportation and warehousing [48-49]',
 'Bus drivers, subway operators and other transit operators': 'Transportation and warehousing [48-49]',
 'Railway and yard locomotive engineers': 'Transportation and warehousing [48-49]',
 'Railway conductors and brakemen/women': 'Transportation and warehousing [48-49]',
 'Heavy equipment operators': 'Construction [23]',
 'Printing press operators': 'Manufacturing [31-33]',
 'Drillers and blasters - surface mining, quarrying and construction': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Mail and parcel sorters and related occupations': 'Transportation and warehousing [48-49]',
 'Letter carriers': 'Transportation and warehousing [48-49]',
 'Couriers and messengers': 'Transportation and warehousing [48-49]',
 'Railway yard and track maintenance workers': 'Transportation and warehousing [48-49]',
 'Air transport ramp attendants': 'Transportation and warehousing [48-49]',
 'Automotive and heavy truck and equipment parts installers and servicers': 'Wholesale and retail trade [41, 44-45]',
 'Utility maintenance workers': 'Utilities [22]',
 'Public works maintenance equipment operators and related workers': 'Public administration [91]',
 'Longshore workers': 'Transportation and warehousing [48-49]',
 'Material handlers': 'Transportation and warehousing [48-49]',
 'Construction trades helpers and labourers': 'Construction [23]',
 'Other trades helpers and labourers': 'Construction [23]',
 'Taxi and limousine drivers and chauffeurs': 'Transportation and warehousing [48-49]',
 'Delivery service drivers and door-to-door distributors': 'Transportation and warehousing [48-49]',
 'Railway and motor transport labourers': 'Transportation and warehousing [48-49]',
 'Public works and maintenance labourers': 'Public administration [91]',
 'Managers in natural resources production and fishing': 'Agriculture [111-112, 1100, 1151-1152]',
 'Managers in agriculture': 'Agriculture [111-112, 1100, 1151-1152]',
 'Supervisors, mining and quarrying': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Contractors and supervisors, oil and gas drilling and services': 'Utilities [22]',
 'Agricultural service contractors and farm supervisors': 'Agriculture [111-112, 1100, 1151-1152]',
 'Contractors and supervisors, landscaping, grounds maintenance and horticulture services': 'Construction [23]',
 'Underground production and development miners': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Oil and gas well drillers, servicers, testers and related workers': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Logging machinery operators': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Underground mine service and support workers': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Oil and gas well drilling and related workers and services operators': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Chain saw and skidder operators': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Silviculture and forestry workers': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Specialized livestock workers and farm machinery operators': 'Agriculture [111-112, 1100, 1151-1152]',
 'Livestock labourers': 'Agriculture [111-112, 1100, 1151-1152]',
 'Harvesting labourers': 'Agriculture [111-112, 1100, 1151-1152]',
 'Nursery and greenhouse labourers': 'Agriculture [111-112, 1100, 1151-1152]',
 'Mine labourers': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Oil and gas drilling, servicing and related labourers': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Logging and forestry labourers': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Landscaping and grounds maintenance labourers': 'Business, building and other support services [55-56]',
 'Manufacturing managers': 'Manufacturing [31-33]',
 'Utilities managers': 'Utilities [22]',
 'Supervisors, mineral and metal processing': 'Forestry, fishing, mining, quarrying, oil and gas [21, 113-114, 1153, 2100]',
 'Supervisors, petroleum, gas and chemical processing and utilities': 'Utilities [22]',
 'Supervisors, food and beverage processing': 'Manufacturing [31-33]',
 'Supervisors, plastic and rubber products manufacturing': 'Manufacturing [31-33]',
 'Supervisors, forest products processing': 'Manufacturing [31-33]',
 'Supervisors, electronics and electrical products manufacturing': 'Manufacturing [31-33]',
 'Supervisors, other mechanical and metal products manufacturing': 'Manufacturing [31-33]',
 'Supervisors, other products manufacturing and assembly': 'Manufacturing [31-33]',
 'Power engineers and power systems operators': 'Utilities [22]',
 'Water and waste treatment plant operators': 'Utilities [22]',
 'Central control and process operators, petroleum, gas and chemical processing': 'Utilities [22]',
 'Machine operators, mineral and metal processing': 'Manufacturing [31-33]',
 'Glass forming and finishing machine operators and glass cutters': 'Manufacturing [31-33]',
 'Concrete, clay and stone forming operators': 'Construction [23]',
 'Inspectors and testers, mineral and metal processing': 'Manufacturing [31-33]',
 'Metalworking and forging machine operators': 'Manufacturing [31-33]',
 'Machining tool operators': 'Manufacturing [31-33]',
 'Machine operators of other metal products': 'Manufacturing [31-33]',
 'Chemical plant machine operators': 'Manufacturing [31-33]',
 'Plastics processing machine operators': 'Manufacturing [31-33]',
 'Rubber processing machine operators and related workers': 'Manufacturing [31-33]',
 'Sawmill machine operators': 'Manufacturing [31-33]',
 'Pulp mill, papermaking and finishing machine operators': 'Manufacturing [31-33]',
 'Lumber graders and other wood processing inspectors and graders': 'Manufacturing [31-33]',
 'Woodworking machine operators': 'Manufacturing [31-33]',
 'Other wood processing machine operators': 'Manufacturing [31-33]',
 'Textile fibre and yarn, hide and pelt processing machine operators and workers': 'Manufacturing [31-33]',
 'Weavers, knitters and other fabric making occupations': 'Manufacturing [31-33]',
 'Industrial sewing machine operators': 'Manufacturing [31-33]',
 'Inspectors and graders, textile, fabric, fur and leather products manufacturing': 'Manufacturing [31-33]',
 'Process control and machine operators, food and beverage processing': 'Manufacturing [31-33]',
 'Industrial butchers and meat cutters, poultry preparers and related workers': 'Manufacturing [31-33]',
 'Testers and graders, food and beverage processing': 'Manufacturing [31-33]',
 'Plateless printing equipment operators': 'Manufacturing [31-33]',
 'Camera, platemaking and other prepress occupations': 'Manufacturing [31-33]',
 'Binding and finishing machine operators': 'Manufacturing [31-33]',
 'Motor vehicle assemblers, inspectors and testers': 'Manufacturing [31-33]',
 'Electronics assemblers, fabricators, inspectors and testers': 'Manufacturing [31-33]',
 'Assemblers and inspectors, electrical appliance, apparatus and equipment manufacturing': 'Manufacturing [31-33]',
 'Assemblers, fabricators and inspectors, industrial electrical motors and transformers': 'Manufacturing [31-33]',
 'Mechanical assemblers and inspectors': 'Manufacturing [31-33]',
 'Machine operators and inspectors, electrical apparatus manufacturing': 'Manufacturing [31-33]',
 'Furniture and fixture assemblers, finishers, refinishers and inspectors': 'Manufacturing [31-33]',
 'Assemblers and inspectors of other wood products': 'Manufacturing [31-33]',
 'Plastic products assemblers, finishers and inspectors': 'Manufacturing [31-33]',
 'Industrial painters, coaters and metal finishing process operators': 'Manufacturing [31-33]',
 'Other products assemblers, finishers and inspectors': 'Manufacturing [31-33]',
 'Labourers in mineral and metal processing': 'Manufacturing [31-33]',
 'Labourers in metal fabrication': 'Manufacturing [31-33]',
 'Labourers in chemical products processing and utilities': 'Utilities [22]',
 'Labourers in wood, pulp and paper processing': 'Manufacturing [31-33]',
 'Labourers in rubber and plastic products manufacturing': 'Manufacturing [31-33]',
 'Labourers in textile processing and cutting': 'Manufacturing [31-33]',
 'Labourers in food and beverage processing': 'Manufacturing [31-33]',
 'Other labourers in processing, manufacturing and utilities': 'Utilities [22]'}

# Add the mapped sectors to the main dataframe
df['Sector'] = pd.Series()
df['Sector'] = df['Occupation Title'].map(occupation_sector_mapping)

# Clean Sector name text by removing brackets
def remove_square_brackets(text):
    # Define a regular expression pattern to match text within square brackets
    pattern = r'\[.*?\]'
    # Use the sub() function from the re module to replace matches with an empty string
    result = re.sub(pattern, '', text)
    return result

def clean_sector_names():
    # Replacing all sector names with versions without the bracketed text
    for Sector in df['Sector'].unique():
        df.replace(Sector, remove_square_brackets(Sector), inplace=True)

clean_sector_names()

df.to_csv('Complete_Occupations.csv', index=False, encoding='utf-8')

# Download CSV into a folder
# # Set filepath
# # Get the current working directory (where your script is located)
# current_dir = os.path.dirname(os.path.abspath(__file__))

# folder_name = 'Datasets'

# # Create the full path of the output folder
# sub_folder = os.path.join(current_dir, folder_name)

# # Create a new directory if it doesn't exist
# if not os.path.exists(sub_folder):
#     os.makedirs(sub_folder)

# # Define the filename for your CSV file
# csv_filename = 'Complete_Occupations.csv'

# # Combine the current directory path and the filename
# # csv_path = os.path.join(current_dir, csv_filename)
# csv_path = os.path.join(sub_folder, csv_filename)

# # Save the DataFrame to CSV
# df.to_csv(csv_path, index=False, encoding='utf-8')

# # print(f"CSV file saved to: {csv_path}")
