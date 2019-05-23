import requests
import pandas as pd
from requests_html import HTML, HTMLSession
from pandas.io.json import json_normalize
import datetime
import time
import random
import sys

curr = datetime.datetime.today()
diff = datetime.timedelta(days = 2)
result = curr - diff
year = str(int(result.strftime('%Y/%m/%d')[:4]))
month = str(int(result.strftime('%Y/%m/%d')[5:7]))
day = str(int(result.strftime('%Y/%m/%d')[8:10]))
#print(f'{year}/{month}/{day}/')

"""
# Execution control
e = '2019-05-24'
if str(curr)[:10] == e:
    print("Fatal execution error.")
    time.sleep(5)
    sys.exit(1)
print(str(curr)[:10])
# End of control block
"""

base_url = 'https://www.flightstats.com/v2/api-next/flight-tracker/arr/MAD/'#2019/5/7/6'

current_date = datetime.datetime.today()
back_in_time = current_date - datetime.timedelta(days = 2) # the queries dates must be two days before the current day.

year = str(int(result.strftime('%Y/%m/%d')[:4]))
month = str(int(result.strftime('%Y/%m/%d')[5:7]))
day = str(int(result.strftime('%Y/%m/%d')[8:10]))

date_of_query = f'{year}/{month}/{day}/'
session = HTMLSession()
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}
url_list = []
hours = (0, 6, 12, 18) # <-- hours groups values to iterate over.
for hour in hours:
    url_list.append(f'{base_url}{date_of_query}{str(hour)}')
#print(url_list)

print("Phase 1/2 of execution starts at:", datetime.datetime.today())
jsonData_h0 = requests.get(url_list[0], headers=headers).json()
time.sleep(5)
h0_full_json_df = json_normalize(jsonData_h0['data']['flights'])

jsonData_h6 = requests.get(url_list[1], headers=headers).json()
time.sleep(8)
h6_full_json_df = json_normalize(jsonData_h6['data']['flights'])

jsonData_h12 = requests.get(url_list[2], headers=headers).json()
time.sleep(6)
h12_full_json_df = json_normalize(jsonData_h12['data']['flights'])

jsonData_h18 = requests.get(url_list[3], headers=headers).json()
time.sleep(2)
h18_full_json_df = json_normalize(jsonData_h18['data']['flights'])
print("Phase 1/2 of execution ends at:", datetime.datetime.today())

code_shared_filter = h0_full_json_df['isCodeshare'] != True
h0_main_flights_df = h0_full_json_df[code_shared_filter]
h0_main_flights_df.reset_index(drop = True, inplace = True)

code_shared_filter = h6_full_json_df['isCodeshare'] != True
h6_main_flights_df = h6_full_json_df[code_shared_filter]
h6_main_flights_df.reset_index(drop = True, inplace = True)

code_shared_filter = h12_full_json_df['isCodeshare'] != True
h12_main_flights_df = h12_full_json_df[code_shared_filter]
h12_main_flights_df.reset_index(drop = True, inplace = True)

code_shared_filter = h18_full_json_df['isCodeshare'] != True
h18_main_flights_df = h18_full_json_df[code_shared_filter]
h18_main_flights_df.reset_index(drop = True, inplace = True)

h24_main_flights_df = pd.DataFrame().append(h0_main_flights_df, ignore_index = True).append(h6_main_flights_df, ignore_index = True).append(h12_main_flights_df, ignore_index = True).append(h18_main_flights_df, ignore_index = True)

h24_main_flights_df['full_url_path'] = "https://www.flightstats.com/v2" + h24_main_flights_df['url']

print("Total flights to track in this execution:", h24_main_flights_df.shape[0])

individual_flights = tuple(h24_main_flights_df['full_url_path'])

request_response_FFF = []
actual_departure_time_FFF = []
actual_arriving_time_FFF = []
flight_counter = 0

print("Phase 2/2 of execution starts at:", datetime.datetime.today())

for flight_url in individual_flights:

    flight_data = session.get(flight_url, headers=headers)
    flight_counter += 1
    print(f'Getting flight {flight_counter}')
    
    try:
        request_response_FFF.append(str(flight_data.status_code))
    except:
        request_response_FFF.append('error')
    
    actual_times_finder = flight_data.html.find('div.text-helper__TextHelper-s8bko4a-0')

    try:
        actual_departure_time_FFF.append(actual_times_finder[16].text[:5])
    except:
        actual_departure_time_FFF.append('error')
        
    try:
        actual_arriving_time_FFF.append(actual_times_finder[29].text[:5])
    except:
        actual_arriving_time_FFF.append('error')
    
    time.sleep(random.randint(2, 4))

h24_main_flights_df['fetching_response'] = request_response_FFF
h24_main_flights_df['actual_departure_time_FFF'] = actual_departure_time_FFF
h24_main_flights_df['actual_arriving_time_FFF'] = actual_arriving_time_FFF

print("Phase 2/2 of execution ends at:", datetime.datetime.today())

data_file = str(back_in_time)[:10]

h24_main_flights_df.to_csv(f'{data_file}_MAD_FlightsInfo.csv')

print("Done")
ending = input("Press Enter/Intro key to exit...")


