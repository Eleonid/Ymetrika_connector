# -*- coding: utf-8 -*-
"""
Created on Mon Dec 25 12:33:08 2017

@author: leonid.enov
"""

# File for downloading data from Yandex Metrika and uploading it to Google Bigquery

# Import libs to interact with Google Bigquery and Yandex Metrika API, create and manage files
import requests
from google.cloud import bigquery
import json
import csv
from datetime import datetime, date, time, timedelta

# Create client, dataset and table objects
client = bigquery.Client(project='XXXX-XXXX')
dataset = client.dataset('yandex_metrika')
table = dataset.table('DATASET_NAME_XXXX')

# Save ouath token and Yandex Metrika counter Id
atoken = 'XXXXXXXXXXXXXX-XXXXXXXXXXXXXXXX'
counterId = 'XXXXXXXX'

# Init necessary dimensions and metrics
dimensions = 'ym:s:bounce,ym:s:visitDurationInterval,ym:s:UTMSource,ym:s:UTMMedium,ym:s:UTMCampaign,ym:s:UTMContent,ym:s:deviceCategory,ym:s:gender,ym:s:ageInterval'
metrics = 'ym:s:visits,ym:s:users,ym:s:pageviews,ym:s:goal29069974visits,ym:s:goal23897010visits,ym:s:goal23897015visits,ym:s:goal17429755visits,ym:s:ecommercePurchases'

# Init industry, client and site info to add it to the API responce
industry = 'INDUSTRY_NAME'
Client = 'CLIENT_NAME'        # Don't name "client" variables in lower case! That's a reserved variable name, name it "Client"
site = 'SITE_NAME'

# Function to make and API request to Yandex Metrika and parse the responce to CSV-ready arrays
def metrikaInvocation(dateFrom, dateTo):  
    resultArr = [['industry','client','site','date','isBounce','time_on_site','utm_source','utm_medium','utm_campaign','utm_content','device_category','gender','age','visits','users','pageviews','goal_add_to_cart','goal_visit_cart','goal_checkout','goal_order','transactions']]
    row = []
    r = requests.get('https://api-metrika.yandex.ru/stat/v1/data?&id=' + counterId + '&accuracy=full&date1='+ dateFrom + '&date2=' + dateTo + '&metrics=' + metrics + '&dimensions=' + dimensions + '&limit=100000' + '&oauth_token=' + atoken)
    returnedJson = json.loads(r.text)

    for i in range(0, len(returnedJson['data'])):
        if row != []:
            resultArr.append(row)
        row = []
        row.insert(0, industry)
        row.insert(1, Client)
        row.insert(2, site)
        row.insert(3, dateFrom.replace("-",""))
        for j in range(0, len(returnedJson['data'][i]['dimensions'])):
            if returnedJson['data'][i]['dimensions'][j]['name'] != None:
                row.append(str(returnedJson['data'][i]['dimensions'][j]['name']))
            else:
                row.append('null')
        for k in range(0, len(returnedJson['data'][i]['metrics'])):
            row.append(returnedJson['data'][i]['metrics'][k])  
    return resultArr

# Download yesterday's data from Yandex Metrika
date_now = datetime.now()
date_actual = date_now + timedelta(days=-1)
date_actual_as_date = date_actual.strftime('%Y-%m-%d')

result = metrikaInvocation(date_actual_as_date,date_actual_as_date)

# Write result to CSV file
with open("PATH_TO_FILE", 'w', encoding="utf-8") as output:
    writer = csv.writer(output, lineterminator='\n', delimiter=',')
    writer.writerows(result)
    print(writer)
    
# Upload actual file to Google Bigquery

# Create load job configuration
job_config = bigquery.LoadJobConfig()
job_config.source_format = 'CSV'
job_config.skip_leading_rows = 1
job_config.write_disposition = 'WRITE_APPEND'
job_config.create_disposition = 'CREATE_IF_NEEDED'
job_config.autodetect = True

# Select actual file as csv_file and run an upload job
with open('PATH_TO_FILE', 'r+b') as csv_file:
    job = client.load_table_from_file(
        csv_file, table, job_config=job_config)  # API request
