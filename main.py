from kaiterra_client import KaiterraAPIClient
import pprint
import seaborn as sb
import numpy as np
import aqi
import csv
import time

def convert_to_aqi_enum(name):
    if name is "rpm10c":
            return aqi.POLLUTANT_PM10
    if name is "rpm25c":
            return aqi.POLLUTANT_PM25
    else:
        return None
       
        
        
    
client = KaiterraAPIClient(api_key='MGIwMTQ5MmIzY2IzNDkwYWI1YjViZmUwMGE3MThhNjM3ZTA3')
csv_file = "entries.csv"
csv_columns = ['Time', 'rco2', 'rhumid', 'rpm10c', 'rpm25c', 'rtemp', 'rtvoc', 'Aqi']

try:
    with open(csv_file, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
except IOError:
    print("I/O error")
        
#pprint.pprint(r[0]['rco2']['points'][0]['value'])
r = client.get_latest_sensor_readings([
        '/sensedges/740c82ab-e4bb-4cea-b018-4dc0c5db0747/history/',
    ])

pprint.pprint(r)

"""
while True:
    r = client.get_latest_sensor_readings([
        '/sensedges/740c82ab-e4bb-4cea-b018-4dc0c5db0747',
    ])
    pm10 = 0
    pm25 = 0
    datadict = {}

    for dev_name in r[0]:
        
        v = r[0][dev_name]['points'][0]['value']
        u = r[0][dev_name]['units']
        t = r[0][dev_name]['points'][0]['ts']
       # pprint.pprint(str(t))
        #pprint.pprint(str(measure) + ": " + str(v) + " " + str(u.value))
        aqt = convert_to_aqi_enum(dev_name)
        if aqt is aqi.POLLUTANT_PM10:
            pm10 = v
        elif aqt is aqi.POLLUTANT_PM25:
            pm25 = v
        datadict[dev_name] = v
        datadict['Time'] = t
            
            


    aqi_measurement = aqi.to_aqi([(aqi.POLLUTANT_PM10, str(pm10)),(aqi.POLLUTANT_PM25, str(pm25))])
    pprint.pprint("AQI:" + str(aqi_measurement))
    pprint.pprint(datadict)
    datadict['Aqi'] = float(aqi_measurement)

    try:
        with open(csv_file, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            #writer.writeheader()
            #for data in datadict:
            print("Writing " + str(datadict))
            writer.writerow(datadict)
    except IOError:
        print("I/O error")
    time.sleep(30)
        
#sb.set()
#sb.boxplot()
"""
