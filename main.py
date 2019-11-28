from kaiterra_client import KaiterraAPIClient
import pprint
import seaborn as sb
import numpy as np
import aqi
import csv


def convert_to_aqi_enum(name):
    if name is "rpm10c":
            return aqi.POLLUTANT_PM10
    if name is "rpm25c":
            return aqi.POLLUTANT_PM25
    else:
        return None
       
        
        
    
client = KaiterraAPIClient(api_key='MGIwMTQ5MmIzY2IzNDkwYWI1YjViZmUwMGE3MThhNjM3ZTA3')

r = client.get_latest_sensor_readings([
        '/sensedges/740c82ab-e4bb-4cea-b018-4dc0c5db0747',
])
#pprint.pprint(r[0]['rco2']['points'][0]['value'])
pm10 = 0
pm25 = 0

for i in r[0]:
    measure = i
    v = r[0][i]['points'][0]['value']
    u = r[0][i]['units']
    t = r[0][i]['points'][0]['ts']
    pprint.pprint(str(t))
    pprint.pprint(str(measure) + ": " + str(v) + " " + str(u.value))
    aqt = convert_to_aqi_enum(measure)
    if aqt is aqi.POLLUTANT_PM10:
        pm10 = v
    elif aqt is aqi.POLLUTANT_PM25:
        pm25 = v
        
        


aqi_measurement = aqi.to_aqi([(aqi.POLLUTANT_PM10, str(pm10)),(aqi.POLLUTANT_PM25, str(pm25))])
pprint.pprint("AQI:" + str(aqi_measurement))


#sb.set()
#sb.boxplot()
