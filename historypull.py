import requests, json
from datetime import datetime as dt
from datetime import timedelta
import json
import csv
#def pull_history():
csv_columns = ['km100.rpm10c', 'km100.rpm25c', 'km102.rhumid', 'km102.rtemp', 'km102.rtvoc (ppb)', 'rco2 (ppm)', 'ts', 'Location', 'Device']
csv_file = "entries.csv"
def write_headers():
    try:
        with open(csv_file, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
    except IOError:
        print("I/O error")        

    
def readHistory(*devs, start =  None, finish = None):
    key = 'MGIwMTQ5MmIzY2IzNDkwYWI1YjViZmUwMGE3MThhNjM3ZTA3'
    
    #url = 'https://api.kaiterra.cn/v1/sensedges/{}'.format(dev)
    r = requests.post(
        'https://api.kaiterra.cn/v1/batch?include_headers=false',
        params= {'key': key},
        headers = {'Content-Type': 'application/json', 'Accept-Encoding': 'gzip'},
        data = json.dumps(
            [{'method': 'GET', 'relative_url': 'sensedges/{}/history?series=raw&begin={}&end={}'.format(dev, start, finish)} for dev in devs]
            )
        )
    
    for response in r.json(): #json.loads(r.content)
        #print(response)
        yield json.loads(response['body'])

startDate =  '2019-11-01T00:00:00Z'
endDate = '2019-12-11T00:00:00Z'
#devs = ['740c82ab-e4bb-4cea-b018-4dc0c5db0747']
#devs = ['29fa3f23-2dfe-493a-b772-9e481622718d']
dateformat = '%Y-%m-%dT%H:%M:%SZ'
tdelta = dt.strptime(endDate,dateformat) - dt.strptime(startDate,dateformat)
chunksize = 3

chunks = int(tdelta.days / chunksize)

def load_config():
    with open('config.json') as f:
        json_data = json.load(f)
    return json_data

config = load_config()
for key in config['Locations']:
    print("Getting data for location: " + str(key))
    location_name = key
    location = config['Locations'][key]
    location_devices = [location['Config']['Device UUIDs'][i] for i in location['Config']['Device UUIDs']]
    print(location_devices)
    for i in range(chunks):
        readings = []
        pulldate = dt.strptime(startDate,dateformat) + (timedelta(days=chunksize) * i)
        chunkend = pulldate + timedelta(days=chunksize)
        for responses in readHistory(*location_devices, start = pulldate.strftime(dateformat), finish= chunkend.isoformat().split('.')[0]+'Z'):
            responseid = None
            for k in responses:
                if k == 'id':
                    responseid = responses[k]
                if k == 'data':
                    for p in responses[k]:
                        p['Device'] = responseid
                        p['Location'] = location_name
                        try:
                            with open(csv_file, 'a', newline='') as csvfile:   
                                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                                writer.writerow(p)
                        except IOError:
                            print("I/O error")     





   
