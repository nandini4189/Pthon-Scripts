import requests 
import json
import io
import codecs
import pandas as pd


# api-endpoint 
URL = "https://api.sendgrid.com/api/bounces.get.json?api_user=graghavendra&api_key=Diversio3413&date=1"
  
# sending get request and saving the response as response object 
r = requests.get(url = URL)
  
# extracting data in json format 
data= r.json()
with open(r'C:\Users\tkalyan\Desktop\data2.json', 'w') as outfile:
    json.dump(data, outfile, indent=4, sort_keys=True, separators=(",", ':'))
df = pd.read_json (r'C:\Users\tkalyan\Desktop\data2.json')
df.to_csv (r'C:\Users\tkalyan\Desktop\bounces.csv', index = None) 
print("The data is stored in the csv format") 