!pip install pysqlite3 
import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import requests
import hmac
import hashlib
import time
import sqlite3
import sqlite3
import urllib
from urllib.parse import urlparse, urlencode
from urllib.request import Request, urlopen


connection = sqlite3.connect('test12.db') #connection to the db
cursor = connection.cursor()
cursor.execute('CREATE TABLE last_checks(Id INTEGER PRIMARY KEY, exchange TEXT, trading_pair TEXT, duration TEXT, table_name TEXT, last_check INT);') #Creation of the table used to track if new candle were added




def List_of_crypto(): #return a list of all available cryptocurrency on Binance's exchange

  r = requests.get("https://api.binance.com/api/v3/exchangeInfo")
  results = r.json()
  coin_list=[]
  for i in results['symbols']:
    coin_list.append(i['baseAsset'])
  coinlist = sorted(set(coin_list))
  print(coinlist)
  return coinlist


def getDepth(direction, pair): #display ask or bid price of an asset
    Depth_url = base_url + "depth"
    r = requests.get(Depth_url, {"symbol":pair, "limit":5}).json()
    if( direction == 'bid'):
      print('5 last bid prices :')
      print(r["bids"])
    else:
      print('5 last ask price')
      print(r["asks"])
      
      
      
def Display_order_book(pair): #Get Order Book For an asset


  r = requests.get("https://api.binance.com/api/v3/depth",
                 params=dict(symbol=pair))
  results = r.json()
  frames = {side: pd.DataFrame(data=results[side], columns=["price", "quantity"],
                             dtype=float)
          for side in ["bids", "asks"]}
  frames_list = [frames[side].assign(side=side) for side in frames]
  data = pd.concat(frames_list, axis="index", 
                 ignore_index=True, sort=True)

  print(data)
  
  
  def refreshDataCandle(pair, duration): #Retrieve candle data for a pair and periodic time

  url= base_url + "klines"
  body = {"symbol": pair, "interval": duration}
  r = requests.get(url,body).json()
  print(r)
  
  
  def Filling(pair, duration, candle): #Will create table and/or update database depending of use case
  TableName = str('Binance_' + pair + "_" + duration)
  cursor.execute("SELECT name FROM sqlite_master WHERE type='table';") #Check if the table exist
  resp = cursor.fetchall()
  new_table = True
  for elem in resp:
    if TableName in elem :
      new_table = False
  if(new_table == False): #The table exist, check if update is needed

    query = "SELECT * from " + TableName
    cursor.execute(query)
    array_table = cursor.fetchall() #Retrieve every candles stored (used to determine the UID of the new candle if she needs to be added to the db
    query = "SELECT * from last_checks WHERE table_name = '%s'" %   ( str('Binance_' + pair + "_" + duration)) #get the timestamp of the last candle added
    cursor.execute(query)
    last_updated = cursor.fetchall()
    last_timestamp_stored = last_updated[0][5]
    print(last_timestamp_stored)
    if(last_timestamp_stored < candle[0]): #Comparison between last timestamp added and actual timestamp of the candle to see if the candle is a new one
      query = "INSERT INTO %s VALUES ( %i , %i, %f, %f , %f, %f, %f, %i, %f, %i) " % ( str('Binance_' + pair + "_" + duration), len(array_table) + 1, candle[0],float(candle[1]), float(candle[2]), float(candle[3]), float(candle[4]), float(candle[5]), candle[6], float(candle[7]), float(candle[8]))
      cursor.execute(query)
      query = "UPDATE last_checks SET last_check = %i WHERE table_name = '%s'" %   ( candle[0],str('Binance_' + pair + "_" + duration)) #update of both table
      cursor.execute(query)
      print("Update")
  else:
    print("creation of a new table") #New table
    tableCreationStatement = """CREATE TABLE """ + TableName + """( INTEGER PRIMARY KEY, date INT, open REAL, high REAL, low REAL, close REAL, volume REAL,closetime INT, quotevolume REAL, numberoftrqdes INT)"""
    cursor.execute(tableCreationStatement) #creation of the new table and insertion of the candle
    query = "INSERT INTO %s VALUES ( %i , %i, %f, %f , %f, %f, %f, %i, %f, %i) " % ( str('Binance_' + pair + "_" + duration), 1, candle[0],float(candle[1]), float(candle[2]), float(candle[3]), float(candle[4]), float(candle[5]), candle[6], float(candle[7]), float(candle[8]))
    cursor.execute(query)
    query = "SELECT * from last_checks"
    cursor.execute(query)
    length_update_array = len(cursor.fetchall()) + 1
    query = "INSERT INTO last_checks VALUES ( %i , 'Binance', '%s', '%s', '%s', %i) " % (length_update_array, str(pair), str(duration), str('Binance_' + pair + "_" + duration), candle[0] )
    cursor.execute(query)
    
    
   
  def candlModify(pair, duration): #retrieve the candle and call FIlling
    url= base_url + "klines"
    body = {"symbol": pair, "interval": duration}
    r = requests.get(url,body).json()
    Filling(pair,duration, r[len(r)-1])
    
    
  def Data_extract(pair): #Get the trade data of a pair
  url = base_url + 'trades'
  r = requests.get(url, {"symbol":pair, "limit":700}).json()
  return r


def RefreshData(pair): #Check if table exist and then add/update 500 trade information
  TableName = str('Data_Binance_' + pair)
  cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
  resp = cursor.fetchall()
  new_table = True
  for elem in resp:
    if TableName in elem :
      new_table = False
  if(new_table == True):
    query = """CREATE TABLE """ + TableName + """(Id INTEGER PRIMARY KEY, uuid TEXT, traded_btc REAL, price REAL, created_at_int INT)"""
    cursor.execute(query)
  query = "SELECT * from '%s'" % (TableName)
  cursor.execute(query)
  length_update_array = len(cursor.fetchall()) + 1
  data = Data_extract(pair)
  print(data[0])
  for elem in data:
    query = "INSERT INTO %s VALUES ( %i , '%s', %f, %f , %i)" % (TableName, length_update_array,elem['id'], float(elem['qty']), float(elem['price']), elem['time']  )
    length_update_array = length_update_array + 1
    cursor.execute(query)
    
    
def Create_order(api_key, secret_key, direction, price, amount, pair , type): #Create an order using api key and secret key
  url = base_url + 'order'
  headers = {'nonce': str(int(time.time())), 'X-MBX-APIKEY':api_key, 'Sign': ''}
  body = {"symbol":pair, "side": direction,"type":type, "quantity": amount, "price": price, "timestamp": int(time.time()*1000), "timeInForce": "GTC" }
  body = urllib.parse.urlencode(body).encode('utf-8')
  signature = hmac.new(secret_key, body, digestmod=hashlib.sha256) #creation of the signature
  headers['Sign'] = signature.hexdigest() #incorporation of the signature in the headers and body
  body = body.decode('utf-8') + "&signature="+str(signature.hexdigest())
  r = requests.post(
  url,data=body, headers=headers)
  print(r.json())
  
  
def Cancel_order(api_key, secret_key, id, pair): # cancel an already existing order
  url = base_url + 'order'
  headers = {'nonce': str(int(time.time())), 'X-MBX-APIKEY':api_key, 'Sign': ''}
  body = {"symbol":pair,"orderId":id, "timestamp": int(time.time()*1000) }
  body = urllib.parse.urlencode(body).encode('utf-8')
  signature = hmac.new(secret_key, body, digestmod=hashlib.sha256)
  headers['Sign'] = signature.hexdigest()
  body = body.decode('utf-8') + "&signature="+str(signature.hexdigest())
  r = requests.delete(
  url,data=body, headers=headers)
  print(r.json())
