#crawl data from weather.com
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import date


url = 'https://weather.com/en-AU/weather/tenday/l/53f9d8c644bf04f648b53a297a23bcf53acb117cac8348eca4990e6466c61e55'
page = requests.get(url)
soup = BeautifulSoup(page.text, 'html.parser')

day = soup.find_all('h3', class_='DetailsSummary--daypartName--kbngc')
d1 = [pt.get_text() for pt in day]
d1[0]=date.today().strftime('%A')[:3] + ' ' + date.today().strftime('%d')

temp = soup.find_all('div', class_='DetailsSummary--temperature--1kVVp')
d2 = [pt.get_text() for pt in temp]

status = soup.find_all('span', class_='DetailsSummary--extendedData--307Ax')
d3 = [pt.get_text() for pt in status]

hum = soup.find_all('div', class_='DetailsSummary--precip--1a98O')
d4 = [pt.get_text()[4:] for pt in hum]

wind = soup.find_all('span', class_='Wind--windWrapper--3Ly7c undefined')
d5 = [pt.get_text() for pt in wind]

#create a spark dataframe from d1 d2 d3 d4 d5
df = spark.createDataFrame(zip(d1,d2,d3,d4,d5),['Day','Temperature','Status','Humidity','Wind'])
df.show()
#data= np.array([d1,d2,d3,d4,d5]).T
#columns=["Day","Temperature","Status","Humidity","Wind"]
#pdf=pd.DataFrame(data=data,columns=columns)
#print(pdf)

#save to delta lake with name current date
df.write.format("delta").mode("overwrite").save("/home/hadoop/WeatherData/"+date.today().strftime('%Y%m%d'))
#remove folder using dbutils
dbutils.fs.rm("/home/hadoop/WeatherData/"+date.today().strftime('%Y%m%d'),True)

// get the latest record,td tn datalake
val conf ■ spark.sparkContext.hadoopConftguratton
val fs ■ org.apache.hadoop.fs.FtleSysten.get(conf)
val exists - fs.extststnew org.apache.hadoop.fs.Path(s'/datalake/$tblName'))
val tblLocatton ■ s hdfs://localhost:9e«e/datalake/$tblHame'
var tblQuery ■ ••

data = [("Alice", 25, "F"), ("Bob", 30, "M"), ("Charlie", 35, "M")]
df = spark.createDataFrame(data, ["name", "age", "gender"])

val df = spark.read .option("header", true) .option("inferSchema", true) .csv("/home/codespace/file/product.csv")