import time
from kafka import KafkaProducer
import yfinance as yf
from datetime import date
import json

current_date = date.today()
company = 'MSFT'
# issue with yfinance during none-trading hours
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
topic_name = 'stocks-demo'

while True:
    data = yf.download(tickers=company, start=current_date, interval='1mo')
    data = data.reset_index(drop=False)
    data['Date'] = data['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    my_dict = data.iloc[-1].to_dict()
    msg = json.dumps(my_dict)
    producer.send(topic_name, key=b'Tesla Stock Update', value=msg.encode())
    print(f"Producing to {topic_name}")
    producer.flush()
    time.sleep(120)



