import yfinance as yf
import s3fs

def run_stock_etl():
    tickerSymbol = 'AAPL'

    tickerData = yf.Ticker(tickerSymbol)

    tickerDf = tickerData.history(period='1d', start='2020-1-1', end='2021-1-1')

    tickerDf.to_csv('s3://sudha-airflow-bucket/AAPL_historical_data.csv')
