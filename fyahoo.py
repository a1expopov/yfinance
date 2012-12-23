#! /usr/bin/env python

import re
import urllib
import urllib2
import csv
import MySQLdb
import sys
import settings


def _fetch_hist_pg(ticker):
    url = 'http://finance.yahoo.com/q/hp?' + \
          urllib.urlencode({'s':'{} Historical Prices'.format(ticker)})
    return urllib2.urlopen(url).read()

    
def _fetch_hist_data_raw(ticker):
    page = _fetch_hist_pg(ticker)
    m = re.search(r'http://ichart\.finance\.yahoo\.com/table\.csv.*\.csv', page)
    try:
        hist_data = urllib2.urlopen(m.group(0)).read().split('\n')
        for row in hist_data:
            yield row
    except AttributeError:
        raise Exception('Price data for {} not found!'.format(ticker))
    except:
        raise

        
def _fetch_hist_data(ticker):
    stream = _fetch_hist_data_raw(ticker)
    return csv.DictReader(stream)
    

def _cache_data(ticker, cursor):
    hist_data = [(ticker, row['Date'], row['Close']) \
                 for row in _fetch_hist_data(ticker)]
    cursor.executemany('''
        INSERT INTO price_data (ticker, date, close)
        VALUES (%s, %s, %s)
        ''', hist_data)  


def process_request(ticker):
    '''
    Retrieve full range of historical data for a ticker.
    If data for ticker is not already stored on local
    instance of MySQL, then go out and retrieve it from yahoo finance,
    storing the data in MySQL for subsequent retrieval.
    '''  
    connection = MySQLdb.connect(
        user=settings.DATABASE['username'],
        passwd=settings.DATABASE['password'],
        db=settings.DATABASE['name'])
    cursor = connection.cursor()
    
    cursor.execute('''
        SELECT EXISTS (SELECT * FROM price_data WHERE ticker = %s)
        ''', ticker)
    exists = cursor.fetchone()[0]
    
    if not exists:
        _cache_data(ticker, cursor)
        connection.commit()
    
    query = '''
        SELECT ticker, date, close
        FROM price_data
        WHERE ticker = %s
        '''
    cursor.execute(query, ticker)
    return cursor.fetchall()
            
    
