#! /usr/bin/env python

"""
The purpose of this module is to allow a user to query historic (unadjusted)
close data for a ticker. The class YahooFinance is the point of contact for
retrieving that data.

If the start date and end date are not specified then pull the entire date
range for the ticker.

When a request for data is made, the code checks whether the request can be
satisfied from the data stored at a locally running MySQL instance. If it can,
return it. If not, fetch the data from finance.yahoo, store it into the DB,
and then return it to the caller.

Example usage below. (Assumes database and tables are already set up)

>>> from datetime import date
>>> end_date = date(2012, 12, 21)
>>> server = YahooFinance()
>>> spy_data = server.get_close_data('SPY', end_date=end_date)
>>> len(spy_data)
5013
"""

import re
import urllib
import urllib2
import csv
import datetime
import MySQLdb
import sys

import settings


class YahooFinance(object):

    def __init__(self):
        self.connection = MySQLdb.connect(
            user=settings.DATABASE['username'],
            passwd=settings.DATABASE['password'],
            db=settings.DATABASE['name'])
        self.cursor = None
        
    def get_close_data(self, ticker, start_date=None, end_date=None):
        
        if start_date is None:
            start_date = datetime.date(1970, 1, 1)
        if end_date is None:
            end_date = datetime.date(
                *datetime.datetime.now().timetuple()[:3])
        
        self.cursor = self.connection.cursor()
        
        self.cursor.execute('''
            SELECT MAX(date) FROM price_data WHERE ticker = %s
            ''', ticker)
        max_date = self.cursor.fetchone()[0]
        
        if max_date is None:
            self._cache_request(ticker)
        elif end_date > max_date:            
            update_date = max_date + datetime.timedelta(days=1)
            self._cache_request(ticker, update_date)  
        
        query = '''
            SELECT ticker, date, close
            FROM price_data
            WHERE ticker = %s
                AND date BETWEEN %s AND %s
            '''
        self.cursor.execute(query, (ticker, start_date, end_date))
        request_data = self.cursor.fetchall()
        self.cursor.close()
        return request_data
                
    def mk_url(self, ticker, start_date=None, end_date=None):
        '''
        The a, b, c parameters of the URL correspond to the month, day, and year
        of the start date. The d, e, f parameters correspond to the same for the end
        date.
        s is for the ticker we are looking for.
        '''
        def dt_info(dt):
            return dt.month - 1, dt.day, dt.year
        
        if not start_date is None:
            a, b, c = dt_info(start_date)
        else:
            a, b, c = (0, ) * 3
            
        if not end_date is None:
            d, e, f = dt_info(end_date)
        else:
            d, e, f = (9999, ) * 3
        
        params = {
            's': '{} Historical Prices'.format(ticker),
            'a': a,
            'b': b,
            'c': c,
            'd': d,
            'e': e,
            'f': f}
        
        url = 'http://finance.yahoo.com/q/hp?' + urllib.urlencode(params)
        return url
    
    def _get_data_link(self, page):
        m = re.search(
            r'http://ichart\.finance\.yahoo\.com/table\.csv.*\.csv', page)
        return m.group(0)
        
    def _fetch_hist_data(self, ticker, start_date, end_date):
        
        url = self.mk_url(ticker, start_date, end_date)
        page = urllib2.urlopen(url).read()
        
        try:
            data_link = self._get_data_link(page)
        except AttributeError:
            raise Exception('Check that {} is a valid ticker!'.format(ticker))
            
        hist_data = urllib2.urlopen(data_link).read().split('\n')
        for row in csv.DictReader(hist_data):
            yield row
            
    def _cache_request(self, ticker, start_date=None, end_date=None):
        hist_data = [(ticker, row['Date'], row['Close']) \
            for row in self._fetch_hist_data(ticker, start_date, end_date)]
        self.cursor.executemany('''
            INSERT INTO price_data (ticker, date, close)
            VALUES (%s, %s, %s)
            ''', hist_data)  
        self.connection.commit()

        
