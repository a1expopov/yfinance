#! /usr/bin/env python

import MySQLdb
import settings



def clean_setup():

    connection = MySQLdb.connect(
        user=settings.DATABASE['username'],
        passwd=settings.DATABASE['password'])

    
    setup_db = (
        'DROP DATABASE IF EXISTS `yfinance`;',
        'CREATE DATABASE `yfinance`;',
        'USE `yfinance`;')
    
    setup_tb = (
        '''
        CREATE TABLE `price_data` (
        `ticker` varchar(10) CHARACTER SET utf8 NOT NULL DEFAULT '',
        `date` date NOT NULL DEFAULT '0000-00-00',
        `close` double NOT NULL,
        PRIMARY KEY (`ticker`,`date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        ''',)
    
    cursor = connection.cursor()        
    
    try:
        for q in setup_db:
            cursor.execute(q)
        for q in setup_tb:
            cursor.execute(q)
        connection.commit()
    except:
        connection.rollback()
        raise
    
if __name__ == '__main__':
    proceed = raw_input(
        ':are you sure you want to create db and tables from scratch? (y/n): ')
    if proceed == 'y':
        clean_setup()
        
