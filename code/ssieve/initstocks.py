import sys
import os
import datetime, time
import urllib
import requests
import numpy as np
import pandas as pd
import sqlite3
import pandas.io.sql as pdsql
from bs4 import BeautifulSoup as bsoup

## CUSTOM
import core

def initializeStockDB(db_name):
	"""
	Control logic for initializing 
	a DB of all stocks on the 
	NASDAQ and NYSE.

	You must pass a db file name 
	with the form "NAME.db" to the 
	interpreter when invoking this module.

	Returns True if successful, 
	otherwise False and error message.
	"""
	try:
		print( '\nStock data collection started: {now}\n'.format(now=datetime.datetime.now().strftime("%I:%M:%S")) )

		# set up variables 
		cwd = os.getcwd() # cwd 
		exchanges = ('NSDAQ','NYSE') # get the db started
		full_db_path = cwd + '/db/' + db_name # set the db depot

		# instantiate coreStocks() class and connect to DB
		all_stocks = core.coreStocks(full_db_path)
		all_stocks.connectToDB(all_stocks.dbcnx)
 
		# download a current list of all stocks
		stock_list = all_stocks.getAllCurStocks(exchanges)
		stock_list = stock_list[['Symbol', 'Market']]

		# create the key table
		all_stocks.createSymbolsKeyTable(stock_list)

		# populate all of the stocks
            # available in full software

		# log the results as a file
		log = ''.join([cwd, '/output/initialize/init_results_', datetime.date.today().strftime("%B_%d_%Y"), '.txt'])
		
        # log results
		with open(log, 'w') as f:
			for msgs in results:
				if isinstance(msgs, str):
					f.write(str(msgs) + '\n')
				else:
					for msg in msgs:
						f.write(str(msg) + '\n')
					f.write('\n') # separate each section
			f.close()

		# close the db
		all_stocks.closeDBConnection(all_stocks.dbcnx[0])

		# send the db path to console for reference
		print( 'Completed initialization: {now}'.format(now=datetime.datetime.now().strftime("%I:%M:%S")) )
		print('\nWrote DB to: ' + full_db_path + '\n')

		return True

	except Exception as e:
		return False, e

# to do: implement https://palletsprojects.com/p/click/ 
if __name__ == '__main__':
	try:
		# check to make sure cmd line argument passed
		assert len(sys.argv) >= 2, "Missing an argument. Please supply a db name to write to with the format NAME.db"
		# check to make sure cmd line arg has proper format
		if '.db' not in sys.argv[1]:
			raise ValueError('No db connection passed with script. Try again.')
		initializeStockDB(sys.argv[1])
			
	except Exception as e:
		print(False, e)
