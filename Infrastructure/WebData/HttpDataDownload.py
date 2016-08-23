#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Script for data retriving

import pandas
import WebData
import datetime, time
import logging

def funcmap(dataSrc, dataType, dataTicker):
    if dataSrc == '163':
        if dataType == 'VALUE' or dataTicker.startswith('16'):
            return WebData.get_fund_value_data_163
        elif dataType == 'PRICE':
            return WebData.get_fund_price_data_163
        else:
            raise "Unknown datatype for 163: %s" %dataType
    elif dataSrc == 'GOOGLE':
        return WebData.get_data_google
                  
class DataFetcher(object):
     
    def __init__(self, tickerList, storePath, includeList=None, asc=True):
        # tickerList is a 3-column pandas data frame
        # including ticker, source, type=[PRICE/VALUE], start, end
        # by default wait 5 seconds for each retrieving
        self.tickerList  = tickerList
        self.storePath   = storePath
        self.includeList = includeList
        self.wait        = 5
        self.verbose     = True
        self.asc         = asc
        logging.basicConfig(filename=self.storePath+'/dataRetrieve.log',level=logging.DEBUG)
    
    def __enter__(self):
        logFile    = self.storePath+'/dataRetrieve.log'
        self.log   = logging.getLogger('DataFetcher')
        loghandler = logging.FileHandler(logFile)
        loghandler.setLevel(logging.DEBUG)
        self.log.addHandler(loghandler) 
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        handlers = self.log.handlers[:]
        for handler in handlers:
            handler.close()
            self.log.removeHandler(handler)
    
    @classmethod
    def fromTrancheFundInfoFile(cls, fIn='J:/Data/TrancheFundInfo.csv', storePath='J:/Data/TrancheFundDataFull', includeFile=None, asc=True):
        """ init from A-share tranche fund info file """
        df = pandas.read_csv(fIn, sep=',')
        baseTicker = df['baseTicker']
        aTicker    = df['aTicker']
        bTicker    = df['bTicker']
        startDate  = df['foundDate']
        startDate  = startDate.map(lambda x: datetime.date(*(time.strptime(x, "%Y/%m/%d")[0:3])))
        lenData    = 2 * (len(baseTicker) + len(aTicker) + len(bTicker))
        allDates   = pandas.concat([startDate, startDate, startDate, startDate, startDate, startDate])
        allTickers = pandas.concat([baseTicker, aTicker, bTicker, baseTicker, aTicker, bTicker]).map(lambda x: str(x))
        allTypes   = pandas.concat([pandas.Series(['PRICE'] * (len(baseTicker) * 3)), pandas.Series(['VALUE'] * (len(baseTicker) * 3))])
        indexData  = pandas.Series(xrange(lenData))
        allDates.index   = indexData
        allTickers.index = indexData
        allTypes.index   = indexData
        allDates.name    = "StartDate"
        allTickers.name  = "Ticker"
        allTypes.name    = "Type"
        tickerList = pandas.concat([allTickers, allTypes, allDates], axis=1)
        tickerList['Source']  = '163'
        tickerList['EndDate'] = datetime.date.today()
        incList = None
        if includeFile != None:
            incList = pandas.read_csv(includeFile)['Ticker'].map(lambda x: str(x))
        else:
            incList = allTickers
        return cls(tickerList = tickerList, storePath = storePath, includeList=incList, asc=asc)
    
    def run(self):
        for rowtuple in self.tickerList.iterrows():
            row         = rowtuple[1]
            if row['Ticker'] not in self.includeList.values:
                continue
            executeFunc = funcmap(row['Source'], row['Type'], row['Ticker'])
            execstr     = ' '.join([row['Ticker'], row['Source'], row['Type'], row['StartDate'].strftime('%Y-%m-%d'), row['EndDate'].strftime('%Y-%m-%d')])
            dfobj       = None
            try:
                dfobj = executeFunc(row['Ticker'], row['StartDate'], row['EndDate'], asc=self.asc)
            except:
                logging.warning('Failed: ' + execstr)
                if self.verbose:
                    print 'Failed: ' + execstr
                continue
            logging.info('Succeeded: ' + execstr)
            if self.verbose:
                print 'Succeeded: ' + execstr
            dfobj.to_csv(self.storePath+'/'+row['Ticker']+'_'+row['Type']+'.csv', date_format='%Y-%m-%d')
            if self.verbose:
                print 'Sleeping %d secs' % self.wait
            time.sleep(self.wait)           
    
if __name__ == "__main__":
    with DataFetcher.fromTrancheFundInfoFile() as df:
        df.run()
    
    
