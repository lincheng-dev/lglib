import datetime
import pandas as pd
import numpy as np
import bisect
from os import listdir
import pdb
import time
import functools
import MySQLdb
import WebData

class DataCrawler:
    beginDate = beginDate = datetime.date(1900, 1, 1)
    today = datetime.date.today()
    def __init__(self, ip, port, user_name, password, dbase):
        self.ip = ip
        self.port = port
        self.user_name = user_name
        self.password = password
        self.dbase = dbase

    def crawlData(self, dataSrc, ticker, beginDate=beginDate, endDate = today, mode="UnitValue"):
        if dataSrc == "163":
            if mode=="UnitValue":
                return WebData.get_fund_value_data_163(ticker=ticker, start=beginDate, end=endDate)
            elif mode=="Price":
                return WebData.get_fund_price_data_163(ticker=ticker, start=beginDate, end=endDate)
            else:
                raise Exception("unrecognized web data grab mode %s"%mode)
        else:
            raise Exception("dataSrc %s not supported yet"%(dataSrc))

    def getTableName(self, dataSrc):
        if dataSrc == "163":
            return "daily_prices_163"
        else:
            raise Exception("dataSrc %s not supported yet"%(dataSrc))

    def analyzeData(self, column_names, column_types, data):
        rows = []
        for row in data:
            dfRow = []
            for idx,item in enumerate(row):
                if column_types[idx].startswith("varchar"):
                    val = item
                elif column_types[idx].startswith("date"):
                    if isinstance(item, datetime.date):
                        val = item
                    elif isinstance(item, str):
                        val = datetime.datetime.strptime(item, "%Y-%m-%d")
                elif column_types[idx].startswith("decimal"):
                    if isinstance(item, float):
                        val = item
                    elif isinstance(item, str):
                        val = float(item)
                    elif item==None:
                        val = np.nan
                else:
                    raise Exception("unrecognized column type %s for column with name %s"%(column_types[idx], column_names[idx]))
                dfRow.append(val)
            rows.append(dfRow)
        df = pd.DataFrame(data=rows,columns=column_names)
        return df

    def getSqlConnection(self):
        conv = MySQLdb.converters.conversions.copy()
        conv[246] = float  # convert decimals to floats
        connector = MySQLdb.connect(self.ip, self.user_name, self.password, self.dbase, port=self.port, conv=conv)
        return connector

    def fetchDataFromDb(self, dataSrc, ticker):
        if dataSrc == "163":
            table_name = self.getTableName(dataSrc)
            sql = "select * from %s where Ticker=\'%s\';"%(table_name, ticker)
            connector = self.getSqlConnection()
            cursor = connector.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.execute("show columns from %s"%table_name)
            tableInfo = cursor.fetchall()
            column_names = [item[0] for item in tableInfo]
            column_types = [item[1] for item in tableInfo]
            cursor.close()
            connector.close()
            df = self.analyzeData(column_names, column_types, results)
            df = df.set_index(['Date'])
            return df
        else:
            raise Exception("dataSrc %s not supported yet" % (dataSrc))

    def updateDb(self, ticker, webData, indicesInsert, indicesUpdate, dataSrc, primaryKeys=["Ticker","Date"]):
        connector = self.getSqlConnection()
        cursor = connector.cursor()
        table_name = self.getTableName(dataSrc)
        headers = "Date, Ticker, "
        for colIdx,col in enumerate(webData.columns):
            headers += col
            if colIdx != len(webData.columns)-1:
                headers += ","

        for idx in indicesInsert:
            valStr = "\'%s\', \'%s\', "%(idx.strftime("%Y-%m-%d"), ticker)
            for colIdx, col in enumerate(webData.columns):
                val = webData.loc[idx,col]
                if isinstance(val, datetime.date):
                    valStr += val.strftime("%Y-%m-%d")
                else:
                    valStr += str(val)
                if colIdx != len(webData.columns)-1:
                    valStr += ","
            sql = "insert into %s (%s) values (%s);"%(table_name, headers, valStr)
            cursor.execute(sql)

        for idx in indicesUpdate:
            setStrList = []
            whereStrList = []
            for col in webData.columns:
                val = webData.loc[idx,col]
                if isinstance(val, datetime.date):
                    valStr = val.strftime("%Y-%m-%d")
                    valStr = "\'%s\'"%valStr
                elif isinstance(val, str):
                    valStr = "\'%s\'"%val
                else:
                    valStr = str(val)
                if col in primaryKeys:
                    whereStrList.append("%s=%s"%(col, valStr))
                else:
                    setStrList.append("%s=%s"%(col,valStr))
            whereStrList.append("Ticker=\'%s\'"%ticker)
            whereStrList.append("Date=\'%s\'" %(idx.strftime("%Y-%m-%d")))
            setStr = ",".join(setStrList)
            whereStr = " and ".join(whereStrList)
            sql = "update %s set %s where %s;"%(table_name,setStr, whereStr)
            cursor.execute(sql)
        connector.commit()
        connector.close()

    def verifyData(self, dbData, webData):
        indicesToUpdate = []
        indicesToInsert = []
        for idx_web in webData.index:
            if idx_web not in dbData.index:
                indicesToInsert.append(idx_web)
            else:
                for col in webData.columns:
                    if not np.isclose(webData.loc[idx_web, col], dbData.loc[idx_web, col], equal_nan=True):
                        indicesToUpdate.append(idx_web)
                        break
        return indicesToUpdate, indicesToInsert

    def fillDataBase(self, dataSrc, ticker):
        if dataSrc == "163":
            dbData = self.fetchDataFromDb(dataSrc, ticker)
            unitValWebData = self.crawlData(dataSrc, ticker, mode="UnitValue")
            unitValWebData.index = [d[0].date() for d in unitValWebData.index]
            indicesToUpdate, indicesToInsert = self.verifyData(dbData, unitValWebData)
            self.updateDb(ticker, unitValWebData, indicesToInsert, indicesToUpdate, dataSrc)
            priceWebData = self.crawlData(dataSrc, ticker, mode="Price")
            priceWebData.index = [d[0].date() for d in priceWebData.index]
            indicesToUpdate, indicesToInsert = self.verifyData(dbData, priceWebData)
            self.updateDb(ticker, priceWebData, indicesToInsert, indicesToUpdate, dataSrc)
        else:
            raise Exception("dataSrc %s not supported yet" % (dataSrc))

dataCrawler = DataCrawler("61.93.62.152",33061, "stratsdev", "makemoney2016", "stratsdev")
fundParam=pd.read_csv("fundInfo.csv", encoding='gbk')
fundParam=fundParam[['aTicker','bTicker','baseTicker']]
for idx in fundParam.index:
    aTicker = fundParam.loc[idx,"aTicker"]
    bTicker = fundParam.loc[idx, "bTicker"]
    baseTicker = fundParam.loc[idx, "baseTicker"]
    dataCrawler.fillDataBase("163", str(aTicker))
    dataCrawler.fillDataBase("163", str(bTicker))
    dataCrawler.fillDataBase("163", str(baseTicker))