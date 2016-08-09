import WebData as web
import datetime
import pandas as pd
import bisect
from os import listdir
import pdb
import time
import functools

def filterWeight(x):
    wStr=str(x['abWeight'])
    idx=wStr.index(":")
    aWeight=float(wStr[:idx])
    bWeight=float(wStr[idx+1:])
    return pd.Series(data=[aWeight,bWeight])

DATE_ZERO = datetime.date(1900,1,1)
TODAY = datetime.date.today()

fundParam=pd.read_csv(r"H:\StrategyDevelopment\data\fundInfo.csv", encoding='gbk')
fundParam=fundParam[['aTicker','bTicker','baseTicker','abWeight']]
weightDf=fundParam.apply(filterWeight,axis=1)
fundParam=fundParam.astype(str)
fundParam=fundParam.assign(aWeight=weightDf[0])
fundParam=fundParam.assign(bWeight=weightDf[1])
fundParam.index=fundParam['aTicker']
fundParam=fundParam.drop('aTicker',axis=1)
fundParam=fundParam.drop('abWeight',axis=1)
baseDataSrc = "163"
aDataSrc = "163"
bDataSrc = "163"
fundParam=fundParam.assign(aDataSrc=aDataSrc)
fundParam=fundParam.assign(bDataSrc=bDataSrc)
fundParam=fundParam.assign(baseDataSrc=baseDataSrc)

class DataGrabber:

    grabberByDataSrc = {}

    def __init__(self, dataSrc):
        self.dataSrc = dataSrc.lower()
        self.PATH_SEP='\\'
        if self.dataSrc == "google":
            self.local_file_path = "H:\\StrategyDevelopment\\data\\googleData"
        elif self.dataSrc == "hexun":
            self.local_file_path = "H:\\StrategyDevelopment\\data\\hexunDataCpy"
        elif self.dataSrc == "163":
            self.local_file_path = "H:\\StrategyDevelopment\\data\\163Data"
        elif self.dataSrc == "yahoo":
            raise Exception("dataSrc yahoo not implemented")
        elif self.dataSrc == "tonglian":
            self.local_file_path = "H:\\StrategyDevelopment\\data\\tonglianData"
            raise Exception("dataSrc tonglian not implemented")
        else:
            raise Exception("unrecognized data src %s"%self.dataSrc)
        self.localPathByTicker = self.getLocalPathByTicker()

    def getLocalPathByTicker(self):
        dataSrcMap = {}
        dataFileList = listdir(self.local_file_path)
        for file_name in dataFileList:
            if file_name[-4:]==".csv":
                ticker = file_name[:6]
                dataSrcMap[ticker] = self.local_file_path + self.PATH_SEP + file_name
        return dataSrcMap

    def grabLocalData(self, ticker, startDate, endDate, suffix=""):
        if self.dataSrc == "google":
            localDataFile = self.localPathByTicker[ticker]
            df = pd.read_csv(localDataFile)
            df.index=[datetime.datetime.strptime(item.replace("/","-"), "%Y-%m-%d").date() for item in df.iloc[:,0]]
            if df.index.duplicated().any():
                raise Exception("ticker %s has multiple values on date %s"%(ticker, df.index.get_duplicates()[0].strftime("%Y-%m-%d")))
            df = df[["Close"]]
            df = df.add_suffix(suffix)
            df = df.sort_index()
        elif self.dataSrc == "hexun":
            localDataFile = self.localPathByTicker[ticker]
            df = pd.read_csv(localDataFile)
            df.index=[datetime.datetime.strptime(item.replace("/","-"), "%Y-%m-%d").date() for item in df.iloc[:,0]]
            if df.index.duplicated().any():
                raise Exception("ticker %s has multiple values on date %s"%(ticker, df.index.get_duplicates()[0].strftime("%Y-%m-%d")))
            df = df[["UnitValue"]]
            df.columns = ["Close"]
            df = df.add_suffix(suffix)
            df = df.sort_index()
        elif self.dataSrc == "163":
            localDataFile = self.localPathByTicker[ticker]
            df = pd.read_csv(localDataFile)
            df.index=[datetime.datetime.strptime(item[item.rfind("(")+1:item.find(")")], "%Y, %m, %d, %M, %S").date() for item in df.iloc[:,0]]
            if df.index.duplicated().any():
                raise Exception("ticker %s has multiple values on date %s"%(ticker, df.index.get_duplicates()[0].strftime("%Y-%m-%d")))
            priceColumn = "Close" if localDataFile.find("_PRICE")!=-1 else "UnitValue"
            df = df[[priceColumn]]
            df.columns = ["Close"]
            df = df.add_suffix(suffix)
            df = df.sort_index()
        elif self.dataSrc == "yahoo":
            raise Exception("grabLocalData for %s not implemented"%self.dataSrc)
        elif self.dataSrc == "tonglian":
            raise Exception("grabLocalData for %s not implemented"%self.dataSrc)
        else:
            raise Exception("unrecognized data src %s"%self.dataSrc)
        #drop any rows with zero values
        hasZero = (df.T==0.0).any()
        df = df[~hasZero]
        return df

    @staticmethod
    def getDataGrabber(dataSrc):
        dataSrc = dataSrc.lower()
        if dataSrc not in DataGrabber.grabberByDataSrc:
            grabber = DataGrabber(dataSrc)
            DataGrabber.grabberByDataSrc[dataSrc] = grabber
        return DataGrabber.grabberByDataSrc[dataSrc]

    @staticmethod
    def getFundData(ticker, dataSrc, startDate, endDate, suffix=""):
        #pdb.set_trace()
        dataGrabber = DataGrabber.getDataGrabber(dataSrc.lower())
        return dataGrabber.grabLocalData(ticker, startDate, endDate, suffix)
    
    @staticmethod
    def getStrucFundData(fundParam, startDate=DATE_ZERO, endDate=TODAY):
        #pdb.set_trace()
        results = {}
        aTickersToDrop=[]
        for aTicker in fundParam.index:
            try:
                fundInfo = fundParam.loc[aTicker]
                bTicker = fundInfo["bTicker"]
                baseTicker = fundInfo["baseTicker"]
                aSeries = DataGrabber.getFundData(aTicker, fundInfo["aDataSrc"], startDate, endDate, "A")
                bSeries = DataGrabber.getFundData(bTicker, fundInfo["bDataSrc"], startDate, endDate, "B")
                baseSeries = DataGrabber.getFundData(baseTicker, fundInfo["baseDataSrc"], startDate, endDate, "Base")
                result = aSeries.join(bSeries, how="outer")
                result = result.join(baseSeries, how="outer")
                results[aTicker] = result.dropna()
            except Exception as e:
                print "failed to load data for %s with exception %s, drop this fund from regressoin test"%(aTicker, str(e))
                aTickersToDrop.append(aTicker)
        for aTickerToDrop in aTickersToDrop:
            fundParam = fundParam.drop(aTickerToDrop, axis=0)
        print results.keys()
        return results

    
class BackTestParamStructFundArbitrage:
    def __init__(self, population=[], initialAmount=100000.0, minTxAmnt=100):
        self.population = population
        self.initialAmount = initialAmount
        self.minTxAmnt = minTxAmnt
        self.commissionRate = 0.0003
        self.baseFundUnwindFeeRate = 0.005

class ArbitrageTransaction:
    txList = []
    def __init__(self, buyDate, missPriceMode,
                       aTicker, bTicker, baseTicker,
                       cashBeforeBuyTx, comissionRate, baseFundUnwindFeeRate,
                       aPriceBuyDate, bPriceBuyDate, basePriceBuyDate,
                       aAmount, bAmount, baseAmount):
        self.buyDate = buyDate
        self.missPriceMode = missPriceMode
        self.aTicker = aTicker
        self.bTicker = bTicker
        self.baseTicker = baseTicker
        self.aPriceBuyDate = aPriceBuyDate
        self.bPriceBuyDate = bPriceBuyDate
        self.basePriceBuyDate = basePriceBuyDate
        self.aAmount = aAmount
        self.bAmount = bAmount
        self.baseAmount = baseAmount
        self.cashBeforeBuyTx = cashBeforeBuyTx
        self.comissionRate = comissionRate
        self.baseFundUnwindFeeRate = baseFundUnwindFeeRate
        self.sellDate = None
        self.aPriceSellDate = .0
        self.bPriceSellDate = .0
        self.basePriceSellDate = .0
        self.arbPnL = .0
        self.mktMovePnl = .0
        self.ttlPnL = .0
        if missPriceMode =="OverPrice":
            # buy baseFund initially
            self.cashToBuy = self.basePriceBuyDate * self.baseAmount
        elif missPriceMode =="UnderPrice":
            # buy AFund and BFund initially
            cashAFund = self.aPriceBuyDate * self.aAmount
            cashBFund = self.bPriceBuyDate * self.bAmount
            self.cashToBuy = (cashAFund + cashBFund)
        else:
            raise Exception("Unrecognized missPriceMode: %s"%missPriceMode)
        self.buyComission = self.comissionRate * self.cashToBuy
        self.cashAfterBuyTx = self.cashBeforeBuyTx -  self.cashToBuy - self.buyComission

    @staticmethod
    def scaleTx(tx, scaleFactor, cashBeforeBuyTx):
        scaledTx = ArbitrageTransaction( tx.buyDate, tx.missPriceMode,
                                        tx.aTicker, tx.bTicker, tx.baseTicker,
                                        cashBeforeBuyTx, tx.comissionRate, tx.baseFundUnwindFeeRate,
                                        tx.aPriceBuyDate, tx.bPriceBuyDate, tx.basePriceBuyDate,
                                        tx.aAmount*scaleFactor, tx.bAmount*scaleFactor, tx.baseAmount*scaleFactor)
        return scaledTx

    def cashBeforeBuy(self):
        return self.cashBeforeBuyTx

    def cashAfterBuy(self):
        return self.cashAfterBuyTx

    def cashBeforeSell(self):
        return self.cashBeforeSellTx

    def cashAfterSell(self):
        return self.cashAfterSellTx

    def collectTxInfos():
        ''' a decorator to optionally collect all transactions for pnl analysis purpose '''
        def decorator(f):
            @functools.wraps(f)
            def wrapper(self, *args, **kwargs):
                ArbitrageTransaction.txList.append(self)
                f(self, *args, **kwargs)
            return wrapper
        return decorator

    @staticmethod
    def reportTxDetails(SEP=","):
        report=""
        for tx in ArbitrageTransaction.txList:
            report += tx.toString(SEP)
        return report


    @collectTxInfos()
    def closeArbitrage(self, sellDate, aPriceSellDate, bPriceSellDate, basePriceSellDate, cashBeforeSellTx):
        self.cashBeforeSellTx = cashBeforeSellTx
        self.sellDate = sellDate
        self.aPriceSellDate = aPriceSellDate
        self.bPriceSellDate = bPriceSellDate
        self.basePriceSellDate = basePriceSellDate
        if self.missPriceMode =="OverPrice":
            # sell AFund and BFund
            cashAFund = self.aPriceSellDate * self.aAmount
            cashBFund = self.bPriceSellDate * self.bAmount
            self.cashFromSell = cashAFund + cashBFund
            self.sellComission = self.comissionRate * self.cashFromSell
            self.cashAfterSellTx = self.cashBeforeSellTx +  self.cashFromSell - self.sellComission
            self.arbPnL = (self.aPriceBuyDate*self.aAmount + self.bPriceBuyDate*self.bAmount) - (self.basePriceBuyDate*self.baseAmount)
            self.mktMovePnL = self.aAmount*(self.aPriceSellDate-self.aPriceBuyDate) + self.bAmount*(self.bPriceSellDate-self.bPriceBuyDate)
            self.ttlPnL = self.cashFromSell - self.cashToBuy - self.buyComission - self.sellComission
        elif self.missPriceMode =="UnderPrice":
            # sell baseFund
            self.cashFromSell = self.basePriceSellDate * self.baseAmount
            self.sellComission = self.baseFundUnwindFeeRate * self.cashFromSell
            self.cashAfterSellTx = self.cashBeforeSellTx +  self.cashFromSell - self.sellComission
            self.arbPnL = (self.basePriceBuyDate*self.baseAmount) - (self.aPriceBuyDate*self.aAmount + self.bPriceBuyDate*self.bAmount)
            self.mktMovePnL = self.baseAmount*(self.basePriceSellDate-self.basePriceBuyDate)
            self.ttlPnL = self.cashFromSell - self.cashToBuy - self.buyComission - self.sellComission
        else:
            raise Exception("Unrecognized missPriceMode: %s"%missPriceMode)

    def toString(self, SEP):
        commonHeaders=["missPriceMode", "aTicker", "bTicker", "baseTicker", "aAmount", "bAmount", "baseAmount", "arbPnL", "mktMovePnL", "ttlPnL"]
        buyHeaders=["buyDate", "aPriceBuyDate", "bPriceBuyDate", "basePriceBuyDate", "cashBeforeBuyTx", "cashAfterBuyTx", "buyComission"]
        sellHeaders=["sellDate", "aPriceSellDate", "bPriceSellDate", "basePriceSellDate", "cashBeforeSellTx", "cashAfterSellTx", "sellComission"]
        allHeaders = commonHeaders + buyHeaders + sellHeaders
        headerStr=""
        txStr=""
        for field in allHeaders:
            headerStr += field + SEP
            val = getattr(self, field)
            txStr += str(val) + SEP
        return headerStr + "\n" + txStr + "\n"

class PositionRecord:
    def __init__(self, ticker, position, price):
        self.ticker = ticker
        self.position = position
        self.price = price
        self.mtm = self.price*self.position

class StructFundArbitrage:
    def __init__(self, fundParam):
        self.fundParam = fundParam
        self.histData = DataGrabber.getStrucFundData(self.fundParam)
        print self.fundParam.index
        print self.histData.keys()
        tickersToRemove=[]
        for aTicker in self.fundParam.index:
            if aTicker not in self.histData:
                print "failed to load data for %s, drop this fund from regressoin test population"%(aTicker)
                tickersToRemove.append(aTicker)
        for aTicker in tickersToRemove:
            self.fundParam = self.fundParam.drop(aTicker, axis=0)
        self.backTestParam = self.getDefaultParam()
        self.simDatesIndex = None
        print "constructor of StructFundArbitrage:"+str(self.histData.keys())
        for aTicker in self.histData:
            if self.simDatesIndex is None:
                self.simDatesIndex = self.histData[aTicker].index
            else:
                try:
                    self.simDatesIndex = self.simDatesIndex.union(self.histData[aTicker].index)
                except Exception as e:
                    raise e
        self.simDatesIndex = self.simDatesIndex.order()
        self.tradeThreshold_overP = 0.015
        self.tradeThreshold_underP = 0.015
        self.TopN2Arbitrage = min(5, len(self.backTestParam.population))
        # book value calculation always assumes fund conversion is immediate, for example, if baseFund xx is OverPriced (A+B>Base), we buy baseFund on t and immediately convert to A+B
        self.bookValueSeries = []
        self.cashRemainedSeries = []
        self.positionRecordSeries = []
        self.positionValueSeries = []
        self.bookValueDateSeries = []

    def getDefaultParam(self):
        return BackTestParamStructFundArbitrage(population = self.fundParam.index)

    def getSimDates(self):
        datelist = self.simDatesIndex.to_datetime()
        return [d.date() for d in datelist]
    
    def initStrategy(self, initDate):
        self.initDate = initDate
        self.cash = self.backTestParam.initialAmount
        self.sellSched = {}
        self.positions = {}

    def notTradableon(self, aTicker, simDate, isBuy):
        ''' check if aTicker relevant fund is not tradable on simDate, eg, already rise/drop by 10% from T-1, or remaining cash not enough to buy minimum tradable amount '''
        return False

    def getTradeAmount(self, cashRemain, closePrice):
        minCashRequired = closePrice * self.backTestParam.minTxAmnt * (1+self.backTestParam.commissionRate)
        if cashRemain >= minCashRequired:
            try:
                tradeAmount = (int)(cashRemain/self.TopN2Arbitrage/closePrice/self.backTestParam.minTxAmnt)
            except Exception as e:
                print "getTradeAmount failed with exception %s"%(str(e))
                raise e
                #pdb.set_trace()
            if tradeAmount==0:
                tradeAmount = 1
            return tradeAmount*self.backTestParam.minTxAmnt
        else:
            return 0

    def overPriceTrade(self, cashRemain, aTicker, bTicker, baseTicker, simDate, aClose, bClose, baseClose, aWeight, bWeight):
        ''' A+B>base, buy base fund on T, sell A+B on T+2 '''
        combinedAmount = (aWeight+bWeight)
        baseAmount = self.getTradeAmount(cashRemain, baseClose*combinedAmount) * combinedAmount
        aAmount = baseAmount * aWeight / combinedAmount
        bAmount = baseAmount * bWeight / combinedAmount
        if baseAmount!=0:
            #self.todayPosMap[aTicker] = ('OverPrice', (baseTicker,None), (baseAmount,None), (baseClose,None), (simDate, aTicker, bTicker))
            tx = ArbitrageTransaction( simDate, 'OverPrice', aTicker, bTicker, baseTicker,
                                       cashRemain, self.backTestParam.commissionRate, self.backTestParam.baseFundUnwindFeeRate,
                                       aClose, bClose, baseClose, aAmount, bAmount, baseAmount)
            self.todayPosMap[aTicker] = tx
            cashRemain = tx.cashAfterBuy()
        return cashRemain, baseAmount

    def underPriceTrade(self, cashRemain, aTicker, bTicker, baseTicker, simDate, aClose, bClose, baseClose, aWeight, bWeight):
        ''' A+B<base, buy A and B on T, merge into baseFund on T+2, sell baseFund on T+3 '''
        aPlusB = aWeight*aClose + bWeight*bClose
        combinedAmount = (aWeight+bWeight)
        tradeAmount = self.getTradeAmount(cashRemain, aPlusB)
        aAmount = tradeAmount * aWeight
        bAmount = tradeAmount * bWeight
        baseAmount = tradeAmount * combinedAmount
        if tradeAmount!=0:
            #self.todayPosMap[aTicker] = ('UnderPrice', (aTicker,bTicker), (aAmount, bAmount), (aClose,bClose), (simDate, baseTicker, None))
            #cashRemain -= tradeAmount * aPlusB * (1+self.backTestParam.commissionRate)
            tx = ArbitrageTransaction( simDate, 'UnderPrice', aTicker, bTicker, baseTicker,
                                       cashRemain, self.backTestParam.commissionRate, self.backTestParam.baseFundUnwindFeeRate,
                                       aClose, bClose, baseClose, aAmount, bAmount, baseAmount)
            self.todayPosMap[aTicker] = tx
            cashRemain = tx.cashAfterBuy()
        return cashRemain, baseAmount


    def run(self, simIdx, simDate):
        missPriceDict = {}
        if simDate==datetime.date(2016,7,25):
            pass
        self.executeSellArbitrage(simDate)

        for aTicker in self.fundParam.index:
            fundInfo = self.fundParam.loc[aTicker]
            histData = self.histData[aTicker]
            if simDate not in histData.index: # no price on simDate, not tradable
                continue
            bTicker = fundInfo["bTicker"]
            baseTicker = fundInfo["baseTicker"]
            aWeight = fundInfo["aWeight"]
            bWeight = fundInfo["bWeight"]
            aClose = histData.loc[simDate]["CloseA"]
            bClose = histData.loc[simDate]["CloseB"]
            baseClose = histData.loc[simDate]["CloseBase"]
            aPlusB = aWeight*aClose + bWeight*bClose
            baseWeightedClose = baseClose*(aWeight+bWeight)
            #print "aPlusB:%f overPriceCandidate:%f underPricecandidate:%f baseWeightedClose:%f"%(aPlusB, (1+self.tradeThreshold_overP) * baseWeightedClose, aPlusB*(1+self.tradeThreshold_underP), baseWeightedClose)
            # not quite reasonable to use T close to decide whether to buy on T, should use the price of one minute before close on T, but normally it's fine
            try:
                if aPlusB > (1+self.tradeThreshold_overP) * baseWeightedClose:
                    profitRate = (aPlusB - baseWeightedClose)/baseWeightedClose - 2*self.backTestParam.commissionRate
                    missPriceDict[aTicker] = (profitRate, "OverPrice")
                    print "%s is over priced on %s by %f"%(aTicker, str(simDate), profitRate)
                elif aPlusB*(1+self.tradeThreshold_underP)  < baseWeightedClose:
                    profitRate = (baseWeightedClose - aPlusB)/aPlusB - self.backTestParam.commissionRate - self.backTestParam.baseFundUnwindFeeRate
                    missPriceDict[aTicker] = (profitRate, "UnderPrice")
                    print "%s is under priced on %s by %f"%(aTicker, str(simDate), profitRate)
            except Exception as e:
                #pdb.set_trace()
                print str(e)
                raise e
        nbTraded = 0
        self.todayPosMap = {}
        cashRemain = self.cash
        for aTicker in sorted(missPriceDict, key=lambda x: missPriceDict[x][0],reverse=True):# assumes there are more than TopN2Arbitrage imnts to do arbitrage today, pick the first TopN2Arbitrage imnts to allocate cash
            profitRate = missPriceDict[aTicker][0]
            missPriceMode = missPriceDict[aTicker][1]
            histData = self.histData[aTicker]
            aClose = histData.loc[simDate]["CloseA"]
            bClose = histData.loc[simDate]["CloseB"]
            fundInfo = self.fundParam.loc[aTicker]
            bTicker = fundInfo["bTicker"]
            baseTicker = fundInfo["baseTicker"]
            aWeight = fundInfo["aWeight"]
            bWeight = fundInfo["bWeight"]
            aClose = histData.loc[simDate]["CloseA"]    
            bClose = histData.loc[simDate]["CloseB"]
            baseClose = histData.loc[simDate]["CloseBase"]
            aPlusB = aWeight*aClose + bWeight*bClose
            baseWeightedClose = baseClose*(aWeight+bWeight)
            if self.notTradableon(aTicker, simDate, True):
                continue
            if missPriceMode == "OverPrice":
                cashRemain,tradeAmount  = self.overPriceTrade(cashRemain, aTicker, bTicker, baseTicker, simDate, aClose, bClose, baseClose, aWeight, bWeight)
            elif missPriceMode == "UnderPrice":
                cashRemain,tradeAmount = self.underPriceTrade(cashRemain, aTicker, bTicker, baseTicker, simDate, aClose, bClose, baseClose, aWeight, bWeight)
            else:
                raise Exception("%s is missed price on %s by %f, but not found in overPriceMap or underPriceMap"%(aTicker, str(simDate), profitRate))
            if tradeAmount!=0:
                nbTraded += 1
                if nbTraded >= self.TopN2Arbitrage:
                    break
        
        if nbTraded < self.TopN2Arbitrage and nbTraded!=0: # if tradable imnts < TopN2Arbitrage, scale up the cash allocation for these arbitragable imnts
            #print "self.cash:%f cashRemain:%f"%(self.cash, cashRemain)
            scalor = self.cash / (self.cash-cashRemain)
            cashRemain = self.cash
            todayPosMapCpy = self.todayPosMap
            self.todayPosMap = {}
            for aTicker in sorted(todayPosMapCpy, key=lambda x: missPriceDict[x][0],reverse=True):
                tx = todayPosMapCpy[aTicker]
                aWeight = fundInfo["aWeight"]
                bWeight = fundInfo["bWeight"]
                if tx.missPriceMode == 'OverPrice':
                    minAmount = self.backTestParam.minTxAmnt * (aWeight+bWeight)
                    scaledBaseAmount = ((int)(tx.baseAmount * scalor / minAmount)) * minAmount
                    scaleFactor = scaledBaseAmount / tx.baseAmount
                elif tx.missPriceMode == 'UnderPrice':
                    scaled_aAmount = ((int)(tx.aAmount * scalor / self.backTestParam.minTxAmnt)) * self.backTestParam.minTxAmnt
                    scaleFactor = scaled_aAmount/tx.aAmount
                else:
                     raise Exception("unrecognized arbitrage pattern")
                scaledTx = ArbitrageTransaction.scaleTx(tx, scaleFactor, cashRemain)
                if scaledTx.cashAfterBuy() >= 0.0:
                    cashRemain = scaledTx.cashAfterBuy()
                    self.todayPosMap[aTicker] = scaledTx

        for aTicker in self.todayPosMap:
            tx = self.todayPosMap[aTicker]
            tickerTradeDates = self.histData[aTicker].index.to_datetime()
            dateIdx = tickerTradeDates.get_loc(tx.buyDate)
            if tx.missPriceMode == 'OverPrice':
                self.addPosition(tx.baseTicker, tx.baseAmount)
                print "buy %f shares of %s on %s at price %f, cashBefore %f, cashAfter %f"%(tx.baseAmount, tx.baseTicker, str(simDate), tx.basePriceBuyDate, tx.cashBeforeBuy(), tx.cashAfterBuy())
                if dateIdx+2 < len(tickerTradeDates):
                    self.addSellSched(tickerTradeDates[dateIdx+2].date(), tx)
                else:
                    self.addSellSched(tickerTradeDates[-1].date(), tx)
                    print "should sell %s and %s 2 trade-days after %s, but backtest timeline not long enough, scheduled to sell on %s"%(tx.aTicker, tx.bTicker, tx.buyDate.strftime("%Y-%m-%d"), tickerTradeDates[-1].date().strftime("%Y-%m-%d"))
            elif tx.missPriceMode == 'UnderPrice':
                self.addPosition(tx.aTicker, tx.aAmount)
                self.addPosition(tx.bTicker, tx.bAmount)
                print "buy %f shares of %s on %s at price %f, cashBefore %f, cashAfter %f"%(tx.aAmount, tx.aTicker, str(simDate), tx.aPriceBuyDate, tx.cashBeforeBuy(), tx.cashAfterBuy())
                print "buy %f shares of %s on %s at price %f, cashBefore %f, cashAfter %f"%(tx.bAmount, tx.bTicker, str(simDate), tx.bPriceBuyDate, tx.cashBeforeBuy(), tx.cashAfterBuy())
                if dateIdx+2 < len(tickerTradeDates):
                    self.addSellSched(tickerTradeDates[dateIdx+2].date(), tx)
                else:
                    self.addSellSched(tickerTradeDates[-1].date(), tx)
                    print "should sell %s 2 trade-days after %s, but backtest timeline not long enough, scheduled to sell on %s"%(tx.baseTicker, tx.buyDate.strftime("%Y-%m-%d"), tickerTradeDates[-1].date().strftime("%Y-%m-%d"))
            else:
                raise Exception("unrecognized arbitrage pattern")
        self.cash = cashRemain
        self.recordBookValue(simDate)

    def addPosition(self, ticker, amount):
        if ticker in self.positions:
            self.positions[ticker] += amount
        else:
            self.positions[ticker] = amount

    def removePosition(self, ticker, amount):
        TINY_POS_TO_IGNORE = 1e-5
        if ticker not in self.positions or (self.positions[ticker]-amount)<-TINY_POS_TO_IGNORE:
            raise Exception("trying to remove position for %s by amount %f, but only hold %f in prflio"%(ticker, amount, self.positions[ticker] if ticker in self.positions else 0.0))
        else:
            self.positions[ticker] -= amount
            if abs(self.positions[ticker] < TINY_POS_TO_IGNORE):
                self.positions.pop(ticker)

    def calcPositionValue(self, simDate):
        ttlPositionValue = 0.0
        positionRecords = []
        for sellDate in self.sellSched:
            txList = self.sellSched[sellDate]
            for tx in txList:
                aTicker = tx.aTicker
                bTicker = tx.bTicker
                baseTicker = tx.baseTicker
                histData = self.histData[aTicker]
                dateIdx = bisect.bisect_left(histData.index, simDate)
                if dateIdx == len(histData.index):
                    dateIdx -= 1
                aPrice = histData.iloc[dateIdx]['CloseA']
                bPrice = histData.iloc[dateIdx]['CloseB']
                basePrice = histData.iloc[dateIdx]['CloseBase']
                if tx.missPriceMode=="OverPrice":
                    positionValue = aPrice*tx.aAmount + bPrice*tx.bAmount
                    positionRecords.append(PositionRecord(aTicker,tx.aAmount, aPrice))
                    positionRecords.append(PositionRecord(bTicker, tx.bAmount, bPrice))
                elif tx.missPriceMode=="UnderPrice":
                    positionValue = basePrice*tx.baseAmount
                    positionRecords.append(PositionRecord(baseTicker, tx.baseAmount, basePrice))
                else:
                    raise Exception("Unrecognized missPriceMode: %s" % tx.missPriceMode)
                ttlPositionValue += positionValue
        return ttlPositionValue, positionRecords

    def recordBookValue(self, simDate):
        positionValue, positionRecords = self.calcPositionValue(simDate)
        self.cashRemainedSeries.append(self.cash)
        self.positionValueSeries.append(positionValue)
        self.positionRecordSeries.append(positionRecords)
        self.bookValueSeries.append(self.cash+positionValue)
        self.bookValueDateSeries.append(simDate)

    def getBookValueSummary(self, SEP=","):
        dailyReturn = pd.DataFrame(data={"Return":self.bookValueSeries}, index=self.bookValueDateSeries)
        dailyReturn = dailyReturn.diff() / dailyReturn
        dailyReturn = dailyReturn.dropna()
        summaryStr = "date" + SEP + "cash" + SEP + "postionVal" + SEP + "bookValue" + SEP + "return\n"
        positionStr = "date" + SEP + "ticker" + SEP + "position" + SEP + "price" + SEP + "mtm\n"
        for idx in xrange(len(self.bookValueDateSeries)):
            valDate = self.bookValueDateSeries[idx]
            bookValue = self.bookValueSeries[idx]
            cash = self.cashRemainedSeries[idx]
            positionValue = self.positionValueSeries[idx]
            try:
                summaryStr += valDate.strftime("%Y-%m-%d") + SEP \
                              + str(cash) + SEP \
                              + str(positionValue) + SEP \
                              + str(bookValue) + SEP \
                              + (str(dailyReturn.loc[valDate].Return) if valDate in dailyReturn.index else "nan") \
                              + "\n"
            except Exception as e:
                raise e
                #pdb.set_trace()
            postionRecords = self.positionRecordSeries[idx]
            for posRecord in postionRecords:
                positionStr += valDate.strftime("%Y-%m-%d") + SEP \
                              + posRecord.ticker + SEP \
                              + str(posRecord.position) + SEP \
                              + str(posRecord.price) + SEP \
                              + str(posRecord.mtm) + "\n"
        return summaryStr + "\n" + positionStr

    def addSellSched(self, sellDate, tx):
        if sellDate not in self.sellSched:
            self.sellSched[sellDate] = []
        self.sellSched[sellDate].append(tx)

    def executeSellOrder(self, sellDate, tx):
        if tx.missPriceMode == 'OverPrice':
            aTicker = tx.aTicker
            bTicker = tx.bTicker
            print "trying to execute OverPrice Sell Order for %s and %s"%(aTicker, bTicker)
            if self.notTradableon(aTicker, sellDate, False) or self.notTradableon(bTicker, sellDate, False):
                return False
            histData = self.histData[aTicker]
            fundData = self.fundParam.loc[aTicker] 
            if sellDate in histData.index:
                print "found price data on %s for %s and %s"%(str(sellDate), aTicker, bTicker)
                aPrice = histData.loc[sellDate]['CloseA']
                bPrice = histData.loc[sellDate]['CloseB']
                basePrice = histData.loc[sellDate]['CloseBase']
                tx.closeArbitrage(sellDate, aPrice, bPrice, basePrice, self.cash)
                self.cash = tx.cashAfterSell()
                print "sell %f shares of %s on %s at price %f, cashBefore %f, cashAfter %f"%(tx.aAmount, aTicker, str(sellDate), aPrice, tx.cashBeforeSell(), tx.cashAfterSell())
                print "sell %f shares of %s on %s at price %f, cashBefore %f, cashAfter %f"%(tx.bAmount, bTicker, str(sellDate), bPrice, tx.cashBeforeSell(), tx.cashAfterSell())
                return True
            else:
                return False
        elif tx.missPriceMode == 'UnderPrice':
            aTicker = tx.aTicker
            baseTicker = tx.baseTicker
            print "trying to execute UnderPrice sell Order for %s"%(baseTicker)
            if self.notTradableon(baseTicker, sellDate, False):
                return False
            histData = self.histData[aTicker]
            if sellDate in histData.index:
                print "found price data on %s for %s"%(str(sellDate), baseTicker)
                aPrice = histData.loc[sellDate]['CloseA']
                bPrice = histData.loc[sellDate]['CloseB']
                basePrice = histData.loc[sellDate]['CloseBase']
                tx.closeArbitrage(sellDate, aPrice, bPrice, basePrice, self.cash)
                self.cash = tx.cashAfterSell()
                print "sell %f shares of %s on %s at price %f, cashBefore %f, cashAfter %f"%(tx.baseAmount, baseTicker, str(sellDate), basePrice, tx.cashBeforeSell(), tx.cashAfterSell())
                return True
            else:
                return False
        
        

    def executeSellArbitrage(self, simDate):
        sellRecordToRemove = {}
        print "checking if anything to sell on %s"%(str(simDate))
        for sellDate in self.sellSched:
            if sellDate <= simDate: # should have been sold by sellDate, but may have not been able to do so due to short of liquidity
                print "something to sell originall on %s"%(sellDate)
                txList = self.sellSched[sellDate]
                txsToRemove = []
                for tx in txList:
                    if tx.missPriceMode == 'OverPrice':
                        print "OverPrice"
                        if self.executeSellOrder(simDate, tx):
                            self.removePosition(tx.baseTicker, tx.baseAmount)
                            txsToRemove.append(tx)
                    elif tx.missPriceMode == 'UnderPrice':
                        print "UnderPrice"
                        if self.executeSellOrder(simDate, tx):
                            self.removePosition(tx.aTicker, tx.aAmount)
                            self.removePosition(tx.bTicker, tx.bAmount)
                            txsToRemove.append(tx)
                    else:
                        raise Exception("unrecognized arbitrage pattern")
                for tx in txsToRemove:
                    txList.remove(tx)
                self.sellSched[sellDate] = txList
                if len(txList) == 0:
                    if sellDate not in sellRecordToRemove:
                        sellRecordToRemove[sellDate] = 1
        for dateToRmv in sellRecordToRemove:
            try:
                self.sellSched.pop(dateToRmv)
            except Exception as e:
                print str(e)
                raise e


def backTest(strategy, sttDate=DATE_ZERO, endDate=TODAY):
    simDates = strategy.getSimDates()
    sttIdx = bisect.bisect_left(simDates, sttDate)
    endIdx =  bisect.bisect_right(simDates, endDate)
    strategy.initStrategy(sttDate)
    for simIdx in xrange(sttIdx, endIdx):
        simDate = simDates[simIdx]
        strategy.run(simIdx, simDate)

#pdb.set_trace()
strategy = StructFundArbitrage(fundParam)
backTest(strategy, datetime.date(2016,1,1))
print ArbitrageTransaction.reportTxDetails(",")
print strategy.getBookValueSummary(SEP=',')