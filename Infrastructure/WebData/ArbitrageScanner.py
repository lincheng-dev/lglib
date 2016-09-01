import WebData
import datetime
import pandas as pd
import threading
import time
import numpy as np
import sys
import getopt
import StructuredFund.StrucFundHelper as StrucFundHelper

class ArbitrageScanner:
    def __init__(self):
        def filterWeight(x):
            wStr = str(x['abWeight'])
            idx = wStr.index(":")
            aWeight = float(wStr[:idx])
            bWeight = float(wStr[idx + 1:])
            return pd.Series(data=[aWeight, bWeight])
        self.fundParam = pd.read_csv(r"fundInfo.csv", encoding='gbk')
        self.fundParam = self.fundParam[['aTicker','bTicker','baseTicker','abWeight']]
        weightDf = self.fundParam.apply(filterWeight,axis=1)
        self.fundParam = self.fundParam.astype(str)
        self.fundParam = self.fundParam.assign(aWeight=weightDf[0])
        self.fundParam = self.fundParam.assign(bWeight=weightDf[1])
        self.fundParam = self.fundParam.set_index('aTicker')
        self.fundParam = self.fundParam.drop('abWeight',axis=1)

    def isZero(self, value):
        return np.isclose(value, 0.0, equal_nan=True)
    
    def scan_detail(self, threshold=0.00, dumpFilePrefix="J:/arb_list"):
        print "Doing detailed scan for arbitrage"
        StrucFundHelper.StrucFundHelperDict.reload()
        StrucFundHelper.TxFeeHelper.reload()
        now = datetime.datetime.now()
        arbitrageDFs = []
        for aTicker in self.fundParam.index:
            bTicker = self.fundParam.loc[aTicker, "bTicker"]
            baseTicker = self.fundParam.loc[aTicker, "baseTicker"]
            #print "processing %s %s %s" % (baseTicker, aTicker, bTicker)
            aWeight = self.fundParam.loc[aTicker, "aWeight"]
            bWeight = self.fundParam.loc[aTicker, "bWeight"]
            tWeight = aWeight + bWeight
            aWeight = aWeight / tWeight
            bWeight = bWeight / tWeight
            infoDict = {'Ticker': baseTicker, 'ATicker': aTicker, 'BTicker': bTicker, 'AWeight': aWeight, 'BWeight': bWeight, 'UpFold': 0., 'DownFold': 0.}
            try:
                aQuote = WebData.get_fund_quote_with_value(aTicker)
                bQuote = WebData.get_fund_quote_with_value(bTicker)
                baseQuote = WebData.get_fund_quote_with_value(baseTicker)
                pxinfo = {baseTicker: baseQuote, aTicker: aQuote, bTicker: bQuote}
            except Exception as e:
                print "fail to load quote for (%s,%s,%s) with error %s, skip"%(aTicker,bTicker,baseTicker, str(e))
                continue
            fundHelper = StrucFundHelper.StrucFundHelperDict.getHelperForTicker(**infoDict)
            mergeArb   = fundHelper.getArbMargin('MERGE', pxinfo, threshold = threshold)
            splitArb   = fundHelper.getArbMargin('SPLIT', pxinfo, threshold = threshold)
            if mergeArb:
                arbitrageDFs.append(pd.DataFrame(mergeArb, index=[now]))
            if splitArb:
                arbitrageDFs.append(pd.DataFrame(splitArb, index=[now]))
        if len(arbitrageDFs) > 0:
            arbitrageTable = pd.concat(arbitrageDFs)
            arbitrageTable = arbitrageTable.sort(columns=["PriceMargin"],ascending=False)
            summary = "\n" + arbitrageTable.to_string()
            print summary
            file = open(name=dumpFilePrefix+"_all.txt",mode="a+")
            file.write(summary)
            file.close()
            file = open(name=dumpFilePrefix+"_latest.txt",mode="w")
            file.write(arbitrageTable.to_string())
            file.close()
        
    def scan(self, mode="default", threshold=0.03, exchangeCommission=0.002, unwindRate=0.005, baseSubscription=0.015, dumpFile="H:\\HashiCorp\\Vagrant\\bin\\arbitrage_summary.txt"):
        if mode=="detail":
            self.scan_detail(threshold=threshold)
            return
        now = datetime.datetime.now()
        arbitrageArray = []
        arbitrageIndices = []
        for aTicker in self.fundParam.index:
            bTicker = self.fundParam.loc[aTicker, "bTicker"]
            baseTicker = self.fundParam.loc[aTicker, "baseTicker"]
            aWeight = self.fundParam.loc[aTicker, "aWeight"]
            bWeight = self.fundParam.loc[aTicker, "bWeight"]
            try:
                aQuote = WebData.get_fund_quote(aTicker).loc[0,"price"]
                bQuote = WebData.get_fund_quote(bTicker).loc[0,"price"]
                baseQuote = WebData.get_fund_quote(baseTicker).loc[0,"price"]
                if self.isZero(aQuote) or self.isZero(bQuote) or self.isZero(baseQuote):
                    continue
            except Exception as e:
                print "fail to load quote for (%s,%s,%s) with error %s, skip"%(aTicker,bTicker,baseTicker, str(e))
                continue
            arbitrageRate = 0.0
            if aWeight*aQuote+bWeight*bQuote > (aWeight+bWeight)*baseQuote*(1+threshold):
                arbitrageRate = (aWeight*aQuote+bWeight*bQuote) / ((aWeight+bWeight)*baseQuote) - baseSubscription - exchangeCommission - 1.0
                arbitrageArray.append([bTicker, baseTicker, aQuote, bQuote, baseQuote, aWeight, bWeight, arbitrageRate])
                arbitrageIndices.append((now, aTicker))
            elif (aWeight*aQuote+bWeight*bQuote)*(1+threshold) < (aWeight+bWeight)*baseQuote:
                arbitrageRate = ((aWeight + bWeight) * baseQuote) / (aWeight * aQuote + bWeight * bQuote) - exchangeCommission - unwindRate - 1.0
                arbitrageArray.append([bTicker, baseTicker, aQuote, bQuote, baseQuote, aWeight, bWeight, arbitrageRate])
                arbitrageIndices.append((now, aTicker))
        arbitrageTable = pd.DataFrame(data=arbitrageArray, index=arbitrageIndices, columns=["bTicker","baseTicker","aPrice","bPrice","basePrice","aWeight","bWeight","arbitrageRate"])
        arbitrageTable = arbitrageTable.sort(columns=["arbitrageRate"],ascending=False)
        summary = "\n" + arbitrageTable.to_string()
        print summary
        file = open(name=dumpFile,mode="a+")
        file.write(summary)
        file.close()

def singleScan(mode="default"):
    scanner = ArbitrageScanner()
    scanner.scan(mode)

def repeatedScan(scan_interval=900, mode="default"):
    while True:
        scanner = ArbitrageScanner()
        scanner.scan(mode)
        time.sleep(scan_interval)

def main():
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)
    # process arguments
    if args[0]=="repeat":
        if len(args)>=2:
            interval = float(args[1])
            repeatedScan(interval)
        else:
            repeatedScan()
    elif args[0]=="single":
        singleScan()
    else:
        print "unrecognized mode %s"%(args[0])

if __name__ == "__main__":
    main()