import WebData
import datetime
import pandas as pd
import threading
import time
import numpy as np

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

    def scan(self, threshold=0.03, exchangeCommission=0.002, unwindRate=0.005, baseSubscription=0.015, dumpFile="H:\\HashiCorp\\Vagrant\\bin\\arbitrage_summary.txt"):
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
        summary = arbitrageTable.to_string()
        print summary
        file = open(name=dumpFile,mode="a+")
        file.write(summary)
        file.close()

scanner = ArbitrageScanner()
scanner.scan()
'''
SCAN_INTERVAL=900
while True:
    scanner = ArbitrageScanner()
    timer=threading.Timer(SCAN_INTERVAL, scanner.scan)
    timer.start()
    time.sleep(SCAN_INTERVAL)
'''