import datetime
import pandas as pd
import numpy as np
import os, sys
import getopt
import logging
import LGLib
import LGLib.Infrastructure.WebData.WebData as WebData
from LGLib.Infrastructure.BobJob.JobScheduler import JobScheduler
from LGLib.Configure.BobJob.StructuredFund.StructuredFundConfig import STRUCFUND_JOB_CONFIG
import StrucFundHelper

class ArbitrageScanner:
        
    def __init__(self, fundInfoFile=None, dumpPath=None):
        def filterWeight(x):
            wStr = str(x['abWeight'])
            idx = wStr.index(":")
            aWeight = float(wStr[:idx])
            bWeight = float(wStr[idx + 1:])
            return pd.Series(data=[aWeight, bWeight])
        self.fundParam = pd.read_csv(fundInfoFile, encoding='gbk')
        self.fundParam = self.fundParam[['aTicker','bTicker','baseTicker','abWeight']]
        weightDf = self.fundParam.apply(filterWeight,axis=1)
        self.fundParam = self.fundParam.astype(str)
        self.fundParam = self.fundParam.assign(aWeight=weightDf[0])
        self.fundParam = self.fundParam.assign(bWeight=weightDf[1])
        self.fundParam = self.fundParam.set_index('aTicker')
        self.fundParam = self.fundParam.drop('abWeight',axis=1)
        
        # log file
        self.dumpPath = dumpPath
        now           = datetime.datetime.now()
        logFile       = os.path.join(self.dumpPath, "%s_scan.log" % now.strftime("%Y%m%d"))
        logging.basicConfig(filename=logFile,level=logging.DEBUG)        
    
    def __enter__(self):
        self.log   = logging.getLogger("ArbitrageScanner")
        loghandler = logging.FileHandler("")
        loghandler.setLevel(logging.DEBUG)
        self.log.addHandler(loghandler)
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        handlers = self.log.handlers[:]
        for handler in handlers:
            handler.close()
            self.log.removeHandler(handler)

    def isZero(self, value):
        return np.isclose(value, 0.0, equal_nan=True)
    
    def scan_detail(self, *args, **kwargs):
        threshold = kwargs['threshold']
        now = datetime.datetime.now()
        print "\nDoing detailed scan for arbitrage"
        logging.info("Doing detailed scan for arbitrage at %s" % str(now))
        arbitrageDFs = []
        for aTicker in self.fundParam.index:
            bTicker = self.fundParam.loc[aTicker, "bTicker"]
            baseTicker = self.fundParam.loc[aTicker, "baseTicker"]
            aWeight = self.fundParam.loc[aTicker, "aWeight"]
            bWeight = self.fundParam.loc[aTicker, "bWeight"]
            tWeight = aWeight + bWeight
            aWeight = aWeight / tWeight
            bWeight = bWeight / tWeight
            infoDict = {'Ticker': baseTicker, 'ATicker': aTicker, 'BTicker': bTicker, 'AWeight': aWeight, 'BWeight': bWeight, 'UpFold': 0., 'DownFold': 0.}
            try:
                aQuote = WebData.get_fund_quote_with_value(aTicker, useLogging=True)
                bQuote = WebData.get_fund_quote_with_value(bTicker, useLogging=True)
                baseQuote = WebData.get_fund_quote_with_value(baseTicker, useLogging=True)
                pxinfo = {baseTicker: baseQuote, aTicker: aQuote, bTicker: bQuote}
            except Exception as e:
                logging.warning("fail to load quote for (%s,%s,%s) with error %s, skip"%(aTicker,bTicker,baseTicker, str(e)))
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
            arbitrageTable = arbitrageTable.sort_values(by=["PriceMargin"],ascending=False)
            summary = "\n" + arbitrageTable.to_string()
            file = open(name=os.path.join(self.dumpPath, "%s_detailscan_threshold_%s.txt" % (now.strftime("%Y%m%d"), str(threshold).replace('.',''))), mode="a+")
            file.write(summary)
            file.close()
            file = open(name=os.path.join(self.dumpPath, "detailscan_threshold_%s_latest.txt" % str(threshold).replace('.','')), mode="w")
            file.write(arbitrageTable.to_string())
            file.close()
        
    def scan(self, *args, **kwargs):
        mode               = kwargs['mode']
        threshold          = kwargs['threshold']
        exchangeCommission = kwargs.get('exchangeCommission', 0.002)
        unwindRate         = kwargs.get('unwindRate', 0.005)
        baseSubscription   = kwargs.get('baseSubscription', 0.015)
        dumpFile           = os.path.join(self.dumpPath, "arbitrage_summary.txt")
        if mode.lower()=="detail":
            self.scan_detail(threshold=threshold)
            return
        now = datetime.datetime.now()
        print "\nDoing simple scan for arbitrage"
        logging.info("Doing simple scan for arbitrage at %s" % str(now))
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
                logging.warning("fail to load quote for (%s,%s,%s) with error %s, skip"%(aTicker,bTicker,baseTicker, str(e)))
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
        file = open(name=dumpFile,mode="a+")
        file.write(summary)
        file.close()

def singleScan(mode="default", threshold=0.03, dumpPath="H:\\HashiCorp\\Vagrant\\bin\\", fundInfoFile=r"fundInfo.csv"):
    scanner = ArbitrageScanner(fundInfoFile, dumpPath)
    scanner.scan(mode=mode, threshold=threshold)

class RepeatedScan(JobScheduler):
    def __init__(self, use_config_file=False, scan_interval=100, scan_count=2, mode="default", threshold=0.03, dumpPath="H:\\HashiCorp\\Vagrant\\bin\\", fundInfoFile=r"fundInfo.csv"):
        if use_config_file:
            self.scanner = ArbitrageScanner(STRUCFUND_JOB_CONFIG['fundInfoFile'], STRUCFUND_JOB_CONFIG['dumpPath'])
            scheDict = {"SCHEMODE": STRUCFUND_JOB_CONFIG["SCHEMODE"], "SCHEPARAMS": STRUCFUND_JOB_CONFIG, "JOBFUNC": self.scanner.scan, "JOBPARAMS": STRUCFUND_JOB_CONFIG}
        else:
            self.scanner = ArbitrageScanner(fundInfoFile, dumpPath)
            scheDict = {"SCHEMODE": "SIMPLEWAIT", "SCHEPARAMS": {"WAIT": scan_interval, "REPEAT": scan_count}, "JOBFUNC": self.scanner.scan, "JOBPARAMS": {"mode": mode, "threshold": threshold}}
        super(RepeatedScan, self).__init__(**scheDict)

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
        scanSchedule = RepeatedScan(use_config_file=True)
        scanSchedule.run()
    elif args[0]=="single":
        singleScan()
    else:
        print "unrecognized mode %s"%(args[0])

if __name__ == "__main__":
    main()