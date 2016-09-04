#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
from numpy import floor, abs
import pandas
import logging

def createHTML(df=pandas.DataFrame()):
    strHTML = ''
    for currow in df.iterrows():
        lol = [['Type', 'Margin of Price', 'Margin of Value', 'Volumn of Price', 'Volumn of Value', 'MoP Max', 'MoV Max', 'VoP Max', 'VoV Max'],
               [currow[1]['Type'], currow[1]['PriceMargin'], currow[1]['ValueMargin'], currow[1]['PriceAmt'], currow[1]['ValueAmt'], currow[1]['MaxPxMargin'], currow[1]['MaxVaMargin'], currow[1]['MaxPxAmt'], currow[1]['MaxVaAmt']],
               ['Base Ticker', 'Base Price', 'Base Value', 'Base Price MaxP', 'Base Price MaxV', 'Base Fee', 'Base Fee OTC', '', ''],
               [currow[1]['Ticker'], currow[1]['BasePrice'], currow[1]['BaseValue'], currow[1]['MaxBasePx'], currow[1]['MaxBaseVaPx'], currow[1]['BaseFee'], currow[1]['BaseFeeOTC'], '', ''],
               ['A Ticker', 'A Price', 'A Value', 'A Price MaxP', 'A Price MaxV', 'A Fee', 'A Weight', '', ''],
               [currow[1]['ATicker'], currow[1]['APrice'], currow[1]['AValue'], currow[1]['MaxAPrice'], currow[1]['MaxAVaPx'], currow[1]['AFee'], currow[1]['AWeight'], '', ''],
               ['B Ticker', 'B Price', 'B Value', 'B Price MaxP', 'B Price MaxV', 'B Fee', 'B Weight', '', ''],
               [currow[1]['BTicker'], currow[1]['BPrice'], currow[1]['BValue'], currow[1]['MaxBPrice'], currow[1]['MaxBVaPx'], currow[1]['BFee'], currow[1]['BWeight'], '', '']]
        for i in xrange(len(lol)):
            for j in xrange(len(lol[i])):
                if type(lol[i][j]) is float:
                    if abs(lol[i][j]) < 0.1:
                        lol[i][j] = "%.3f%%" % (lol[i][j] * 100.)
                    elif abs(lol[i][j]) < 10.:
                        lol[i][j] = "%8.4f" % lol[i][j]
                    else:
                        lol[i][j] = "%d" % lol[i][j]
                else:
                    lol[i][j] = str(lol[i][j])
        strHTML += '<table border=\"1\">'
        for sublist in lol:
            strHTML += '  <tr><td>'
            strHTML += '    </td><td>'.join(sublist)
            strHTML += '  </td><tr>'
        strHTML += '</table><p></p>'
    return strHTML
            
def NotExchFund(ticker):
    return ticker.startswith('16')
    
def getQuoteAmount(side, num, pxinfo):
    totalAmt = 0
    for i in xrange(1, num+1):
        name   = side + str(i) + '_v'
        curAmt = pxinfo.get(name, [0])[0]
        if not pandas.isnull(curAmt):
            totalAmt += curAmt * 100
    return totalAmt
    
def getQuoteAvg(side, num, pxinfo, amount):
    totalAmt = getQuoteAmount(side, num, pxinfo)
    if  totalAmt < amount:
        raise ValueError("only %d quote, not enought for %d" % (totalAmt, amount))
    accuAmt = 0.
    accuPx  = 0.
    for i in xrange(1, num+1):
        name1  = side + str(i) + '_v'
        name2  = side + str(i) + '_p'
        curAmt = pxinfo.get(name1, [0.])[0]*100
        curPx  = pxinfo.get(name2, [-999.])[0]
        if not pandas.isnull(curAmt) and not pandas.isnull(curPx):
            if curAmt + accuAmt <= amount:
                accuPx  += curAmt * curPx
                accuAmt += curAmt
            else:
                accuPx  += (amount - accuAmt) * curPx
                accuAmt  = amount
                break
    return accuPx / accuAmt

BUYFEEEXCEPTOTC  = {}
SELLFEEEXCEPTOTC = {}
BUYFEEEXCEPT     = {}
SELLFEEEXCEPT    = {}
ExchTxFee        = 0.0019
OTCFundBuy       = 0.015
OTCFundSell      = 0.005

class TxFeeHelper(object):
    
    feeList = {}
    
    @classmethod
    def getFee(cls, type, *args, **kwargs):
        ticker = kwargs.get('Ticker', None)
        assert ticker != None, "Invalid ticker info in HelperDict.getHelperForTicker."
        if (type, ticker) in cls.feeList:
            return cls.feeList[(type, ticker)]
        else:
            fee = cls.calcFee(type, *args, **kwargs)
            cls.feeList[(type, ticker)] = fee 
            return fee
    
    @classmethod
    def calcFee(cls, type, *args, **kwargs):
        Ticker  = kwargs['Ticker']
        ATicker = kwargs['ATicker']
        BTicker = kwargs['BTicker']
        return getattr(cls, 'calcFee'+type.upper())(Ticker, ATicker, BTicker)
        
    @classmethod
    def calcFeeSPLIT(cls, Ticker, ATicker, BTicker):
        buyFee    = BUYFEEEXCEPT.get(Ticker, ExchTxFee)
        buyFeeOTC = BUYFEEEXCEPTOTC.get(Ticker, OTCFundBuy)
        sellFeeA  = SELLFEEEXCEPT.get(ATicker, ExchTxFee)
        sellFeeB  = SELLFEEEXCEPT.get(BTicker, ExchTxFee)
        if NotExchFund(Ticker):
            buyFee = buyFeeOTC
        return (buyFee, buyFeeOTC, sellFeeA, sellFeeB)

    @classmethod
    def calcFeeMERGE(cls, Ticker, ATicker, BTicker):
        buyFeeA    = BUYFEEEXCEPT.get(ATicker, ExchTxFee)
        buyFeeB    = BUYFEEEXCEPT.get(BTicker, ExchTxFee)
        sellFee    = SELLFEEEXCEPT.get(Ticker, ExchTxFee)
        sellFeeOTC = SELLFEEEXCEPTOTC.get(Ticker, OTCFundSell)
        if NotExchFund(Ticker):
            sellFee = sellFeeOTC
        return (sellFee, sellFeeOTC, buyFeeA, buyFeeB)
            
    @classmethod
    def reload(cls):
        cls.feeList = {}
            
class StrucFundHelperDict(object):
    
    helperList = {}
    
    @classmethod
    def getHelperForTicker(cls, *args, **kwargs):
        ticker = kwargs.get('Ticker', None)
        assert ticker != None, "Invalid ticker info in HelperDict.getHelperForTicker."
        if ticker in cls.helperList:
            return cls.helperList[ticker]
        else:
            cls.helperList[ticker] = StrucFundHelper(*args, **kwargs)
            return cls.helperList[ticker]
            
    @classmethod
    def reload(cls):
        cls.helperList = {}

class StrucFundHelper(object):
    
    def __init__(self, *args, **kwargs):
        self.info             = {}
        self.info['Ticker']   = kwargs['Ticker']
        self.info['ATicker']  = kwargs['ATicker']
        self.info['BTicker']  = kwargs['BTicker']
        self.info['AWeight']  = kwargs['AWeight']
        self.info['BWeight']  = kwargs['BWeight']
        self.info['UpFold']   = kwargs['UpFold']
        self.info['DownFold'] = kwargs['DownFold']
    
    def validPxInfo(self, postype, pxinfo):
        baseInfo = pxinfo[self.info['Ticker']]
        AInfo    = pxinfo[self.info['ATicker']]
        BInfo    = pxinfo[self.info['BTicker']]
        today    = datetime.date.today()
        if baseInfo is None:
            logging.warning("get %s price and value failed" % self.info['Ticker'])
        if AInfo is None:
            logging.warning("get %s price and value failed" % self.info['ATicker'])
        if BInfo is None:
            logging.warning("get %s price and value failed" % self.info['BTicker'])
        if postype == 'MERGE' and (baseInfo['b1_p'][0] < 0.01 or AInfo['a1_p'][0] < 0.01 or BInfo['a1_p'][0] < 0.01):
            logging.warning("merge price invalid %s, %s, %s" % (self.info['Ticker'], self.info['ATicker'], self.info['BTicker']))
            return False
        if postype == 'SPLIT' and (baseInfo['a1_p'][0] < 0.01 or AInfo['b1_p'][0] < 0.01 or BInfo['b1_p'][0] < 0.01):
            logging.warning("split price invalid %s, %s, %s" % (self.info['Ticker'], self.info['ATicker'], self.info['BTicker']))
            return False
        if baseInfo['date'][0].date() < today or baseInfo['value_date'][0].date() < today:
            logging.warning("base price date invalid %s" % (self.info['Ticker']))
            return False
        if AInfo['date'][0].date() < today or AInfo['value_date'][0].date() < today:
            logging.warning("A price date invalid %s" % (self.info['ATicker']))
            return False
        if BInfo['date'][0].date() < today or BInfo['value_date'][0].date() < today:
            logging.warning("B price date invalid %s" % (self.info['BTicker']))
            return False
        return True
        
    def getArbMargin(self, postype, pxinfo, threshold=0.0):
        if not self.validPxInfo(postype.upper(), pxinfo):
            return None
        if postype.upper() == 'SPLIT':
            return self.getArbMarginSplit(pxinfo, threshold=threshold)
        elif postype.upper() == 'MERGE':
            return self.getArbMarginMerge(pxinfo, threshold=threshold)
        else:
            raise NotImplementedError
    
    def getArbMarginSplit(self, pxinfo, threshold=0.0):
        baseBuyFee, baseBuyFeeOTC, ASellFee, BSellFee = TxFeeHelper.getFee(type='SPLIT', **self.info)
        baseInfo = pxinfo[self.info['Ticker']]
        AInfo    = pxinfo[self.info['ATicker']]
        BInfo    = pxinfo[self.info['BTicker']]
        
        # look at bid price for sell, ask price for buy
        APrice    = AInfo['b1_p'][0]
        BPrice    = BInfo['b1_p'][0]
        basePrice = baseInfo['a1_p'][0]
        AValue    = AInfo['value'][0]
        BValue    = BInfo['value'][0]
        baseValue = baseInfo['value'][0]
        
        # calculate margin
        buyPrice  = (1.0 + baseBuyFee) * basePrice
        buyValue  = (1.0 + baseBuyFeeOTC) * baseValue
        sellPrice = (1.0 - ASellFee) * APrice * self.info['AWeight'] + (1.0 - BSellFee) * BPrice * self.info['BWeight']
        pxMargin  = sellPrice / buyPrice - 1.0
        valMargin = sellPrice / buyValue - 1.0
        
        # check margin
        if pxMargin < threshold and valMargin < threshold:
            # not any chance
            return None
        else:
            # if chance, check more details on amount and bid-ask spread
            outdict    = {}
            outdict.update(self.info)
            AAmount    = getQuoteAmount('b', 1, AInfo) / self.info['AWeight']
            BAmount    = getQuoteAmount('b', 1, BInfo) / self.info['BWeight']
            baseAmt    = getQuoteAmount('a', 1, baseInfo)
            valAmount  = min([AAmount, BAmount])
            pxAmount   = min([valAmount, baseAmt])
            AAmountL5  = getQuoteAmount('b', 5, AInfo) / self.info['AWeight']
            BAmountL5  = getQuoteAmount('b', 5, BInfo) / self.info['BWeight']
            baseAmtL5  = getQuoteAmount('a', 5, baseInfo)
            valAmtL5   = min([50000, AAmountL5, BAmountL5])
            pxAmtL5    = min([valAmtL5, baseAmtL5])
            APxPxL5    = getQuoteAvg('b', 5, AInfo, pxAmtL5 * self.info['AWeight'])
            BPxPxL5    = getQuoteAvg('b', 5, BInfo, pxAmtL5 * self.info['BWeight'])
            basePxPxL5 = getQuoteAvg('a', 5, baseInfo, pxAmtL5)
            APxVaL5    = getQuoteAvg('b', 5, AInfo, valAmtL5 * self.info['AWeight'])
            BPxVaL5    = getQuoteAvg('b', 5, BInfo, valAmtL5 * self.info['BWeight'])
            basePxVaL5 = baseValue
            maxPxMargin = ((1.0 - ASellFee) * APxPxL5 * self.info['AWeight'] + (1.0 - BSellFee) * BPxPxL5 * self.info['BWeight']) / ((1.0 + baseBuyFee) * basePxPxL5) - 1.0
            maxVaMargin = ((1.0 - ASellFee) * APxVaL5 * self.info['AWeight'] + (1.0 - BSellFee) * BPxVaL5 * self.info['BWeight']) / ((1.0 + baseBuyFeeOTC) * basePxVaL5) - 1.0
            outdict['Type']        = 'SPLIT'
            outdict['BaseFee']     = baseBuyFee
            outdict['BaseFeeOTC']  = baseBuyFeeOTC
            outdict['AFee']        = ASellFee
            outdict['BFee']        = BSellFee
            outdict['PriceMargin'] = pxMargin
            outdict['ValueMargin'] = valMargin
            outdict['BasePrice']   = basePrice
            outdict['APrice']      = APrice
            outdict['BPrice']      = BPrice
            outdict['BaseValue']   = baseValue
            outdict['AValue']      = AValue
            outdict['BValue']      = BValue
            outdict['PriceAmt']    = pxAmount 
            outdict['ValueAmt']    = valAmount
            outdict['MaxPxAmt']    = pxAmtL5
            outdict['MaxPxMargin'] = maxPxMargin
            outdict['MaxAPrice']   = APxPxL5
            outdict['MaxBPrice']   = BPxPxL5
            outdict['MaxBasePx']   = basePxPxL5
            outdict['MaxVaAmt']    = valAmtL5
            outdict['MaxVaMargin'] = maxVaMargin
            outdict['MaxAVaPx']    = APxVaL5
            outdict['MaxBVaPx']    = BPxVaL5
            outdict['MaxBaseVaPx'] = basePxVaL5
            return outdict
            
    def getArbMarginMerge(self, pxinfo, threshold=0.0):
        baseSellFee, baseSellFeeOTC, ABuyFee, BBuyFee = TxFeeHelper.getFee(type='MERGE', **self.info)
        baseInfo = pxinfo[self.info['Ticker']]
        AInfo    = pxinfo[self.info['ATicker']]
        BInfo    = pxinfo[self.info['BTicker']]
        
        # look at bid price for sell, ask price for buy
        APrice    = AInfo['a1_p'][0]
        BPrice    = BInfo['a1_p'][0]
        basePrice = baseInfo['b1_p'][0]
        AValue    = AInfo['value'][0]
        BValue    = BInfo['value'][0]
        baseValue = baseInfo['value'][0]
        
        # calculate margin
        buyPrice  = (1.0 + ABuyFee) * APrice * self.info['AWeight'] + (1.0 + BBuyFee) * BPrice * self.info['BWeight']
        sellPrice = (1.0 - baseSellFee) * basePrice
        sellValue = (1.0 - baseSellFeeOTC) * baseValue
        pxMargin  = sellPrice / buyPrice - 1.0
        valMargin = sellValue / buyPrice - 1.0
        
        # check margin
        if pxMargin < threshold and valMargin < threshold:
            # not any chance
            return None
        else:
            # if chance, check more details on amount and bid-ask spread
            outdict    = {}
            outdict.update(self.info)
            AAmount    = getQuoteAmount('a', 1, AInfo) / self.info['AWeight']
            BAmount    = getQuoteAmount('a', 1, BInfo) / self.info['BWeight']
            baseAmt    = getQuoteAmount('b', 1, baseInfo)
            valAmount  = min(50000, min([AAmount, BAmount]))
            pxAmount   = min([valAmount, baseAmt])
            AAmountL5  = getQuoteAmount('a', 5, AInfo) / self.info['AWeight']
            BAmountL5  = getQuoteAmount('a', 5, BInfo) / self.info['BWeight']
            baseAmtL5  = getQuoteAmount('b', 5, baseInfo)
            valAmtL5   = min([50000, AAmountL5, BAmountL5])
            pxAmtL5    = min([valAmtL5, baseAmtL5])
            APxPxL5    = getQuoteAvg('a', 5, AInfo, pxAmtL5 * self.info['AWeight'])
            BPxPxL5    = getQuoteAvg('a', 5, BInfo, pxAmtL5 * self.info['BWeight'])
            basePxPxL5 = getQuoteAvg('b', 5, baseInfo, pxAmtL5)
            APxVaL5    = getQuoteAvg('a', 5, AInfo, valAmtL5 * self.info['AWeight'])
            BPxVaL5    = getQuoteAvg('a', 5, BInfo, valAmtL5 * self.info['BWeight'])
            basePxVaL5 = baseValue
            maxPxMargin = ((1.0 - baseSellFee) * basePxPxL5) / ((1.0 + ABuyFee) * APxPxL5 * self.info['AWeight'] + (1.0 + BBuyFee) * BPxPxL5 * self.info['BWeight']) - 1.0
            maxVaMargin = ((1.0 - baseSellFeeOTC) * basePxVaL5) / ((1.0 + ABuyFee) * APxVaL5 * self.info['AWeight'] + (1.0 + BBuyFee) * BPxVaL5 * self.info['BWeight'])  - 1.0
            outdict['Type']        = 'MERGE'
            outdict['BaseFee']     = baseSellFee
            outdict['BaseFeeOTC']  = baseSellFeeOTC
            outdict['AFee']        = ABuyFee
            outdict['BFee']        = BBuyFee
            outdict['PriceMargin'] = pxMargin
            outdict['ValueMargin'] = valMargin
            outdict['BasePrice']   = basePrice
            outdict['APrice']      = APrice
            outdict['BPrice']      = BPrice
            outdict['BaseValue']   = baseValue
            outdict['AValue']      = AValue
            outdict['BValue']      = BValue
            outdict['PriceAmt']    = pxAmount 
            outdict['ValueAmt']    = valAmount
            outdict['MaxPxAmt']    = pxAmtL5
            outdict['MaxPxMargin'] = maxPxMargin
            outdict['MaxAPrice']   = APxPxL5
            outdict['MaxBPrice']   = BPxPxL5
            outdict['MaxBasePx']   = basePxPxL5
            outdict['MaxVaAmt']    = valAmtL5
            outdict['MaxVaMargin'] = maxVaMargin
            outdict['MaxAVaPx']    = APxVaL5
            outdict['MaxBVaPx']    = BPxVaL5
            outdict['MaxBaseVaPx'] = basePxVaL5
            return outdict
    
