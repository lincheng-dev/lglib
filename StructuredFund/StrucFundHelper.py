#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
from numpy import floor, abs

def NotExchFund(ticker):
    return ticker.startswith('16')
    
def getQuoteAmount(side, num, pxinfo):
    totalAmt = 0
    for i in xrange(1, num+1):
        name = side + str(i) + '_v'
        totalAmt += pxinfo.get(name, [0])[0]*100
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
        if (type, ticker) in feeList:
            return feeList[(type, ticker)]
        else:
            fee = cls.calcFee(type, args, kwargs)
            feeList[(type, ticker)] = fee 
            return fee
    
    @classmethod
    def calcFee(cls, type, *args, **kwargs):
        Ticker  = kwargs['Ticker']
        ATicker = kwargs['ATicker']
        BTicker = kwargs['BTicker']
        return cls.__getattr__('calcFee'+type.upper())(Ticker, ATicker, BTicker)
        
    @classmethod
    def calcFeeSplit(cls, Ticker, ATicker, BTicker):
        buyFee    = BUYFEEEXCEPT.get(Ticker, ExchTxFee)
        buyFeeOTC = BUYFEEEXCEPTOTC.get(Ticker, OTCFundBuy)
        sellFeeA  = SELLFEEEXCEPT.get(ATicker, ExchTxFee)
        sellFeeB  = SELLFEEEXCEPT.get(BTicker, ExchTxFee)
        if NotExchFund(Ticker):
            buyFee = buyFeeOTC
        return (buyFee, buyFeeOTC, sellFeeA, sellFeeB)

    @classmethod
    def calcFeeMerge(cls, Ticker, ATicker, BTicker):
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
        if cls.helperList.find(ticker):
            return cls.helperList[ticker]
        else:
            helperList[ticker] = StrucFundHelper(args, kwargs)
            
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
    
    def getArbMargin(self, type, pxinfo):
        if type.upper() == 'SPLIT':
            return self.getArbMarginSplit(pxinfo).update(self.info)
        elif type.upper() == 'MERGE':
            return self.getArbMarginMerge(pxinfo).update(self.info)
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
        AValue    = APx['value'][0]
        BValue    = BPx['value'][0]
        baseValue = basePx['value'][0]
        
        # calculate margin
        buyPrice  = (1.0 + baseBuyFee) * basePrice
        buyValue  = (1.0 + baseBuyFeeOTC) * baseValue
        sellPrice = (1.0 + ASellFee) * APrice * self.info['AWeight'] + (1.0 + BSellFee) * BPrice * self.info['BWeight']
        pxMargin  = sellPrice / buyPrice - 1.0
        valMargin = sellPrice / buyValue - 1.0
        
        # check margin
        if pxMargin < threshold and valMargin < threshold:
            # not any chance
            return None
        else:
            # if chance, check more details on amount and bid-ask spread
            outdict    = {}
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
            maxPxMargin = ((1.0 + ASellFee) * APxPxL5 * self.info['AWeight'] + (1.0 + BSellFee) * BPxPxL5 * self.info['BWeight']) / ((1.0 + baseBuyFee) * basePxPxL5) - 1.0
            maxVaMargin = ((1.0 + ASellFee) * APxVaL5 * self.info['AWeight'] + (1.0 + BSellFee) * BPxVaL5 * self.info['BWeight']) / ((1.0 + baseBuyFeeOTC) * basePxVaL5) - 1.0
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
            outdict['BValue']      = AValue
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
            
    def getArbMarginMerge(self, pxinfo, threshold=0.0):
        baseSellFee, baseSellFeeOTC, ABuyFee, BBuyFee = TxFeeHelper.getFee(type='MERGE', **self.info)
        baseInfo = pxinfo[self.info['Ticker']]
        AInfo    = pxinfo[self.info['ATicker']]
        BInfo    = pxinfo[self.info['BTicker']]
        
        # look at bid price for sell, ask price for buy
        APrice    = AInfo['a1_p'][0]
        BPrice    = BInfo['a1_p'][0]
        basePrice = baseInfo['b1_p'][0]
        AValue    = APx['value'][0]
        BValue    = BPx['value'][0]
        baseValue = basePx['value'][0]
        
        # calculate margin
        buyPrice  = (1.0 + ABuyFee) * APrice * self.info['AWeight'] + (1.0 + BBuyFee) * BPrice * self.info['BWeight']
        sellPrice = (1.0 + baseSellFee) * basePrice
        sellValue = (1.0 + baseSellFeeOTC) * baseValue
        pxMargin  = sellPrice / buyPrice - 1.0
        valMargin = sellValue / buyPrice - 1.0
        
        # check margin
        if pxMargin < threshold and valMargin < threshold:
            # not any chance
            return None
        else:
            # if chance, check more details on amount and bid-ask spread
            outdict    = {}
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
            maxPxMargin = ((1.0 + baseSellFee) * basePxPxL5) / ((1.0 + ABuyFee) * APxPxL5 * self.info['AWeight'] + (1.0 + BBuyFee) * BPxPxL5 * self.info['BWeight']) /  - 1.0
            maxVaMargin = ((1.0 + baseSellFeeOTC) * basePxVaL5) / ((1.0 + ABuyFee) * APxVaL5 * self.info['AWeight'] + (1.0 + BBuyFee) * BPxVaL5 * self.info['BWeight'])  - 1.0
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
            outdict['BValue']      = AValue
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
    
