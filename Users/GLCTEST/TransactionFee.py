#!/usr/bin/env python
# -*- coding: utf-8 -*-
class TxSingleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(TxSingleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class TxFee(object):
    
    def __init__(self, txRate):
        self.txRate = txRate
        
    def getTxFeeRate(self):
        return self.txRate
        
    def getTxFee(self, txAmount):
        return self.txRate * abs(txAmount)
        
class TxFeeFundExchTrade(TxFee): 
    __metaclass__ = TxSingleton
    def __init__(self, txRate=0.0003):
        super(TxFeeFundExchTrade, self).__init__(txRate)

class TxFeeFundSub(TxFee):
    
    def __init__(self, txRate=0.015):
        super(TxFeeFundSub, self).__init__(txRate)

class TxFeeFundRedemp(TxFee):
     
     def __init__(self, txRate=0.005):
        super(TxFeeFundRedemp, self).__init__(txRate)
     
       
