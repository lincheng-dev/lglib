#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy
import numpy.random
import matplotlib.pyplot as pyplot
import BlackScholes
from scipy.optimize import brentq

class MappingBase(object):
    # Base for smooth mapping from gaussian to non-gaussian
    def __init__(self, rf=-0.05, b=10., br=10., sl=0.01, sr=0.1, nc=1.):
        self.rf = rf
        self.b  = b
        self.br = br
        self.sl = sl
        self.sr = sr
        self.nc = nc
        self._validate()
    
    def _validate(self):
        assert self.rf <= self.sl, "rate floor should be smaller than switch left"
        assert self.sl <= self.sr, "switch left should be smaller than switch right"
        assert self.sr <= self.nc, "switch right should be smaller than normal cutoff"
    
    def Map(self, xlist):
        return [self.MapOnePoint(x) for x in xlist]
    
    def MapOnePoint(self, x):
        raise NotImplementedError    
        
    def MapDerivOnePoint(self, x):
        raise NotImplementedError  
    
    def MapGaussianSample(self, mean=0., stdev=0.1, size=10000, plotsample=False, binsize=100):
        assert size>0, "number of sample should be positive"
        xlist = numpy.random.normal(mean, stdev, size)
        slist = self.Map(xlist)
        if plotsample:
            f, (ax1, ax2) = pyplot.subplots(1, 2)
            ax1.scatter(xlist, slist)
            ax2.hist(xlist, size/binsize)
            ax2.hist(slist, size/binsize)  
            f.show()    
        return slist  
        
    def SampleStatistics(self, meanlist=[0.,], stdevlist=[0.1,], size=10000, plotvol=False):
        assert size>0, "number of sample should be positive"
        resultDict = {}
        lencol     = len(stdevlist)
        lenrow     = len(meanlist)
        plotcount  = 1
        for mean in meanlist:
            for stdev in stdevlist:
                slist  = numpy.array(self.MapGaussianSample(mean, stdev, size))
                smean  = numpy.mean(slist)
                sstdev = numpy.std(slist)
                klist  = numpy.arange(-8,8)*sstdev/4.+smean
                klist  = self.rf+(klist-self.rf).clip(0.)
                klist  = self.nc-(self.nc-klist).clip(0.)
                plist  = [numpy.mean((slist-k).clip(0.)) for k in klist]
                vlist  = []
                try:
                    vlist  = [brentq(BlackScholes.getNormalVolSolvFuncCall(smean,k,1.0,p), BlackScholes.MINFLOAT, 100.*sstdev) for k,p in zip(klist,plist)]
                except:
                    print "Failed for mean=%8.3e stdev=%8.3e, vol larger than %8.3e" % (mean, stdev, 100.*sstdev)
                    continue
                if plotvol:
                    pyplot.subplot(lenrow, lencol, plotcount)
                    pyplot.plot(klist, vlist, 'ko-')
                    pyplot.title('xm=%8.3e, xs=%8.3e, sm=%8.3e, ss=%8.3e' % (mean, stdev, smean, sstdev))
                    pyplot.plot(numpy.array([smean]*11), numpy.arange(11)/10.*(max(vlist)-min(vlist))+min(vlist), 'r--')
                    plotcount+=1
                resultDict[(mean, stdev)] = (smean, sstdev, klist, plist, vlist)
        if plotvol:
            pyplot.show()
        return resultDict
        
    @staticmethod 
    def BMap(xt, k, x, d, q):
        """
        calculate k+d*[exp(q(xt-x))-1]/q
        """
        return k+d*(xt-x) if abs(q)<BlackScholes.MINFLOAT else k+d*(exp(q*(xt-x))-1.)/q
    
class Mapping3B(MappingBase):
    # 3B mapping     
    def __init__(self, rf=-0.05, b=10., br=10., sl=0.01, sr=0.1, nc=1.):
        super(Mapping3B, self).__init__(rf,b,br,sl,sr,nc)
        self.k2 = (self.sl+self.sr)/2.
        self.x2 = 0.
        self.d2 = 1.
        self.q2 = 2.*self.b
        
        self.k1 = self.sl
        self.x1 = self.k1-self.k2 if abs(self.q2)<BlackScholes.MINFLOAT else log(self.q2*(self.k1-self.k2)+1.)/self.q2
        self.d1 = exp(self.q2*self.x1)
        self.q1 = self.d1/(self.sl-self.rf)
        
        self.k3 = self.sr
        self.x3 = self.k3-self.k2 if abs(self.q2)<BlackScholes.MINFLOAT else log(self.q2*(self.k3-self.k2)+1.)/self.q2
        self.d3 = exp(self.q2*self.x3)
        self.q3 = 2*self.br
        
        self.k4 = self.nc
        self.x4 = (self.k4-self.k3)/self.d3+self.x3 if abs(self.q3)<BlackScholes.MINFLOAT else log(self.q3*(self.k4-self.k3)/self.d3+1)/self.q3+self.x3
        self.d4 = self.d3*exp(self.q3*(self.x4-self.x3))
        self.q4 = 0.
        
    def MapOnePoint(self, x):
        if x < self.x1:
            return MappingBase.BMap(x, self.k1, self.x1, self.d1, self.q1)
        elif x < self.x3:
            return MappingBase.BMap(x, self.k2, self.x2, self.d2, self.q2)
        elif x < self.x4:
            return MappingBase.BMap(x, self.k3, self.x3, self.d3, self.q3)
        else:
            return MappingBase.BMap(x, self.k4, self.x4, self.d4, self.q4)
            
class Mapping5B(Mapping3B):
    # 5B mapping     
    def __init__(self, rf=-0.05, b=10., br=10., sl=0.01, sr=0.1, nc=1.):
        super(Mapping5B, self).__init__(rf,b,br,sl,sr,nc)
        self.k0 = max(self.rf/2., self.rf+0.0005)
        self.x0 = (self.k0-self.k1)/self.d1+self.x1 if abs(self.q1)<BlackScholes.MINFLOAT else log(self.q1*(self.k0-self.k1)/self.d1+1)/self.q1+self.x1
        self.d0 = self.d1*exp(self.q1*(self.x0-self.x1))
        self.q0 = 0.
        
        self.q4 = -self.q3
        
        self.k5 = 2.*self.nc-self.sr
        self.x5 = (self.k5-self.k4)/self.d4+self.x4 if abs(self.q4)<BlackScholes.MINFLOAT else log(self.q4*(self.k5-self.k4)/self.d4+1)/self.q4+self.x4
        self.d5 = self.d4*exp(self.q4*(self.x5-self.x4))
        self.q5 = 0.
            
        #print '%12.5f%12.5f%12.5f%12.5f%12.5f%12.5f' % (self.k0, self.k1, self.k2, self.k3, self.k4, self.k5)
        #print '%12.5f%12.5f%12.5f%12.5f%12.5f%12.5f' % (self.x0, self.x1, self.x2, self.x3, self.x4, self.x5)
        #print '%12.5f%12.5f%12.5f%12.5f%12.5f%12.5f' % (self.d0, self.d1, self.d2, self.d3, self.d4, self.d5)
        #print '%12.5f%12.5f%12.5f%12.5f%12.5f%12.5f' % (self.q0, self.q1, self.q2, self.q3, self.q4, self.q5)
        
    def MapOnePoint(self, x):
        if x < self.x0:
            return MappingBase.BMap(x, self.k0, self.x0, self.d0, self.q0)
        elif x < self.x1:
            return MappingBase.BMap(x, self.k1, self.x1, self.d1, self.q1)
        elif x < self.x3:
            return MappingBase.BMap(x, self.k2, self.x2, self.d2, self.q2)
        elif x < self.x4:
            return MappingBase.BMap(x, self.k3, self.x3, self.d3, self.q3)
        elif x < self.x5:
            return MappingBase.BMap(x, self.k4, self.x4, self.d4, self.q4)
        else:
            return MappingBase.BMap(x, self.k5, self.x5, self.d5, self.q5)
        