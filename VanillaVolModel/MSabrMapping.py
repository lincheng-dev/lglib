#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy
import numpy.random
from numpy import exp
from numpy import log
import matplotlib.pyplot as pyplot

minFloat = numpy.finfo(float).eps
class MappingBase(object):
    # Base for smooth mapping from gaussian to non-gaussian
    def __init__(self, rf=0., b2=0., b3=0., s1=0., s2=1., nc=1.):
        self.rf = rf
        self.b2 = b2
        self.b3 = b3
        self.s1 = s1
        self.s2 = s2
        self.nc = nc
        self._validate()
    
    def _validate(self):
        assert self.s1 <= self.s2, "switch left should be smaller than switch right"
        assert self.s2 <= self.nc, "switch right should be smaller than normal cutoff"
    
    def Map(self, xlist):
        return [self.MapOnePoint(x) for x in xlist]
    
    def MapOnePoint(self, x):
        raise NotImplementedError      
    
    def MapStdGaussianSample(self, size, plotsample=False):
        assert size>0, "number of sample should be positive"
        xlist = numpy.random.normal(0., 1., size)
        slist = self.Map(xlist)
        if plotsample:
            f, (ax1, ax2) = pyplot.subplots(1, 2)
            ax1.scatter(xlist, slist)
            ax2.hist(xlist, size/100)
            ax2.hist(slist, size/100)  
            f.show()    
        return slist  
    
class Mapping3B(MappingBase):
    # 3B mapping     
    def __init__(self, rf=0.0, b2=0., b3=0., s1=0.01, s2=0.1, nc=1.):
        super(Mapping3B, self).__init__(rf,b2,b3,s1,s2,nc)
        self.c  = (s1+s2)/2.
        self.x1 = self.s1-self.c if abs(self.b2)<minFloat else log(2.*self.b2*(self.s1-self.c)+1.)/2./self.b2
        self.d1 = exp(2.*self.b2*self.x1)
        self.x2 = self.s2-self.c if abs(self.b2)<minFloat else log(2.*self.b2*(self.s2-self.c)+1.)/2./self.b2
        self.d2 = exp(2.*self.b2*self.x2)
        self.x3 = (self.nc-self.s2)/self.d2+self.x2 if abs(self.b3)<minFloat else log(2.*self.b3*(self.nc-self.s2)/self.d2+1)/2./self.b3+self.x2
        self.d3 = self.d2*exp(2.*self.b3*(self.x3-self.x2))
        self.b1 = self.d1/2./(self.s1-self.rf)
        
    def MapOnePoint(self, x):
        if x < self.x1:
            return self.s1+self.d1*(x-self.x1) if abs(self.b1)<minFloat else self.s1+self.d1*(exp(2.*self.b1*(x-self.x1))-1.)/2./self.b1
        elif x < self.x2:
            return self.c+x if abs(self.b2)<minFloat else self.c+(exp(2.*self.b2*x)-1.)/2./self.b2
        elif x < self.x3:
            return self.s2+self.d2*(x-self.x2) if abs(self.b3)<minFloat else self.s2+self.d2*(exp(2.*self.b3*(x-self.x2))-1.)/2./self.b3
        else:
            return self.nc + self.d3*(x-self.x3)
            
class Mapping5B(Mapping3B):
    # 5B mapping     
    def __init__(self, rf=0., b2=0., b3=0., s1=0., s2=1., nc=1.):
        super(Mapping5B, self).__init__(rf,b2,b3,s1,s2,nc)
        self.nc0 = max(self.rf/2., rf+0.0005)
        self.nc2 = 2*self.nc-self.s2
        self.b4  = -self.b3
        self.x0  = (self.nc0-self.s1)/self.d1+self.x1 if abs(self.b1)<minFloat else log(2.*self.b1*(self.nc0-self.s1)/self.d1+1)/2./self.b1+self.x1
        self.d0  = self.d1*exp(2.*self.b1*(self.nc0-self.x1))
        self.x3  = (self.nc-self.s2)/self.d2+self.x2 if abs(self.b3)<minFloat else log(2.*self.b3*(self.nc-self.s2)/self.d2+1)/2./self.b3+self.x2
        self.x4  = (self.nc2-self.nc)/self.d3+self.x3 if abs(self.b4)<minFloat else log(2.*self.b4*(self.nc2-self.nc)/self.d3+1)/2./self.b4+self.x3
        self.d4  = self.d3*exp(2.*self.b4*(self.nc2-self.nc))     
        print self.nc0, self.nc, self.nc2
        print self.b1, self.b2, self.b3, self.b4
        print self.x0, self.x1, self.x2, self.x3, self.x4
        print self.d0, self.d1, self.d2, self.d3, self.d4
        
    def MapOnePoint(self, x):
        if x < self.x0:
            return self.nc0+self.d0*(x-self.x0)
        elif x < self.x1:
            return self.s1+self.d1*(x-self.x1) if abs(self.b1)<minFloat else self.s1+self.d1*(exp(2.*self.b1*(x-self.x1))-1.)/2./self.b1
        elif x < self.x2:
            return self.c+x if abs(self.b2)<minFloat else self.c+(exp(2.*self.b2*x)-1.)/2./self.b2
        elif x < self.x3:
            return self.s2+self.d2*(x-self.x2) if abs(self.b3)<minFloat else self.s2+self.d2*(exp(2.*self.b3*(x-self.x2))-1.)/2./self.b3
        elif x < self.x4:
            return self.nc+self.d3*(x-self.x3) if abs(self.b4)<minFloat else self.nc+self.d3*(exp(2.*self.b4*(x-self.x3))-1.)/2./self.b4
        else:
            return self.nc2 + self.d4*(x-self.x4)
        