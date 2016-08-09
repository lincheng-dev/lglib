#!/usr/bin/env python
# -*- coding: utf-8 -*-
import numpy
from numpy import sqrt, pi, exp, log
from scipy.stats import norm

ONEOVERSQRT2PI = 1./sqrt(2.*pi)
MINFLOAT       = numpy.finfo(float).eps

def normalCallPrice(f, k, t, v):
    if v <= MINFLOAT:
        return max([f-k, 0.])
    vt = v * sqrt(t)
    d1 = (f-k)/vt
    return (f-k)*norm.cdf(d1)+vt*ONEOVERSQRT2PI*exp(-d1*d1/2.)
    
def getNormalVolSolvFuncCall(f, k, t, p):
    def func(v):
        return normalCallPrice(f, k, t, v)-p
    return func
    
    