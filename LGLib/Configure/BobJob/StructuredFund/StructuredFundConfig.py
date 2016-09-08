#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import LGLib
libpath = os.path.dirname(os.path.realpath(__file__))
outpath = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(libpath))))
STRUCFUND_JOB_CONFIG = {
'SCHEMODE'     : 'QUARTERHOUR',
'REPEAT'       : 36,
'threshold'    : -0.001,
'mode'         : 'DETAIL',
'dumpPath'     : os.path.join(outpath, 'Results', 'StructuredFund'),
'fundInfoFile' : os.path.join(libpath, 'fundInfo.csv')
}
