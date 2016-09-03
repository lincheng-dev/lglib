# -*- coding: utf-8 -*-
import datetime, time
import math

def nextQuarterSeconds(dt):
    nsecs = float(dt.minute*60 + dt.second + dt.microsecond*1e-6)
    delta = math.ceil(nsecs / 900) * 900 - nsecs
    return delta

class JobScheduler(object):
    
    def _dummyRun(self, *args, **kwargs):
        print "Dummy job scheduler run, do nothing."
        
    def __init__(self, *args, **kwargs):
        # Default means just run one time
        # we expect to get a run mode and run parameters in a dict
        self.jobScheMode   = kwargs.get("SCHEMODE", "DEFAULT")
        self.jobScheParams = kwargs.get("SCHEPARAMS", {})
        self.jobFunc       = kwargs.get("JOBFUNC", self._dummyRun)
        self.jobParams     = kwargs.get("JOBPARAMS", 0.)
        self.keepRun       = True
        self.runCount      = 0
        
    def gotoNextRunTime(self):
        if self.jobScheMode.upper() == "DEFAULT":
            # only run one time right now
            if self.runCount == 0:
                return datetime.datetime.now()
            else:
                return None
        elif self.jobScheMode.upper() == "SIMPLEWAIT":
            # by default wait 900 seconds
            waitIvl = self.jobScheParams.get('WAIT', 900)
            repeat  = self.jobScheParams.get('REPEAT', 1)
            if self.runCount == 0:
                return datetime.datetime.now()
            elif self.runCount < repeat:
                print "Sleeping %s seconds" % str(waitIvl)
                time.sleep(waitIvl)
                return datetime.datetime.now()
            else:
                return None
        elif self.jobScheMode.upper() == "QUARTERHOUR":
            repeat  = self.jobScheParams.get('REPEAT', 1)
            if self.runCount < repeat:
                now     = datetime.datetime.now()
                waitIvl = nextQuarterSeconds(now)
                print "Sleeping %s seconds for next quarter" % str(waitIvl)
                time.sleep(waitIvl)
                return datetime.datetime.now()
            else:
                return None
        else:
            raise NotImplementedError       
                
    def run(self):
        print "Scheduled run started with mode %s" % self.jobScheMode
        while self.keepRun:
            nextRunTime = self.gotoNextRunTime()
            if nextRunTime is None:
                self.keepRun = False
                continue
            else:
                self.runCount += 1
                print "Running at %s for the %s time" % (str(nextRunTime), str(self.runCount))
                self.jobFunc(**self.jobParams)
        print "Job finished at %s" % str(datetime.datetime.now())
                
                
        
        
    