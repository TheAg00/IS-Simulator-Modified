from Environment.Moldable_task_model import optimalCoresLOW
import random

class MoldableSchedulingImproved:
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.varianceModel = "high"
        self.moldedJobhistory = {"before": [], "after": []}

    
    def pack(self, job, servers = None, openNew = True):
        

        if servers == None: servers = self.scheduler.servers

        
            
