from math import sqrt

class HybridAlgorithm:
    def __init__(self, scheduler, i) -> None:
        self.scheduler = scheduler
        self.i = i
    
    def new_server(self, job, category):
        self.scheduler.add_server(job, category)

    def calculate_jobs_type(self, job):
        return None
    
    def cal_load_of_type(self, t, job):
        servers = [m for m in self.scheduler if m.type == t]    
        load = 0
        for m in servers:
            for j in m.jobs:
                load += j.dur * j.req

        load += job.dur * job.req
        return load
    
    def pack(self, job):
        t = self.calculate_jobs_type(job)

        cd_same_type = [x for x in self.ha_servers['cd'] if x.t == t]

        if cd_same_type:
            self.modified_first_fit(job, t)
        else:
            load = self.cal_load_of_type(t, job)
            if load <= 1/(2 * sqrt(self.i)):
                self.modified_first_fit(job, 'general')
            else:
                self.new_server(job,)

    def modified_first_fit(self, job, category):
        servers = [m for m in self.scheduler.servers if m.type == category]

        # if there are no servers open a new one
        if not servers:
            self.new_server(job, category=category)
        else:
            # try to find the first server that can proc the job
            handled = False
            for m in servers:
                fits = self.scheduler.check_fit(m, job)
                if fits:
                    m.add_job(job)
                    handled = True
                    break
            # if no server was found open new one
            if not handled:
                self.new_server(job, category)
