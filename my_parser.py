class job:
    def __init__(self, ar, fin, dur, req):
        self.ar = ar
        self.fin = fin
        self.dur = dur
        self.req = req
        self.timestamp_beg = None
        self.timestamp_end = None

    def __str__(self) -> str:
        return f"|A:{self.ar} F:{self.fin} D:{self.dur} R:{self.req}|"    
    
    def __repr__(self) -> str:
        return self.__str__()
    
    ######## COMPARATORS ########
    def __le__(self, other):
        return self.fin <= other.fin
    
    def __lt__(self, other):
        return self.fin < other.fin

    def __gt__(self, other):
        return self.fin > other.fin
    
    def __ge__(self, other):
        return self.fin >= other.fin
    
    def __eq__(self, other):
        return self.fin == other.fin
    ######## COMPARATORS ########

    def info(self):
        print('Arrived at:', self.ar, 'I will be done at:', self.fin,
              'dur:', self.dur, 'I need', self.req, 'cores!')
    
    def info_time(self):
        print('arrived:',self.timestamp_beg,'finished:',self.timestamp_end)


def parse(path, cores=32, cluster=False):
    jobs = []

    with open(path, "rt") as f:
        for line in f:
            job_info = line.rstrip().split('\t')
            arrival, wait_for, duration, requirements = (int(x) for x in job_info)
            arrival += max(wait_for, 0) # to ignore -1 which is used when jobs do not wait
            finish_time = arrival + duration

            # checking if the job requires less cores than machine capacity and has a valid duration
            if cores > requirements > 0 and duration > 0:
                if not cluster:
                    jobs.append(job(arrival, finish_time, duration, requirements))
                else:
                    for _ in range(requirements):
                        jobs.append(job(arrival, finish_time, duration, 1))

    # sorting by arrival
    jobs = sorted(jobs, key=lambda x: x.ar)


    return jobs