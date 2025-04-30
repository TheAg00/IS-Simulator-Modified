from Environment.Algorithms.FirstFit import FirstFit
import heapq

class MET_AD:
    def __init__(self, scheduler, threshold) -> None:
        self.scheduler = scheduler
        self.first_first = FirstFit(scheduler)
        self.threshold = threshold
    
    def new_server(self, job):
        self.scheduler.add_server(job)

    def pack(self, job):
        sub_set = []
        
        # for every server find min/max calculate ratios
        # and append to sub set if < threshold
        for m in self.scheduler.servers:
            min_duration_server = m.jobs[0][1].dur
            ratio_min = max(min_duration_server, job.dur) / min(min_duration_server, job.dur)

            if ratio_min > self.threshold:
                continue

            max_duration_server = heapq.nlargest(1, m.jobs)[0][1].dur
            ratio_max = max(max_duration_server, job.dur) / min(max_duration_server, job.dur)

            if ratio_max <= self.threshold:
                sub_set.append(m)

        if not sub_set:
            self.new_server(job)
        else:
            self.first_first.pack(job, sub_set)