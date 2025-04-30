import statistics as st

class BestFit:
    def __init__(self, scheduler) -> None:
        self.scheduler = scheduler
    
    def new_server(self, job):
        self.scheduler.add_server(job)

    def pack(self, job):
        # if there are no servers open a new one
        if not self.scheduler.servers:
            self.new_server(job)
        else:
            min_leftover = None
            candidate_server = -1
            for i, m in enumerate(self.scheduler.servers):
                fits, leftovers = m.check_fit(job, track_leftover=True)
                if fits:
                    average_leftover = st.mean(leftovers)
                    if min_leftover is None or average_leftover < min_leftover:
                        min_leftover = average_leftover
                        candidate_server = i

            if candidate_server != -1:
                self.scheduler.servers[candidate_server].add_job(job)
            else:
                self.new_server(job)
