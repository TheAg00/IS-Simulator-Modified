class HybridFirstFit:
    def __init__(self, scheduler, beta) -> None:
        self.scheduler = scheduler
        self.beta = beta
    
    def new_server(self, job, category):
        self.scheduler.add_server(job, category)

    def pack(self, job):
        if job.dur > 1 / self.beta:  # to avoid norm use max cap / b
            self.modified_first_fit(job, 'big')
        else:
            self.modified_first_fit(job, 'little')

    def modified_first_fit(self, job, category):
        servers = [m for m in self.scheduler.servers if m.category == category]

        # if there are no servers open a new one
        if not servers:
            self.new_server(job, category)
        else:
            # try to find the first server that can process the job
            handled = False
            for m in servers:
                fits = m.check_fit(job)
                if fits:
                    m.add_job(job)
                    handled = True
                    break
            # if no server was found open new one
            if not handled:
                self.new_server(job, category)