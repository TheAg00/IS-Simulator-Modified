class BF_MAT:
    def __init__(self, scheduler) -> None:
        self.scheduler = scheduler
    
    def new_server(self, job):
        self.scheduler.add_server(job)

    def pack(self, job):
        servers = self.scheduler.servers
        # if there are no servers open a new one
        if not servers:
            self.new_server(job)
        else:
            max_requests = None
            idx = -1
            for i, m in enumerate(servers):
                fits = m.check_fit(job)
                requests = len(m.jobs)
                if fits and (max_requests is None or max_requests > requests):
                    idx = i
                    max_requests = requests
            if idx != -1:
                servers[idx].add_job(job)
            else:
                self.new_server(job)
