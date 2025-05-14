class FirstFit:
    def __init__(self, scheduler) -> None:
        self.scheduler = scheduler
    
    def new_server(self, job):
        self.scheduler.add_server(job)

    def pack(self, job, servers=None):
        
        if servers is None:
            servers = self.scheduler.servers
        
        # if there are no servers open a new one
        if not servers:
            self.new_server(job)
        else:
            # try to find the first server that can process the job
            for m in servers:
                # Ελέγχουμε αν η εργασία χωράει στο server.
                if m.check_fit(job):
                    m.add_job(job)
                    return 
   
            self.new_server(job)