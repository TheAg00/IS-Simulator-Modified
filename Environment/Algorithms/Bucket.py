class Bucket:
    def __init__(self, scheduler, num_categories, k) -> None:
        self.scheduler = scheduler

        self.categories = {}

        for i in range(1, num_categories+1):
            self.categories[i] = {'lower_bound': 2 ^ k,
                                'upper_bound': 2 ^ (k+1) - 1}
            
    def new_server(self, job, category):
        self.scheduler.add_server(job, category=category)

    def pack(self, job):
        for key in self.categories:
            if self.categories[key]['lower_bound'] < job.dur < self.categories[key]['upper_bound']:
                self.modified_first_fit(job, self.categories[key])

    def modified_first_fit(self, job, category):
        servers = [m for m in self.scheduler.servers if m.category == category]

        # if there are no servers open a new one
        if not servers:
            self.new_server(job, category=category)
        else:
            # try to find the first server that can process the job
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
