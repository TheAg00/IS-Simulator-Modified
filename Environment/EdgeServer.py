from Environment.Intervals import Points
import heapq

class edge_server():
    '''
        A wrapper around Points utilising the interval logic defined
        there to model a processor processing a workload 
    '''
    def __init__(self, capacity, id=-1, shelfLimit  = -1):
        self.capacity = capacity
        self.index = id
        self.points = Points()
        self.jobs = []
        self.shelves = []
        
        self.shelfLimit = shelfLimit
        self.category = None

   

    def update(self, time):
        '''
            Simulates the progression of time for the processor.
            Removes completed jobs and their associated intervals. Partially completed jobs 
            remain in the list although their intervals are shrieked to start at 'time'
        '''
        busy_time = self.points.move_to_time(time)
       

        while self.jobs:
            if self.jobs[0][0] < time:
                heapq.heappop(self.jobs)
            else:
                break  
            
        return busy_time
    
    def update_shalves(self, time):
        '''
            Simulates the progression of time for the processor.
            Removes completed shelves and their associated intervals. Partially completed shelves 
            remain in the list although their intervals are shrieked to start at 'time'
        '''
        busy_time = self.points.move_to_time(time)


        while self.shelves:
            if self.shelves[0][0] >= time: break
            
            heapq.heappop(self.shelves)
            

         
        return busy_time
    
    def measure_remaining_busy_time(self):
        '''
            Calculates the remaining busy time of the jobs in the processor 
            DOES NOT UPDATE THE SERVER STATE!
        '''
        return self.points.measure_remaining_busy_time()

    def check_fit(self, job, track_leftover=False):
        '''
            Utilises the point methods to check if a job fits in the current state 
        '''
        if track_leftover:
            return self.points.check_fit_and_measure_leftover(job.ar, job.fin, job.req, self.capacity)
        else:
            return self.points.check_fit(job.ar, job.fin, job.req, self.capacity)

    def add_shelf(self, shelf):
        '''
            Implements the logic for adding a shelf to the set of points
        '''
        heapq.heappush(self.shelves, (shelf.fin, shelf))
        self.points.insert_interval(shelf.ar, shelf.fin, shelf.req)

    # def add_job(self, job):
    #     '''
    #         Implements the logic for adding a job to the set of points
    #     '''
    #     heapq.heappush(self.jobs, (job.fin, job))
        
    #     self.points.insert_interval(job.ar, job.fin, job.req)
        