from Environment.EdgeServer import edge_server
from Environment.Moldable_task_model import *
import functions as f


from copy import deepcopy
import math
import random

class Scheduler:
    def __init__(self, wl, cores, alg, variance):
        self.cores = cores
        self.alg = alg
        self.wl = wl
        self.variance = variance

        self.servers = []
        self.total_bt = 0
        self.total_servers = 0

        self.algorithm = None
        self.set_algorithm(alg, workload=self.wl)
    
    def __str__(self):
        return f"Scheduler -> cores:{self.cores} wl:{self.wl} alg:{self.alg} total_bt:{self.total_bt} opened_servers:{len(self.servers)}"

    def set_algorithm(self, alg, **kwargs):
        match alg:
            case 'FF':
                from Environment.Algorithms.FirstFit import FirstFit
                self.alg = "FF"
                self.algorithm = FirstFit(self)
            case 'MET_AD':
                from Environment.Algorithms.MET_AD import MET_AD
                self.alg = "MET_AD"
                if 'workload' in kwargs:
                    thresholds = f.get_threshold('MET_AD', self.cores)
                    self.algorithm = MET_AD(self, thresholds[kwargs['workload']])
                elif 'threshold' in kwargs:
                    self.algorithm = MET_AD(self, kwargs['threshold'])
                else:
                    self.algorithm = MET_AD(self, None)
            case 'BF':
                from Environment.Algorithms.BestFit import BestFit
                self.alg = "BF"
                self.algorithm = BestFit(self)
            case 'HFF':
                from Environment.Algorithms.HybridFirstFit import HybridFirstFit
                self.alg = "HFF"
                self.algorithm = HybridFirstFit(self, .25)
            case 'BCFF':
                from Environment.Algorithms.Bucket import Bucket
                self.alg = "BCFF"
                # FIGURE OUT HOW THE CATEGORIES ARE IMPLEMENTED AND WHAT VALUES ARE USED
                self.algorithm = Bucket(self, None, None)
                raise(NotImplementedError("There are some parameters for this algorithm that Im not sure how they work (ask Panos?)"))
            case 'HA':
                from Environment.Algorithms.HybridAlgorithm import HybridAlgorithm
                self.alg = "HA"
                # FIGURE OUT HOW THE CATEGORIES ARE IMPLEMENTED AND WHAT VALUES ARE USED
                self.algorithm = HybridAlgorithm(self, None)
                raise(NotImplementedError("There are some parameters for this algorithm that Im not sure how they work (ask Panos?)"))
            case 'BFMAT':
                from Environment.Algorithms.BF_mat import BF_MAT
                self.alg = "BFAT"
                self.algorithm = BF_MAT(self)
            case 'Improved_MS':
                from Environment.Algorithms.Improved_MS import Improved_MS
                self.alg = alg
                self.algorithm = Improved_MS(self)
            case _:
                raise ValueError(f"'{alg}' is not a supported algorithm.")
            
    def reset(self):
        self.servers = []
        self.total_bt = 0
        self.total_servers = 0

    def clone(self):
        copy = Scheduler(self.wl, self.cores, self.alg, self.variance)
        copy.total_bt = self.total_bt
        copy.total_servers = self.total_servers

        # Set to None to trigger an error when attempting to use a cloned scheduler
        # without explicitly setting the alg to prevent logical errors
        copy.algorithm = None 

        copy.servers = deepcopy(self.servers)
        return copy

    def add_server(self, job, category=None):
        server = edge_server(self.cores, len(self.servers) + 1)
        server.category = category
        server.add_job(job)
        self.servers.append(server)
    
    def add_server_shelves(self, shelf, category = None):
        server = edge_server(self.cores, len(self.servers) + 1)
        server.category = category
        server.add_shelf(shelf)
        self.servers.append(server)

    
    # Υπολογίζουμε το busy time για κάθε server που εκτελεί κάποια εργασία και καταργούμε τους servers που είναι άδειοι.
    def update_all(self, time, close_empty=False):
        remove_list = [] # Λίστα με τους servers που θα καταργήσουμε.

        # Μπαίνει μόνο όταν υπάρχουν servers με εργασίες μέσα.
        for m in self.servers:
            if self.alg == 'Improved_MS':
                self.total_bt += m.update_shelf(time) # Μετράμε το συνολικό busy time
            else:
                self.total_bt += m.update(time)
            
            if close_empty and m.points.head is None: remove_list.append(m)
        
        # Καταργούμε τους άδειους servers.
        if close_empty and remove_list: self.servers = [x for x in self.servers if x not in remove_list]
        
    # Μετατρέπουμε τις εργασίες των μη moldable αλγορίθμων(π.χ. first fit) σε moldable,
    # ώστε οι συγκρίσεις που θα κάνουμε να αναπαριστούν το βέλτιστο busy time κάθε φορά.
    def makeMoldable(self, job):
        averageParallelism = job.req # Απαιτήσεις σε πυρήνες της εργασίας

        # Μετατρέπουμε σε moldable τις εργασίες με χαμηλή διακύμανση. 
        if self.variance == 'LOW':
            sigma = random.uniform(0, 1)
            optimalCores = optimal_coresLOW(averageParallelism, sigma, self.cores)
            newDur = math.ceil(duration_with_nLOW(job, averageParallelism, optimalCores, sigma))

            job.req = optimalCores
            job.dur = newDur

            return

        # Μετατρέπουμε σε moldable τις εργασίες με υψηλή διακύμανση.
        sigma = random.uniform(1.01, 10)
        optimalCores = optimal_coresHIGH(averageParallelism, sigma, self.cores)
        newDur = math.ceil(duration_with_nHIGH(job, averageParallelism, optimalCores, sigma))

        job.req = optimalCores
        job.dur = newDur

        return

    def run(self, jobs):
        for j in jobs:
            if self.alg != 'Improved_MS': self.makeMoldable(j)
                
            self.update_all(j.ar, close_empty=True)
            self.algorithm.pack(j)


        for m in self.servers: self.total_bt += m.measure_remaining_busy_time()

        return math.ceil(self.total_bt)
    
    def measure_to_end(self):
        busy_time_to_end = 0
        for m in self.servers:
            busy_time_to_end += m.measure_remaining_busy_time()

        return busy_time_to_end

    def run_batch(self, batch):
        for j in batch:
            self.update_all(j.ar, close_empty=True)
            self.algorithm.pack(j)

