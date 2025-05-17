from Environment.EdgeServer import edge_server
import my_parser
import functions as f

from copy import deepcopy

class Scheduler:
    def __init__(self, wl, cores, alg):
        self.cores = cores
        self.alg = alg
        self.wl = wl

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
            case 'Improved_MS_Varaince_LOW' | 'Improved_MS_Varaince_HIGH':
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
        copy = Scheduler(self.wl, self.cores, self.alg)
        copy.total_bt = self.total_bt
        copy.total_servers = self.total_servers
        # Set to None to trigger an error when attempting to use a cloned scheduler
        # without explicitly setting the alg to prevent logical errors
        copy.algorithm = None 

        copy.servers = deepcopy(self.servers)
        return copy

    def add_server(self, job, category=None):
        server = edge_server(self.cores, len(self.servers)+1)
        server.category = category
        server.add_job(job)
        self.servers.append(server)

    # Συγχρονίζει τις καταστάσεις του μηχανήματος με βάση τον παρεχόμενο χρόνο.
    # Αυτό περιλαμβάνει τον υπολογισμό του makespan του κάθε μηχανήματος, την κατάργηση
    # ολοκληρωμένων διεργασιών και την απόφαση εάν ένα μηχάνημα θα πρέπει να
    # τερματιστεί. Τα μηχανήματα που δεν είναι πλέον ενεργά αφαιρούνται από το σύστημα.
    def update_all(self, time, close_empty=False):
        remove_list = []
        for m in self.servers:
            self.total_bt += m.update(time)
            if close_empty and m.points.head is None:
                remove_list.append(m)
        
        if close_empty:
            self.servers = [x for x in self.servers if x not in remove_list]

    def run(self, jobs):
        for j in jobs:
            self.update_all(j.ar, close_empty=True)
            self.algorithm.pack(j)

        for m in self.servers:
            self.total_bt += m.measure_remaining_busy_time()
        
        return self.total_bt
    
    def measure_to_end(self):
        busy_time_to_end = 0
        for m in self.servers:
            busy_time_to_end += m.measure_remaining_busy_time()

        return busy_time_to_end

    def run_batch(self, batch):
        for j in batch:
            self.update_all(j.ar, close_empty=True)
            self.algorithm.pack(j)

