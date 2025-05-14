from Environment.Moldable_task_model import *

import random
import math
import heapq

class Improved_MS:
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.varianceModel = "low"
        self.moldedJobhistory = {"before": [], "after": []}
        self.coreCapacity = {server: server.cores for server in self.scheduler.servers}
        self.schedulingAlgorithm = FirstFit(self.scheduler)
        self.shelves = []

        self.alpha = None
        self.beta = 1.56
        self.r = 1.44
        self.height = 0

    # Δημιουργούμε νέο server.
    def new_server(self, job):
        self.scheduler.add_server(job)

    # Για κάθε server, ελέγχουμε για έναν διαφορετικό αριθμό πυρήνων s το χρόνο ολοκλήρωσης της εργασίας job.
    # Αν ο χρόνος ολοκλήρωσης είναι <= α, τότε προσθέτουμε ένα σετ (server, s) στη λίστα με τα servers που πληρούν τις προϋποθέσεις.
    def f(self, job):
        eligibleCores = list()
        for server in self.scheduler.servers:
            for s in range(1, self.coreCapacity[server] + 1):
                if  job[s] <= self.alpha:
                    eligibleCores.append(s)
        
        return eligibleCores

    # Μετατρέπουμε την εργασία σε moldable.
    def convert_to_mold(self, job):
        # Λεξικό που θα κρατάει το χρόνο ολοκλήρωσης της εργασίας για κάθε αριθμό πυρήνων στο διάστημα [1, scheduler.cores]
        moldedJobDict = {}

        averageParallelism = job.req # Απαιτήσεις σε πυρήνες που θέλει η εργασία για το συγκεκριμένο duration.
        for currentCores in range(1, self.scheduler.cores + 1):
            # Το sigma παίρνει τυχαία τιμή στο διάστημα [0, 1] αν είναι low variance και [1.01, 10] αν είναι high variance.
            if self.varianceModel == "low":
                sigma = random.uniform(0, 1)
                newDur = math.ceil(duration_with_nLOW(job, averageParallelism, currentCores, sigma))
            else:
                sigma = random.uniform(1.01, 10)
                newDur = math.ceil(duration_with_nHIGH(job, averageParallelism, currentCores, sigma))

            moldedJobDict.update({currentCores: newDur})

        
        return moldedJobDict

    # Ενημερώνουμε το νέο αριθμό διαθέσημων πυρήνων μετά τον προγραμματισμό της εργασίας.
    def updateCoreCapacity(self, server, usedCores):
        self.coreCapacity[server] -= usedCores
    
    def minCost(self, moldedJobDict, eligibleCores):
        minCores = 1
        minProcessingTime = moldedJobDict[minCores]
        minProduct = minCores * minProcessingTime

        for currentCore in eligibleCores[1:]:
            currentProduct = currentCore * moldedJobDict[currentCore]
            if currentProduct < minProduct:
                minProduct = currentProduct
                minCores = currentCore
                minProcessingTime = moldedJobDict[currentCore]

        return minProcessingTime, minCores

    def pack_to_shelf(self, job):
        s = job.req
        p = job.dur

        if s > self.scheduler.cores // 2:
            # Big job → full shelf
            shelf = Shelf(p, self.scheduler.cores)
            shelf.add_job(job)
            self.shelves.append(shelf)
        else:
            # Small job → pack in shelf of height r^k
            k = 0
            while True:
                
                lowerBound = pow(self.r, k)
                upperBound = pow(self.r, k + 1)

                # Αν υπάρχει εργασία που έχει χρόνο ολοκλήρωσης 1, τότε τοποθετείται στο ράφι με ύψος 1.44.
                if lowerBound < p <= upperBound or p == 1.0: break
                k += 1

            # First Fit: try to find shelf of that height
            for shelf in self.shelves:
                if shelf.height == upperBound and shelf.shelfFit(s):
                    shelf.add_job(job)
                    return
                
            # Else, create new shelf
            new_shelf = Shelf(upperBound, self.scheduler.cores)
            new_shelf.add_job(job)
            self.shelves.append(new_shelf)


    def pack(self, job, servers = None, openNew = True):
        moldedJobDict = self.convert_to_mold(job) # {..., s_n: p(s_n, j), ...}

        if servers == None: servers = self.scheduler.servers

        # Ο ελάχιστος χρόνος ολοκλήρωσης της 1ης εργασίας.
        if self.alpha is None:
            self.alpha = min(moldedJobDict.values())
        
        w = 0
        i = 1
        while True:
            eligibleCores = self.f(moldedJobDict) # Όλα τα s, όπου p(s, j) <= self.alpha

            if eligibleCores:
                # Βρίσκουμε το σετ (server, s), για το οποίο ελεχιστοποιείται το workload(χρόνος ολοκλήρωσης * s πυρήνες).
                minProcessingTime, minCores = self.minCost(moldedJobDict, eligibleCores) # Κρατάμε τον αριθμό των ελάχιστοων πυρήνων και του server.
                minProcessingTime = moldedJobDict[minCores] # Κρατάμε το χρόνο ολοκλήρωσης που ελαχιστοποιείται το s * p.
                w += minCores * minProcessingTime # Αυξάνουμε το συνολικό χρόνο ολοκλήρωσης για τη συγκεκριμένη χρονική φάση.
            else:
                self.new_server(job)
                newServer = self.scheduler.servers[-1]
                self.coreCapacity.update({newServer: newServer.capacity})
                return

            # Ενημερώνουμε τη νέα διάρκεια και πυρήνες.
            job.dur = minProcessingTime
            job.req = minCores

            # Αν ο συνολικός χρόνος ολοκλήρωσης της φάσης είναι <= του α * (αριθμό των πυρήνων) και βρέθηκαν eligible servers,
            # τότε γίνεται ο προγραμματισμός της εργασίας. 
            if w <= self.alpha * self.scheduler.cores and eligibleCores:

                self.pack_to_shelf(job)

                # # Αν η εργασία είναι μεγάλη, τότε δημιουργούμε ένα νέο ράφι ύψους ίση με το χρόνο ολοκλήρωσης της εργασίας για minCores πυρήνες
                # # και το τοποθετούμε πάνω απ' τον τρέχον scheduler.
                # shelfStart = self.height + 0.01
                # shelfEnd = shelfStart + minProcessingTime

                # # Αν η εργασία είναι μικρή, βρίσκουμε ακέραιο k, τέτοιο ώστε να ανήκει στο διάστημα (r^(k-1), r^k].
                # # Αυτό θα είναι το μέγεθος του ραφιού που θα τοποθετηθεί η εργασία.
                # if minCores <= self.scheduler.cores / 2:
                #     k = 0
                #     while True:
                        
                #         lowerBound = pow(self.r, k)
                #         upperBound = pow(self.r, k + 1)

                #         # Αν υπάρχει εργασία που έχει χρόνο ολοκλήρωσης 1, τότε τοποθετείται στο ράφι με ύψος 1.44.
                #         if lowerBound < minProcessingTime <= upperBound or minProcessingTime == 1.0: break
                #         k += 1
                #     shelfStart = lowerBound + 0.01
                #     shelfEnd = shelfStart + minProcessingTime

                # job.ar = shelfStart
                # job.fin = shelfEnd
                
                # scheduledServer = self.schedulingAlgorithm.pack(job, servers)
                # if servers: self.updateCoreCapacity(scheduledServer, minCores)
                # self.height += minProcessingTime
                # self.moldedJobhistory["after"].append([minProcessingTime, minCores])

                return
            
            self.alpha *= self.beta
            i += 1
            w = 0


class FirstFit:
    def __init__(self, scheduler) -> None:
        self.scheduler = scheduler
    
    def new_server(self, job):
        self.scheduler.add_server(job)

    def pack(self, job, servers = None):
        if servers is None: servers = self.scheduler.servers

        # if there are no servers open a new one
        if not servers:
            self.new_server(job)
            newServer = self.scheduler.servers[-1]
            return newServer
    
        # try to find the first server that can process the job
        for server in servers:
            # Ελέγχουμε αν η εργασία χωράει στο server.
            if server.check_fit(job):
                server.add_job(job)
                return server

        
        self.new_server(job)
        newServer = self.scheduler.servers[-1]
        return newServer

class Shelf:
    def __init__(self, height, max_width):
        self.height = height
        self.remaining_width = max_width
        self.jobs = []

    def shelfFit(self, width):
        return self.remaining_width >= width

    def add_job(self, job):
        self.jobs.append(job)
        self.remaining_width -= job.req
