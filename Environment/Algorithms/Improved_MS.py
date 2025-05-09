from Environment.Moldable_task_model import *
from Environment.Algorithms.FirstFit import FirstFit

import random
import math

class Improved_MS:
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.varianceModel = "low"
        self.moldedJobhistory = {"before": [], "after": []}
        self.blackbox = FirstFit(self.scheduler)
        self.coreCapacity = {server: server.cores for server in self.scheduler.servers}

        self.alpha = 0
        self.beta = 1.56
        self.r = 1.44
        self.makespan = 0
        self.firstJob = True

    # Δημιουργούμε νέο server.
    def new_server(self, job):
        self.scheduler.add_server(job)

    # Για κάθε server, ελέγχουμε για έναν διαφορετικό αριθμό πυρήνων s το χρόνο ολοκλήρωσης της εργασίας job.
    # Αν ο χρόνος ολοκλήρωσης είναι <= a[i], τότε προσθέτουμε ένα σετ (server, s) στη λίστα με τα servers που πληρούν τις προϋποθέσεις.
    def f(self, job):
        eligibleServers = list()
        for server in self.scheduler.servers:
            for s in range(1, self.coreCapacity[server] + 1):
                if  job[s] <= self.alpha:
                    eligibleServers.append([server, s])
        
        return eligibleServers

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

    def updateJob(self, job, moldedJob):
        job.ar = moldedJob[0]
        job.fin = moldedJob[1]
        job.dur = moldedJob[2]
        job.req = moldedJob[3]

        return job

    def pack(self, job, servers = None, openNew = True):
        # moldedJobDict = self.convert_to_mold(job)

        if servers == None: servers = self.scheduler.servers

        self.moldedJobhistory["before"].append([job.dur, job.req])

        if self.firstJob:
            self.alpha = min(moldedJobDict.values()) # Ο ελάχιστος χρόνος ολοκλήρωσης της 1ης εργασίας.
            self.makespan = self.alpha
            self.firstJob = False

        w = 0
        i = 1
        while True:
            eligibleServers = self.f(moldedJobDict)

            if eligibleServers:
                # Βρίσκουμε το σετ (server, s), για το οποίο ελεχιστοποιείται το workload(χρόνος ολοκλήρωσης * s πυρήνες).
                minPair = eligibleServers[0]
                minProduct = minPair[1] * moldedJobDict[1]

                for row in eligibleServers[1:]:
                    s = row[1]
                    p = moldedJobDict[s]
                    currentProduct = s * p
                    if currentProduct < minProduct:
                        minPair = row
                        minProduct = currentProduct
                
                minServer, minCores = minPair[0], minPair[1] # Κρατάμε τον αριθμό των ελάχιστοων πυρήνων και του server.
                minProcessingTime = moldedJobDict[minCores] # Κρατάμε το χρόνο ολοκλήρωσης που ελαχιστοποιείται το s * p.
                w += minCores * minProcessingTime # Αυξάνουμε το συνολικό χρόνο ολοκλήρωσης για τη συγκεκριμένη χρονική φάση.
            else:
                self.new_server(job)
                newServer = self.scheduler.servers[-1]
                self.coreCapacity.update({newServer: newServer.capacity})
                


            # Αν ο συνολικός χρόνος ολοκλήρωσης της φάσης είναι <= του α * (αριθμό των πυρήνων) και βρέθηκαν eligible servers,
            # τότε γίνεται ο προγραμματισμός της εργασίας. 
            if w <= self.alpha * self.scheduler.cores and eligibleServers:
                # Αν η εργασία είναι μεγάλη, τότε δημιουργούμε ένα νέο ράφι ύψους ίση με το χρόνο ολοκλήρωσης της εργασίας για minCores πυρήνες
                # και το τοποθετούμε πάνω απ' τον τρέχον scheduler.
                moldedJob = [self.makespan + 1, self.makespan + minProcessingTime + 1, minProcessingTime, minCores]

                # Αν η εργασία είναι μικρή, βρίσκουμε ακέραιο k, τέτοιο ώστε να ανήκει στο διάστημα (r^(k-1), r^k].
                # Αυτό θα είναι το μέγεθος του ραφιού που θα τοποθετηθεί η εργασία.
                if minCores <= self.scheduler.cores / 2:
                    k = 0
                    while True:
                        
                        lowerBound = pow(self.r, k)
                        upperBound = pow(self.r, k + 1)

                        # Αν υπάρχει εργασία που έχει χρόνο ολοκλήρωσης 1, τότε τοποθετείται στο ράφι με ύψος 1.44.
                        if lowerBound < minProcessingTime <= upperBound or minProcessingTime == 1.0: break
                        k += 1

                    moldedJob = [lowerBound + 1, lowerBound + minProcessingTime + 1, minProcessingTime, minCores]

                job = self.updateJob(job, moldedJob)
                
                self.blackbox.pack(job, [minServer])
                if servers: self.updateCoreCapacity(minServer, minCores)
                self.makespan += minProcessingTime
                self.moldedJobhistory["after"].append([minProcessingTime, minCores])

                break

            self.alpha *= self.beta
            i += 1
            w = 0


        
            
