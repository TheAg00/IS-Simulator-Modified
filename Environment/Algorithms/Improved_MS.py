from Environment.Moldable_task_model import *

import random
import math

class Improved_MS:
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.varianceModel = "low" if self.scheduler.alg == 'Improved_MS_Varaince_LOW' else "high"

        self.shelves = []
        self.alpha = None
        self.beta = 1.56
        self.r = 1.44
        self.height = 0

    # Μετατρέπουμε την εργασία σε moldable.
    def convertMold(self, job):
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

    # Ελέγχουμε για διαφορετικό αριθμό πυρήνων s το χρόνο ολοκλήρωσης της εργασίας job.
    # Αν ο χρόνος ολοκλήρωσης είναι <= α, τότε προσθέτουμε το s στη λίστα με τους πυρήνες που πληρούν τις προϋποθέσεις.
    def f(self, moldedJobDict, job):
        eligibleCores = list()
        for s in range(1, self.scheduler.cores + 1):
            if  moldedJobDict[s] <= self.alpha:
                eligibleCores.append(s)
        
        return eligibleCores
    
    # Βρίσκουμε τον αριμθό πυρήνων s και το χρόνο ολοκλήρωσης p, για τον οποίο ελαχιστοποιείται το s * p.
    def minCost(self, moldedJobDict, eligibleCores):
        minCores = eligibleCores[0]
        minProcessingTime = moldedJobDict[minCores]
        minProduct = minCores * minProcessingTime

        for s in eligibleCores[1:]:
            p = moldedJobDict[s]
            currentProduct = s * p
            if currentProduct < minProduct:
                minProduct = currentProduct
                minCores = s
                minProcessingTime = p

        return minProcessingTime, minCores

    # Τοποθετούμε την τρέχουσα εργασία στο κατάλληλο 'ράφι'.
    def packShelf(self, job):
        s = job.req
        p = job.dur

        # Για μεγάλες εργασίες, δημιουργούμε ένα νέο ράφι πάνω απ' το τρέχον, με ύψος ίσο με το χρόνο ολοκλήρωσης της εργασίας.
        if s > self.scheduler.cores // 2:
            shelf = Shelf(p, self.scheduler.cores)
            shelf.add_job(job)
            self.shelves.append(shelf)
            self.height += p # Αυξάνουμε το συνολικό ύψος όλων των ραφιών.
            return


        # Για μικρές εργασίες, βρίσκουμε έναν ακέραιο k, τέτοιο ώστε να ισχύει r ^ k < χρόνος ολοκλήρωσης εργασίας <= r ^ (k + 1).
        k = 0
        while True:
            
            lowerBound = pow(self.r, k)
            upperBound = pow(self.r, k + 1)

            # Αν υπάρχει εργασία που έχει χρόνο ολοκλήρωσης 1, τότε τοποθετείται στο ράφι με ύψος 1.44(αποφεύγουμε ατέρμων βρόχο).
            if lowerBound < p <= upperBound or p == 1.0: break
            k += 1

        # Η εργασία θα τοποθετηθεί στο 1ο διαθέσιμο ράφι ύψους r ^ (k + 1).
        for shelf in self.shelves:
            if math.isclose(shelf.height, upperBound) and shelf.shelfFit(s):
                shelf.add_job(job)
                return
                
        # Αν δεν μπορεί να τοποθετηθεί σε ήδη υπάρχον ράφι, δημιουργούμε ένα καινούριο πάνω απ' το τρέχον.
        new_shelf = Shelf(upperBound, self.scheduler.cores)
        new_shelf.add_job(job)
        self.shelves.append(new_shelf)
        self.height += upperBound


    def pack(self, job, servers = None, openNew = True):
        if servers == None: servers = self.scheduler.servers

        moldedJobDict = self.convertMold(job) # {...,s_n: p(s_n, j), ...}
        
        # Ο ελάχιστος χρόνος ολοκλήρωσης της 1ης εργασίας.
        if self.alpha is None: self.alpha = min(moldedJobDict.values())
        
        w = 0
        while True:
            eligibleCores = self.f(moldedJobDict, job) # Όλοι οι πυρήνες s, όπου p(s, j) <= self.alpha

            if eligibleCores:
                job.dur, job.req = self.minCost(moldedJobDict, eligibleCores)
                w += job.dur * job.req # Αυξάνουμε το συνολικό χρόνο ολοκλήρωσης για τη συγκεκριμένη χρονική φάση.

            # Αν ο συνολικός χρόνος ολοκλήρωσης της φάσης είναι <= του α * (αριθμό των πυρήνων) και βρέθηκαν eligible servers,
            # τότε γίνεται ο προγραμματισμός της εργασίας στο κατάλληλο 'ράφι'. 
            if w <= self.alpha * self.scheduler.cores and eligibleCores:
                self.packShelf(job)
                return
            
            self.alpha *= self.beta
            w = 0

class Shelf:
    def __init__(self, height, maxWidth):
        self.height = height
        self.remainingWidth = maxWidth
        self.jobs = []

        self.ar = 0
        self.fin = math.ceil(height)
        self.req = 0

    # Ελέγχει αν μια εργασία χωράει στο ράφι.
    def shelfFit(self, width):
        return self.remainingWidth >= width

    # Προσθέτουμε την εργασία στο ράφι.
    def add_job(self, job):
        self.jobs.append(job)
        self.remainingWidth -= job.req
        self.req += job.req # Αριθμός πυρήνων που θα χρησιμοποιειθούν για τον προγραμματισμό του ραφιού.