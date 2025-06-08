from Environment.Moldable_task_model import *
from Environment.Algorithms.FirstFitShelf import FirstFitShelf


import random
import math

class Improved_MS:
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.varianceModel = "low" if self.scheduler.alg == 'Improved_MS_Varaince_LOW' else "high"

        self.alpha = None
        self.beta = 1.56
        self.r = 1.44

        self.firstFitShelf = FirstFitShelf(scheduler)

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
    def f(self, moldedJobDict):
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

    # Αν το ράφι έχει γαμήσει, τότε το προγραμματίζουμε στον 1ο διαθέσημο server.
    def packShelf(self, shelf):
        self.firstFitShelf.pack(shelf)

    # Τοποθετούμε την τρέχουσα εργασία στο κατάλληλο 'ράφι'.
    def createShelf(self, job):
        s, p = job.req, job.dur
        maxCores = self.scheduler.cores

        # Για μεγάλες εργασίες, δημιουργούμε ένα νέο ράφι πάνω απ' το τρέχον, με ύψος ίσο με το χρόνο ολοκλήρωσης της εργασίας.
        if s > maxCores // 2:
            shelf = Shelf(self.scheduler, p, maxCores)
            shelf.add_job(job)
            self.packShelf(shelf)

            return

        # Για μικρές εργασίες, βρίσκουμε έναν ακέραιο k, τέτοιο ώστε να ισχύει r ^ k < χρόνος ολοκλήρωσης εργασίας <= r ^ (k + 1).
        k = 0
        while True:
            
            lowerBound = pow(self.r, k)
            upperBound = pow(self.r, k + 1)

            # Αν υπάρχει εργασία που έχει χρόνο ολοκλήρωσης 1, τότε τοποθετείται στο ράφι με ύψος 1.44(αποφεύγουμε ατέρμων βρόχο).
            if lowerBound < p <= upperBound or p == 1.0: break
            k += 1

        # Ελέγχουμε για ανοιχτά servers ώστε να τοποθετήσουμε την εργασία σε υπάρχον ράφι.
        for server in self.scheduler.servers:
            for _, shelf in server.shelves:
                if math.isclose(shelf.height, upperBound) and shelf.shelfFit(s):
                    shelf.add_job(job)
                    return
    
        # Αν δεν μπορεί να τοποθετηθεί σε ήδη υπάρχον ράφι, δημιουργούμε ένα καινούριο πάνω απ' το τρέχον.
        new_shelf = Shelf(self.scheduler, upperBound, maxCores)
        new_shelf.add_job(job)
        self.packShelf(new_shelf)
        

    def pack(self, job, servers = None, openNew = True):
        if servers == None: servers = self.scheduler.servers

        moldedJobDict = self.convertMold(job) # {...,s_n: p(s_n, j), ...}
        
        # Ο ελάχιστος χρόνος ολοκλήρωσης της 1ης εργασίας.
        if self.alpha is None: self.alpha = min(moldedJobDict.values())
        
        w = 0
        while True:
            eligibleCores = self.f(moldedJobDict) # Όλοι οι πυρήνες s, όπου p(s, j) <= self.alpha

            if eligibleCores:
                job.dur, job.req = self.minCost(moldedJobDict, eligibleCores)
                w += job.dur * job.req # Αυξάνουμε το συνολικό χρόνο ολοκλήρωσης για τη συγκεκριμένη χρονική φάση.

            # Αν ο συνολικός χρόνος ολοκλήρωσης της φάσης είναι <= του α * (αριθμό των πυρήνων) και βρέθηκαν eligible servers,
            # τότε γίνεται ο προγραμματισμός της εργασίας στο κατάλληλο 'ράφι'. 
            if w <= self.alpha * self.scheduler.cores and eligibleCores:
                self.createShelf(job)
                return
            
            self.alpha *= self.beta
            w = 0

class Shelf:
    def __init__(self, scheduler, height, maxWidth):
        self.scheduler = scheduler
        self.height = height
        self.remainingWidth = maxWidth
        self.jobs = []

        self.ar = 0
        self.fin = math.ceil(height)
        self.req = 0

    # Αποφεύγουμε το σφάλμα -> TypeError: '<' not supported between instances of 'Shelf' and 'Shelf'
    # όταν συγκρίνουμε δύο shelf.fin που έχουν την ίδια τιμή.
    def __lt__(self, other):
        return self.fin < other.fin

    # Ελέγχει αν μια εργασία χωράει στο ράφι.
    def shelfFit(self, width):
        return self.remainingWidth >= width

    # Προσθέτουμε την εργασία στο ράφι.
    def add_job(self, job):
        self.jobs.append(job)
        self.remainingWidth -= job.req
        self.req += job.req # Αριθμός πυρήνων που θα χρησιμοποιειθούν για τον προγραμματισμό του ραφιού.

