import math

class Improved_MS_test:
    def __init__(self, jobs):
        self.jobs = jobs
        self.alpha = min(jobs[0].values())

        self.beta = 1.56
        self.r = 1.44
        self.time = 0
        self.i = 1
        self.w = 0
        self.shelves = []
        self.height = 0

    def f(self, job):
        eligibleServers = list()
        for s in range(1, 6):
            if  job[s] <= self.alpha:
                eligibleServers.append(s)
        
        return eligibleServers
    

    def minCost(self, job, eligibleServers):
        minCores = eligibleServers[0]
        minProcessingTime = job[minCores]
        minProduct = minCores * minProcessingTime
        for s in eligibleServers[1:]:
            p = job[s]
            currentProduct = s * p
            if currentProduct < minProduct:
                minProduct = currentProduct
                minCores = s
                minProcessingTime = p
        
        return minProcessingTime, minCores


    def packShelf(self, job, p, s):
            # Για μεγάλες εργασίες, δημιουργούμε ένα νέο ράφι πάνω απ' το τρέχον, με ύψος ίσο με το χρόνο ολοκλήρωσης της εργασίας.
            if s > 2:
                shelf = Shelf(p, 5)
                shelf.add_job(job, s)
                self.shelves.append(shelf)
                self.height += p
                return


            # Για μικρές εργασίες, βρίσκουμε έναν ακέραιο k, τέτοιο ώστε να ισχύει r ^ k < χρόνος ολοκλήρωσης εργασίας <= r ^ (k + 1).
            # Η εργασία θα τοποθετηθεί σε ράφι ύψους r ^ (k + 1) χρησιμοποιώντας First Fit.
            k = 0
            while True:
                
                lowerBound = pow(self.r, k)
                upperBound = pow(self.r, k + 1)

                # Αν υπάρχει εργασία που έχει χρόνο ολοκλήρωσης 1, τότε τοποθετείται στο ράφι με ύψος 1.44.
                if lowerBound < p <= upperBound or p == 1.0: break
                k += 1

            # Εφαρμογή της First Fit.
            for shelf in self.shelves:
                if math.isclose(shelf.height, upperBound) and shelf.shelfFit(s):
                    shelf.add_job(job, s)
                    return
                    
            # Αν δεν μπορεί να τοποθετηθεί σε ήδη υπάρχον ράφι, δημιουργούμε ένα καινούριο πάνω απ' το τρέχον.
            new_shelf = Shelf(upperBound, 5)
            new_shelf.add_job(job, s)
            self.shelves.append(new_shelf)
            self.height += upperBound


    def pack(self, job):
        while True:
            eligibleServers = self.f(job)
            if eligibleServers:
                p, s = self.minCost(job, eligibleServers)
                self.w += p * s

            if self.w <= self.alpha * 5 and eligibleServers:
                self.packShelf(job, p, s)
                return
            
            self.alpha *= self.beta
            self.w = 0
        

class Shelf:
    def __init__(self, height, maxWidth):
        self.height = height
        self.remainingWidth = maxWidth
        self.jobs = []

    # Ελέγχει αν μια εργασία χωράει στο ράφι.
    def shelfFit(self, width):
        return self.remainingWidth >= width

    # Προσθέτουμε την εργασία στο ράφι και ενημερώνουμε τον απομείνωντα χώρο.
    def add_job(self, job, s):
        self.jobs.append(job)
        self.remainingWidth -= s



with open("./molded_jobs.txt") as f:
    jobs = list()
    for line in f:
        job = {}
        job_info = line.rstrip().split(' ')
        for index, x in enumerate(job_info):
            job.update({(index + 1) : float(x)})
        
        jobs.append(job)


schedule = Improved_MS_test(jobs)

for j in jobs:
    schedule.pack(j)

for shelf in schedule.shelves:
    print("height for shelf", shelf, "->", shelf.height)
print("total height ->", schedule.height)