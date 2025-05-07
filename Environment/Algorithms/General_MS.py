from Environment.Algorithms.FirstFit import FirstFit

class General_MS:
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.blackbox = FirstFit(self.scheduler)
        self.coreCapacity = {server: self.scheduler.cores for server in self.scheduler.servers}

    # Δημιουργούμε νέο server.
    def new_server(self, job):
        self.scheduler.add_server(job)

    # Υπολογίζουμε το χρόνο ολοκλήρωσης της εργασίας για συγκεκριμένο αριθμό cores(newCores).
    def calculateDur(self, job, newCores):
        currentCores = job.req
        currentDur = job.dur
        newDur = (currentCores / newCores) * currentDur
        
        return newDur


    # Για κάθε server, ελέγχουμε για έναν διαφορετικό αριθμό πυρήνων s το χρόνο ολοκλήρωσης της εργασίας job.
    # Αν ο χρόνος ολοκλήρωσης είναι <= a[i], τότε προσθέτουμε ένα σετ (server, s) στη λίστα με τα servers που πληρούν τις προϋποθέσεις.
    def f(self, job, alpha):
        eligibleServers = list()
        for server in self.scheduler.servers:
            for s in range(1, server.capacity + 1):
                if self.calculateDur(job, s) <= alpha:
                    eligibleServers.append([server, s])
        
        return eligibleServers

    # Εφόσον το task γίνει moldable, ενημερώνουμε το νέο χρόνο ολοκλήρωσης της εργασίας.
    # Ο χρόνος ολοκλήρωσης μπορεί να άλλαξε, μιας και ο αριθμός των κατανεμημένων πυρήνων μπορεί να είναι διαφορετικός απ' την αρχική απαιτήσεις.
    def updateDurationMoldable(self, job, newCores):
        job.dur = self.calculateDur(job, newCores)
        job.req = newCores
        
        return job

    # Ενημερώνουμε το νέο αριθμό διαθέσημων πυρήνων μετά τον προγραμματισμό της εργασίας.
    def updateCoreCapacity(self, server, usedCores):
        self.coreCapacity[server] -= usedCores

    def pack(self, job):
        a = [job.dur]
        w = [0]

        i = 0
        while True:
            # Αποθηκεύουμε σετς (server, s), όπου  ο χρόνος ολοκλήρωσης της εργασίας είναι <= a[i].
            eligibleServers = self.f(job, a[i]) 
            if eligibleServers:
                # Βρίσκουμε το σετ (server, s), για το οποίο ελεχιστοποιείται το workload(χρόνος ολοκλήρωσης * s πυρήνες).
                minPair = eligibleServers[0]
                minProduct = minPair[1] * self.calculateDur(job, minPair[1])

                for row in eligibleServers[1:]:
                    currentProduct = row[1] * self.calculateDur(job, row[1])
                    if currentProduct < minProduct:
                        minPair = row
                        minProduct = currentProduct
                
                minServers, minS = minPair[0], minPair[1]
                w[i] += minS * self.calculateDur(job, minS) # Αυξάνουμε το συνολικό έργο.
            else:
                self.new_server(job)

            # Ελέγχουμε αν το συνολικό workload είναι μικρότερο του γινομένου του a[1] επί του συνολικού αριθμού servers.
            # Αυτό το κάνουμε για να γίνεται καλύτερη ομαδοποίηση στον προγραμματισμό εργασιών, ώστε να έχουν παρόμοιο χρόνο ολοκλήρωσης.
            # Επίσης, κοιτάμε η λίστα με τα eligible servers να μην είναι κενή.
            if w[i] <= a[i] * len(self.scheduler.servers) and eligibleServers:
                moldJob = self.updateDurationMoldable(job, minS)
                self.blackbox.pack(moldJob, [minServers])
                self.updateCoreCapacity(minServers, minS)
                break
            
            # Αν δεν μπορεί να γίνει ο προγραμματισμός της εργασίας, ενημερώνουμε τις παρακάτω τιμές και ξαναπροσπαθούμε.
            i += 1
            a[i] = a[i - 1] * 2
            w[i] = 0

