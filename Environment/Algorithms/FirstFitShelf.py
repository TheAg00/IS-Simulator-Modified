import math

class FirstFitShelf:
    def __init__(self, scheduler) -> None:
        self.scheduler = scheduler
    
    def new_server(self, shelf):
        self.scheduler.add_server_shelves(shelf)

    def modifyShelf(self,shelf, earliestStartTime, earliestFinishTimeServer):
        # Εφόσον θα αλλάξουμε τα χαρατηριστηκά του shelf, αλλάζουμε και για τις εργασίες.
        for job in shelf.jobs:
            job.ar = (job.ar - shelf.ar) + earliestStartTime
            job.fin = job.ar + job.dur
        
        # Αλλάζουμε τα arrival και finish time του shelf και το προσθέτουμε στο server.
        shelf.ar = earliestStartTime
        shelf.fin = earliestStartTime + shelf.height
        earliestFinishTimeServer.add_shelf(shelf)


    def pack(self, shelf, servers = None):
        if servers is None: servers = self.scheduler.servers

        # Αν δεν υπάρχουν διαθέσημοι servers, ανοίγουμε έναν καινούριο.
        if not servers:
            self.new_server(shelf)
            return
        
        # Ελέγχουμε αν μπορούμε να ανοίξουμε νέο server για να βάλουμε κάποιο ράφι, ή φτάσαμε στο όριο του περιορισμού των ανοιχτών servers. 
        canAddServer = True if (self.scheduler.serverLimit == None or len(self.scheduler.servers) <= self.scheduler.serverLimit) else False
        earliestFinishTime = servers[0].serverFinishTime
        earliestFinishTimeServer = servers[0]

        # Ψάχνουμε τον 1ο διαθέσημο server που μπορεί να προγραμματιστεί το ράφι.
        for server in servers:
            # Αν έχουμε φτάσει στο όριο των servers και το νέο ράφι δε χωράει πουθενά, θα του βάλουμε arrival_time ίσο με το ελάχιστο finish time απ' όλους τους server. 
            if not canAddServer:
                earliestFinishTime = min(earliestFinishTime, server.serverFinishTime)
                earliestFinishTimeServer = server


            # Ελέγχουμε αν το shelf χωράει στο server.
            if server.check_fit(shelf):
                server.add_shelf(shelf)
                return 
   
        # Προσθέτουμε νέο server, όταν δεν έχουμε φτάσει το καθορισμένο όριο από ανοιχτούς servers που μπορούμε να έχουμε.
        if canAddServer:
            self.new_server(shelf)
            return

        earliestStartTime = math.ceil(earliestFinishTime) if earliestFinishTime != math.ceil(earliestFinishTime) else (earliestFinishTime + 1)
        self.modifyShelf(shelf, earliestStartTime, earliestFinishTimeServer)
