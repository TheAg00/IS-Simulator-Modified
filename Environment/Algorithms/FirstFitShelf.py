import math

class FirstFitShelf:
    def __init__(self, scheduler) -> None:
        self.scheduler = scheduler
    
    def new_server(self, shelf):
        self.scheduler.add_server_shelves(shelf)

    def pack(self, shelf, servers = None):
        if servers is None: servers = self.scheduler.servers

        # Αν δεν υπάρχουν διαθέσημοι servers, ανοίγουμε έναν καινούριο.
        if not servers:
            self.new_server(shelf)
            return

        # Ψάχνουμε τον 1ο διαθέσημο server που μπορεί να προγραμματιστεί το ράφι.
        for server in servers:
            # Ελέγχουμε αν το shelf χωράει στο server.
            if server.check_fit(shelf):
                server.add_shelf(shelf)
                return 
    
        # Αν δε βρει, ανοίγουμε νέο server
        self.new_server(shelf)
        return

