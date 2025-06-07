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
            # Επιτρέπουμε προσθήκη ραφιού σε ένα server μόνο αν τηρεί τον περιορισμό που έχουμε δώσει(αν shelfLimit == -1, τότε δεν υπάρχει περιορισμός.) 
            if len(server.shelves) >= server.shelfLimit and server.shelfLimit != -1: break

            # Ελέγχουμε αν η εργασία χωράει στο server.
            if server.check_fit(shelf):
                server.add_shelf(shelf)
                return 
   
        self.new_server(shelf)