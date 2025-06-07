class Node:
    '''
        A point in which the load placed on the system changes (increases or deceases).
        Functions as a node in a Linked List representing computation intervals
    '''
    def __init__(self, time) -> None:
        self.time = time
        self.requirements = 0
        self.next = None
    
    def __str__(self) -> str:
        return f"[T:{self.time} R:{self.requirements}]"

class Points:
    '''
        Implements a modified linked list whose points represent intervals of processing requirements
        These intervals are in the form of [start_time, end_time) during which some load is placed on the system.
        The load is defined by the requirements of the interval. When intervals overlap, their requirements are combined (additively).
    '''
    def __init__(self) -> None:
        self.head = None

    def traverse(self, delim=" "):
        '''
            Traverses the linked list printing all nodes separated by "delim" (default: " ")
        '''
        if self.head is None:
            return
        
        print(self.head, end=delim) 
        current_node = self.head
        while current_node.next is not None:
            current_node = current_node.next
            print(current_node, end=delim)
        print()
    
    def _debug_only_length(self):
        '''
            Traverses the list counting the nodes - !THIS IS INEFFICIENT! 
            If you need to know the number of intervals its recommended to implement a way to
            dynamically track the size as it changes
        '''
        if self.head is None:
            return 0
        node, len = self.head, 1
        while node.next is not None:
            node = node.next
            len += 1
        return len
    
    def _insert_interval_start(self, time):
        '''
            INTERNAL_METHOD
            Traverses the linked list looking for a suitable location to insert a starting point for an interval
             a: If another points that shares the same timestamp is found we do nothing 
             b: When a point is inserted its requirements are set to that of the previous point
                (NOTE for (a) and (b): all requirement adjustments are made by _insert_interval_end)
        '''
        new_node = Node(time)

        if self.head is None:
            self.head = new_node
            return
        
        if time < self.head.time:
            new_node.next = self.head
            self.head = new_node
            return

        current_node = self.head

        # prevents adding a duplicate point whose value is equal to the head (future conditions start after head)
        if current_node.time == time:
            return
        
        while current_node.next is not None:
            if current_node.next.time == time:
                return
            elif current_node.next.time > time:
                break
            current_node = current_node.next
        
        new_node.next = current_node.next
        new_node.requirements = current_node.requirements
        current_node.next = new_node
    
    def _insert_interval_end(self, time, start_time, requirements):
        '''
            INTERNAL_METHOD
            Traverses the linked list looking for a suitable location to insert an ending point for an interval
            Utilises the interval_start time to increment all overlaying points in [start_time, time) by requirements
            In a node with the same time is found this method adds no new node (intermediate point requirements are always incremented)
        '''
        
        # Εμφανίζει error με κατάλληλο μήνυμα και σταματάει στις περιπτώσεις που η λίστα είναι άδεια
        # και και αν ο τελικός χρόνος είναι μικρότερος ή ίσος με τον αρχικό.
        assert self.head is not None, "Attempted to add 'end interval' to an empty list..."
        assert time > self.head.time, "Attempted to add 'end interval' at the head of the list..."

        current_node = self.head

        while current_node is not None:
            # if the end point already exists no need to do anything
            if current_node.time == time:
                return

            # update requirements if points within the interval [start_time, time)
            if current_node.time >= start_time:
                current_node.requirements += requirements

            # if we reached the right spot to insert the point
            if current_node.next is None or current_node.next.time > time:
                new_node = Node(time)
                # subtract the requirements from current load (previous point) since the task is now finished
                new_node.requirements = current_node.requirements - requirements  
                new_node.next = current_node.next
                current_node.next = new_node
                return

            current_node = current_node.next
    
    def insert_interval(self, start, end, requirements):
        '''
            Adds an interval using _insert_interval_start and _insert_interval_end
        '''
        self._insert_interval_start(start)
        self._insert_interval_end(end, start, requirements)

    def move_to_time(self, time):
        '''
            Moves the head of the list to point at 'time' and measures the busy_time. 
            The buys time is defined as the span of intervals with non-zero requirements
            NOTE: If no point exists with timestamp 'time' time, the starting point of the interval that contains 
            'time' is updated to 'time' 
        '''
        if self.head is None:
            return 0
        
        busy_time = 0
        current_node = self.head
        while current_node.next is not None:
            if current_node.time == time:
                self.head = current_node
                return busy_time
            elif current_node.next.time > time:
                self.head = current_node
                if current_node.requirements != 0:
                    busy_time += time - current_node.time
                self.head.time = time
                return busy_time
        
            if current_node.requirements != 0:
                busy_time += current_node.next.time - current_node.time
            
            current_node = current_node.next
        
        # if execution gets here we have moved past all points - list is now empty
        # Αν ο επόμενος κόμβος απ' αυτόν που βρισκόμαστε είναι κενός, σημαίνει πως προσπελάσαμε όλους τους κόμβους.
        self.head = None
        return busy_time
    
    def measure_remaining_busy_time(self):
        '''
            Similar logic with move_to_time although it moves all the way to the end and DOES NOT update the head.
            Effectively measures the remaining busy_time until the server closes
        '''
        if self.head is None:
            return 0
        
        busy_time = 0
        current_node = self.head

        while current_node.next is not None:
            if current_node.requirements != 0:
                busy_time += current_node.next.time - current_node.time
            current_node = current_node.next

        return busy_time
            
    def check_fit(self, start, end, requirements, capacity):
        '''
            Checks if an interval can be added to the list.
            An interval can be added iff all overlaying points do not exceed
            capacity when the intervals requirements are added to them.
        '''
        current_node = self.head
        # for all points before 'end'
        while current_node and current_node.time < end:
            # if the points is after 'start' (i.e intermediate points): check if adding interval will cause any points to exceed capacity
            if current_node.time >= start and current_node.requirements + requirements > capacity:
                return False
            current_node = current_node.next
        return True
    
    def check_fit_and_measure_leftover(self, start, end, requirements, capacity):
        '''
            Checks if an interval can be added to the list. 
            Additionally, if the interval fits, returns a list of the leftover space for each overlaying interval.
            If it does not fit, returns an empty list
        '''
        leftovers = []
        current_node = self.head
        # for all points before 'end'
        while current_node and current_node.time < end:
            # if the points is after 'start' (i.e intermediate points): check if adding interval will cause any points to exceed capacity
            if current_node.time >= start:
                if current_node.requirements + requirements > capacity:
                    return False, []
                else:
                    leftovers.append(capacity - (current_node.requirements + requirements))
            
            current_node = current_node.next
        return True, leftovers

    def measure_span(self, jobs):
        '''
            Adds all jobs to a server with infinite capacity
            Effectively measures the overlap of the jobs to determine density
        '''
        span = 0
        for j in jobs:
            span += self.move_to_time(j.ar)
            self.insert_interval(j.ar, j.fin, j.dur)
    
        return span + self.measure_remaining_busy_time()