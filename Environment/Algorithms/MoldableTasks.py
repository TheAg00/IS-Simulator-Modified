class MoldableTasks:
    def __init__(self, scheduler):
        self.scheduler = scheduler
    

    def convertToMoldable(self, job):
        jobDuration = job.duration # Χρόνος μέχρι να ολοκληρωθεί η εργασία
        jobRequirements = job.req # απαιτήσεις σε πυρήνες που θέλει η εργασία για το συγκεκριμένο duration.

        


    def pack(self, job):
        pass