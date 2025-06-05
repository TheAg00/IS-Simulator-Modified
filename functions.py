import glob
import numpy as np

from my_parser import parse
from Environment.Intervals import Points


def get_threshold(alg, cores):
    '''
        Parses the threshold values from the thresholds directory
    '''
    if alg == 'MET_AD':
        f = open(f"thresholds/actual_{cores}.txt", "rt")
    elif alg == 'MET_RD':
        f = open(f"thresholds/REM1_{cores}.txt", "rt")
    else:
        raise ValueError(f"No threshold for algorithm: {alg}")

    thresholds = []

    for line in f:
        thresholds.append(float(line.rstrip().split("\t")[1]))
    f.close()

    return thresholds

def split_time(batch_dur, jobs):
    '''
        Splits jobs into batches of jobs with length 'batch_dur'
    '''
    batches, batch = [], []
    end_of_batch = jobs[0].ar + batch_dur

    for job in jobs:
        if job.ar > end_of_batch:
            # ensure end_of_batch is moved ahead of job.ar
            # if this is skipped (when job.ar > end_of_batch+batch_dur) we will create single
            # job batches until end_of_batch 'catches up' to the job arrivals
            while job.ar > end_of_batch:
                end_of_batch += batch_dur

            batches.append(batch)
            batch = []     

        batch.append(job)
    
    batches.append(batch)
    
    return batches

def get_sparsity(batch):
    '''
        Calculates the sparsity of a given batch of jobs
        Sparsity: span / sum_of_durations
    '''
    if len(batch) == 1: return 1
    
    sum_of_durations = sum([j.dur for j in batch])

    points = Points()
    interval_span = points.measure_span(batch)

    return interval_span / sum_of_durations

def gini_coefficient(batch):
    '''
        Calculates the Gini coefficient using definition:
            Gini = half of the relative mean absolute difference 

        https://en.wikipedia.org/wiki/Gini_coefficient
    '''
    if len(batch) < 1:
        return 0.0

    if not isinstance(batch[0], int):
        data = np.array([x.dur for x in batch])
    else:
        data = np.array(batch)
    
    sum_of_absolute_differences = 0
    for point in data:
        sum_of_absolute_differences += np.sum(np.absolute(np.array(point)-data))

    gini = sum_of_absolute_differences / (2 * len(data) * sum(data))
    return gini


def parse_workload(set, cluster, cores):
    '''
        Parses the workload 'set' from in the data directory.
        cluster:
            True    ->     splits jobs with requirements N into N jobs with requirements 1
            False   ->     does not do that :p
    '''
    data = glob.glob("data/*")
    # Βάζει 5 και 7, γιατί το όνομα θα μοιάζει με κάτι σαν "data/00_NASA..."
    data = sorted(data, key=lambda x: int(x[5:7]))
    
    return parse(data[set], cores=cores, cluster=cluster)