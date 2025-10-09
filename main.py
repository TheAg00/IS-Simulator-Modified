from Environment.Scheduler import Scheduler
import functions as f
from Charts.Graphs import Graphs


def main(config):
    # initialise a scheduler (effectively our simulation environment)
    sch = Scheduler(config['wl'], config['cores'], config['alg'], config['variance'])

    # returns a list of all the jobs in the workload([arrival, finish time, duration, requirements])
    jobs = f.parse_workload(config['wl'], cluster=config['cluster'], cores=config['cores'])

    # schedules the workload
    busy_time = sch.run(jobs)

    return busy_time

if __name__ == "__main__":
    cores, cluster, variacne = 32, False, 'HIGH'
    print(f'RUNNING FOR {cores} CORES! CLUSTER IS SET TO {cluster}. VARIANCE IS {variacne}')

    workloadToParse = [2, 4, 8, 9, 15, 16, 17, 18, 19, 20, 21, 22]
    workloadNames = ['SCSC Par96', 'LLNL Atlas', 'Sandia Ros', 'SDSC DataStar', 'LCG', 'MetaCentrum', 'CERIT-SC', 'Eucalyptus', 'KIT FH2 2016', 'UniLu Gaia 2014', 'PIK IPLEX 2009', 'RICC 2010']
    algorithmsToParse = ['FF', 'MET_AD', 'BF', 'HFF', 'BFMAT', 'Improved_MS']

    allWorkloadData = list()
    allWorkloadData.append(algorithmsToParse)
    for wl in workloadToParse:
        workloadData = []
        print(f'WL:{wl:<5}', end='', flush=True)
        for alg in algorithmsToParse:
            config = {
                "wl": wl,
                "cores": cores,
                "alg": alg,
                "cluster": cluster,
                "variance": variacne
            }
            result = main(config)
            print(f'{alg}:{result:<10}', end=' ', flush=True)
            workloadData.append(result)
            
        allWorkloadData.append(workloadData)
        
        print()
    

    # Ο παρακάτω κώδικας χρησιμοποιείθηκε για τη δημιουργία των γράφων και csv αρχείο με τα busy times που παράγει ο κάθε αλγόριθμος.
    # Τα αποτελέσματα βρίσκονται στο φάκελο Charts και δε χειάζεται να τρέχει κάθε φορά γι' αυτό είναι σε σχόλια.
    
    # filePathCSV = './Charts/Data/data_cores_' + str(cores) + '.csv'
    # chart = Graphs(filePathCSV, algorithmsToParse, cores, variacne, workloadToParse, workloadNames)
    # chart.createCSV(allWorkloadData, filePathCSV)