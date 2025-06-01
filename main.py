from Environment.Scheduler import Scheduler
import functions as f

def main(config):
    # initialise a scheduler (effectively our simulation environment)
    sch = Scheduler(config['wl'], config['cores'], config['alg'], config['shelfLimit'])

    # returns a list of all the jobs in the workload([arrival, finish time, duration, requirements])
    jobs = f.parse_workload(config['wl'], cluster=config['cluster'], cores=config['cores'])

    # schedules the workload
    busy_time = sch.run(jobs)

    return busy_time, sch.totalDelay

if __name__ == "__main__":
    cores, cluster, shelfLimit = 32, False, 1
    print(f'RUNNING FOR {cores} CORES! CLUSTER IS SET TO {cluster}. SHELF LIMIT IS SET TO {shelfLimit}')

    for wl in range(0, 16):
        print(f'WL:{wl:<5}', end='', flush=True)
        # for alg in ['FF', 'MET_AD', 'BF', 'HFF', 'BFMAT', 'Improved_MS_Varaince_LOW', 'Improved_MS_Varaince_HIGH']:
        for alg in ['Improved_MS_Varaince_LOW', 'Improved_MS_Varaince_HIGH']:
            config = {
                "wl": wl,
                "cores": cores,
                "alg": alg,
                "cluster": cluster,
                "shelfLimit": shelfLimit
            }
            result, totalDelay = main(config)
            print(f'{alg}:{result:<10}', end=' ', flush=True)
            print(f'total delay: {totalDelay:<10}', end = ' ')
        print()

            