import matplotlib.pyplot as plt
import pandas as pd
import os

class Graphs:
    def __init__(self, filePath, algs, cores, variance, wl, wlNames):
        self.filePath = filePath
        self.algs = algs
        self.cores = cores
        self.variance = variance
        self.wl = wl
        self.wlNames = wlNames

    def createGraph(self, normalisedBusyTimeList):   
        for row in range(len(normalisedBusyTimeList)):

            plt.figure(figsize=(10, 6))
            plt.bar(self.algs, normalisedBusyTimeList[row], color=['red', 'green', 'blue', 'orange', 'purple', 'cyan'])
            
            plt.title(f'Workload: {self.wlNames[row]}. Cores: {self.cores}. Variance: {self.variance}')
            plt.xlabel("Algorithms")
            plt.ylabel("Normalized Busy Time")

            plt.ylim(0, 1.1)
            plt.xticks(rotation=30, ha='right')
            plt.tight_layout()

            savePath = f'./Charts/Graphs/barplot_wl_{self.wl[row]}_cores_{self.cores}_variance_{self.variance}.png'
            plt.savefig(savePath)



    def normaliseBusyTime(self):
        data = pd.read_csv(self.filePath)
        df = pd.DataFrame(data)
        res = df.values.tolist()

        normalisedBusyTimeList = []
        for row in res:
            maxBusyTime = max(row)
            if maxBusyTime != 0:
                normalisedList = [busyTime / maxBusyTime for busyTime in row]
            else:
                normalisedList = [0, 0, 0, 0, 0, 0]
            normalisedBusyTimeList.append(normalisedList)

        self.createGraph(normalisedBusyTimeList)


    def createCSV(self, data, filePath):
        df = pd.DataFrame(data)

        newHeader = df.iloc[0]
        df = df[1:]
        df.columns = newHeader

        if os.path.exists(filePath): os.remove(filePath)
        df.to_csv(filePath, mode = 'a', index = False, header = True)
        self.normaliseBusyTime()