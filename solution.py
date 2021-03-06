# -*- coding: utf-8
# -*- author: Shangyu Liu

import copy, sys, os, random, subprocess
from scipy import optimize as opt
import matplotlib.pyplot as plt
import numpy as np


class FilePath:

    def __init__(self, PATH):
        self.PATH = PATH
        self.FILE_DC_SLOT = PATH + "datacenter_slot.dat"
        self.FILE_DC_DATA = PATH + "databases.dat"
        self.FILE_DC_LINK = PATH + "bandwidth.dat"
        self.FILE_TASK_EXEC_TIME = PATH + "task_exec_time.dat"
        self.FILE_TASK_PRECEDENCE = PATH + "task_precedence.dat"
        self.FILE_TASK_DATADEMAND = PATH + "task_dataDemand.dat"

class Task:

    def __init__(self, task_name, exec_time):
        self.task_name = task_name
        self.exec_time = exec_time

class Job:

    def __init__(self, job_name):
        self.job_name = job_name
        self.tasks = []
        self.parents = []
        self.children = []
        self.dataneed = []

    def addTask(self, task):
        self.tasks.append(task)
        self.parents.append([])
        self.children.append([])
        self.dataneed.append([])

    def taskID(self, task_name):
        for i, task in enumerate(self.tasks):
            if task.task_name == task_name:
                return i
        raise Exception("TaskNotFound:", task_name)

    def taskByName(self, task_name):
        return self.tasks[self.taskID(task_name)]

    def addPrecedence(self, task_1_name, task_2_name, demand):
        task_1, task_2 = self.taskID(task_1_name), self.taskID(task_2_name)
        self.parents[task_2].append((task_1, demand))
        self.children[task_1].append(task_2)

    def addDataDemand(self, task_name, database, demand):
        task = self.taskID(task_name)
        self.dataneed[task].append((database, demand))

class Stage:

    def __init__(self, job_ID, stage_ID, tasks):
        self.job_ID = job_ID
        self.stage_ID = stage_ID
        self.tasks = tasks
        self.finish = False
        self.wait = 0

    def __repr__(self):
        return "<Stage (Job {}, Stage {}, Tasks {})>".format(
            self.job_ID, self.stage_ID, self.tasks)

class Resource:

    def __init__(self):
        self.datacenters = []
        self.processors = []
        with open(filePath.FILE_DC_SLOT, "r") as reader:
            for line in reader.readlines():
                datacenter, processor = line.strip().split()
                datacenter, processor = datacenter.strip(), int(processor.strip())
                self.datacenters.append(datacenter)
                self.processors.append(processor)
        
        self.databases = []
        self.datalocat = []
        with open(filePath.FILE_DC_DATA, "r") as reader:
            for line in reader.readlines():
                database, dataloct = line.strip().split()
                self.databases.append(database.strip())
                self.datalocat.append(self.dcID(dataloct.strip()))
        
        self.bandwidths = [[1e10 for i in range(len(self.datacenters))] for i in range(len(self.datacenters))]
        self.flag = [[-1 for j in range(len(self.datacenters))] for i in range(len(self.datacenters))]
        self.path = [[[] for j in range(len(self.datacenters))] for i in range(len(self.datacenters))]
        with open(filePath.FILE_DC_LINK, "r") as reader:
            for line in reader.readlines():
                dc_1, dc_2, bandwidth = line.strip().split()
                dc_1, dc_2, bandwidth = self.dcID(dc_1.strip()), self.dcID(dc_2.strip()), float(bandwidth.strip())
                self.bandwidths[dc_1][dc_2] = 1000 / bandwidth
        self.bandwidths_sav = copy.deepcopy(self.bandwidths)
        self.extendLink()
        for i in range(len(self.datacenters)):
            for j in range(len(self.datacenters)):
                self.buildPath(i, j, self.path[i][j])
                self.path[i][j] = self.path[i][j][:-1]
    
    def extendLink(self):
        for k in range(len(self.datacenters)):
            for i in range(len(self.datacenters)):
                for j in range(len(self.datacenters)):
                    if self.bandwidths[i][j] > 2 * max(self.bandwidths[i][k], self.bandwidths[k][j]):
                        self.bandwidths[i][j] = 2 * max(self.bandwidths[i][k], self.bandwidths[k][j])
                        self.flag[i][j] = k

    def tidyPath(self, i, j):
        _path = self.path[i][j]
        if _path:
            path = [(i, _path[0]), (_path[-1], j)]
            path += [(_path[p], _path[p + 1]) for p in range(len(_path) - 1)]
        else:
            path = [(i, j)]
        return path

    def showPath(self, i, j):
        sentences = []
        path = self.tidyPath(i, j)
        for u in range(len(self.datacenters)):
            for v in range(len(self.datacenters)):
                bandwidth = int(1000 / self.bandwidths_sav[u][v])
                if bandwidth > 0 and u != v:
                    color = "red" if (u, v) in path else "black"
                    sentences.append(f'{self.datacenters[u]} -> {self.datacenters[v]} [label="{bandwidth}", color="{color}"]\n')
        with open("raw_links.dot", "w") as writer:
            writer.write("digraph G {{{}}}\n".format("".join(sentences)))
        os.system("dot raw_links.dot -T png -o pic.png")
        subprocess.call(["open", "pic.png"])
        sentences = []
        for u in range(len(self.datacenters)):
            for v in range(len(self.datacenters)):
                bandwidth = float(1000 / self.bandwidths[u][v])
                if bandwidth > 0 and u != v:
                    color = "red" if (i, j) == (u, v) else "black"
                    sentences.append(f'{self.datacenters[u]} -> {self.datacenters[v]} [label="{bandwidth:.1f}", color="{color}"]\n')
        with open("raw_links.dot", "w") as writer:
            writer.write("digraph G {{{}}}\n".format("".join(sentences)))
        os.system("dot raw_links.dot -T png -o pic_2.png")
        subprocess.call(["open", "pic_2.png"])
    
    def buildPath(self, i, j, path):
        if i == j:
            return  
        elif self.flag[i][j] == -1:
            path.append(j)
        else:
            self.buildPath(i, self.flag[i][j], path)
            self.buildPath(self.flag[i][j], j, path)

    def dcID(self, datacenter):
        return self.datacenters.index(datacenter)

    def dbID(self, database):
        return self.databases.index(database)

class DAGScheduler:

    def __init__(self, resource):
        self.resource = resource
        self.jobs = []
        self.jobs_stages = []
        with open(filePath.FILE_TASK_EXEC_TIME, "r") as reader:
            for line in reader.readlines():
                job_name, tasks = line.strip().split(':')
                job_name, tasks = job_name.strip(), tasks.strip().split(',')
                job = Job(job_name)
                for task in tasks:
                    if not task.strip(): continue
                    task_name, exec_time = task.strip().split(' ')
                    task_name, exec_time = task_name.strip(), float(exec_time.strip())
                    job.addTask(Task(task_name, exec_time))
                self.addJob(job)

        with open(filePath.FILE_TASK_PRECEDENCE, "r") as reader:
            for line in reader.readlines():
                job_name, precedences = line.strip().split(':')
                job_name, precedences = job_name.strip(), precedences.strip().split(',')
                for precedence in precedences:
                    if not precedence.strip(): continue
                    task_1, task_2, demand = precedence.strip().split()
                    task_1, task_2, demand = task_1.strip(), task_2.strip(), float(demand.strip())
                    self.addPrecedence(job_name, task_1, task_2, demand)

        with open(filePath.FILE_TASK_DATADEMAND, "r") as reader:
            for line in reader.readlines():
                job_name, dataDemands = line.strip().split(':')
                job_name, dataDemands = job_name.strip(), dataDemands.strip().split(',')
                for dataDemand in dataDemands:
                    if not dataDemand.strip(): continue
                    task, database, demand = dataDemand.strip().split()
                    task, database, demand = task.strip(), database.strip(), float(demand.strip())
                    self.addDataDemand(job_name, task, database, demand)

    def addJob(self, job):
        self.jobs.append(job)
        self.jobs_stages.append([])

    def jobID(self, job_name):
        for i, job in enumerate(self.jobs):
            if job.job_name == job_name:
                return i
        raise Exception("JobNotFound:", job_name)

    def jobByName(self, job_name):
        return self.jobs[self.jobID(job_name)]

    def addPrecedence(self, job_name, task_1_name, task_2_name, demand):
        self.jobs[self.jobID(job_name)].addPrecedence(task_1_name, task_2_name, demand)

    def addDataDemand(self, job_name, task_name, database, demand):
        self.jobs[self.jobID(job_name)].addDataDemand(task_name, self.resource.dbID(database), demand)

    def partition(self):
        for k, job in enumerate(self.jobs):
            parents = copy.copy(job.parents)
            taskres = set(range(len(job.tasks)))
            while True:
                stage = [i for i in taskres if not parents[i]]
                for i in stage:
                    for j in job.children[i]:
                        parents[j] = list(filter(lambda parent: parent[0] != i, parents[j]))
                taskres -= set(stage)
                if not stage: break   
                self.jobs_stages[k].append(Stage(k, len(self.jobs_stages[k]), stage))

    def getStage(self, job_ID, stage_ID):
        if stage_ID >= len(self.jobs_stages[job_ID]): return None
        return self.jobs_stages[job_ID][stage_ID]


    def showTasks(self):
        for job in self.jobs:
            print(job.job_name)
            for i in range(len(job.tasks)):
                print(job.tasks[i].task_name, job.tasks[i].exec_time, job.parents[i])


class TaskScheduler:

    def __init__(self, dags):
        self.dags = dags
        self.resource = dags.resource
        self.dags.partition()
        self.task_pool = self.initialPool()
        self.to_launch = []
        self.time_point = 0
        self.finish_time = []
        self.solutions = {}

    def initialPool(self):
        return [stages[0] for stages in self.dags.jobs_stages]

    def refreshPool(self):
        new_pool = []
        for stage in self.to_launch: self.task_pool[stage].finish = True
        self.to_launch.clear()
        for stage in self.task_pool:
            if stage.finish:
                stage_next = self.dags.getStage(stage.job_ID, stage.stage_ID + 1)
                if stage_next: 
                    new_pool.append(stage_next)
                else:
                    # print(f"Job {self.dags.jobs[stage.job_ID].job_name} done at {self.time_point:.2f}s")
                    self.finish_time.append(self.time_point)
            else:
                stage.wait += 1
                new_pool.append(stage)
        self.task_pool = sorted(
            new_pool, reverse=True,
            key=lambda s: max(s.wait, len(self.dags.jobs_stages[s.job_ID]))
        )

    def jobOfStage(self, k):
        return self.dags.jobs[self.task_pool[self.to_launch[k]].job_ID]

    def showAssign(self, k, i, j):
        print("({}, {}, {})".format(
            self.jobOfStage(k).job_name,
            self.jobOfStage(k).tasks[i].task_name,
            self.resource.datacenters[j]
        ))

    def schedule(self, mode="schedule"):
        # print(f"Start Scheduling at {self.time_point:.2f}s")

        task_total, slot_total = 0, sum(self.resource.processors)
        for i in range(len(self.task_pool)):
            if task_total + len(self.task_pool[i].tasks) > slot_total: break
            self.to_launch.append(i)
            task_total += len(self.task_pool[i].tasks)

        assert self.to_launch
        self.solutions.clear()

        # for k in self.to_launch:
        #     print("{}-{}: {}".format(
        #         self.jobOfStage(k).job_name,
        #         self.task_pool[k].stage_ID,
        #         ', '.join([self.jobOfStage(k).tasks[task_ID].task_name for task_ID in self.task_pool[k].tasks])
        #     ))

        J, K = len(self.resource.datacenters), len(self.to_launch)
        n = lambda k: len(self.task_pool[self.to_launch[k]].tasks)
        M = J * sum([n(k) for k in range(K)])
        C = lambda k, i, j: max([
            demand * self.resource.bandwidths[self.resource.datalocat[database]][j] / 1000
            for database, demand in self.jobOfStage(k).dataneed[i] 
        ])
        E = lambda k, i, j: self.jobOfStage(k).tasks[i].exec_time
        A = lambda k, i, j: M ** (C(k, i, j) + E(k, i, j)) - 1 

        # ATTENTION: k 是 self.to_launch 的下标
        if mode != "schedule":
            random_tasks = [(k, i) for k in range(K) for i in range(n(k))]
            random_slots = [j for j in range(J) for p in range(self.resource.processors[j])]
            random.shuffle(random_tasks)
            random.shuffle(random_slots)
            random_assign = list(zip(random_tasks, random_slots[: len(random_tasks)]))
        else:
            function = [[[A(k, i, j) for j in range(J)] for i in range(n(k))] for k in range(K)]
            upperbound_l = [[[[int(j ==_j) for j in range(J)] for i in range(n(k))] for k in range(K)] for _j in range(J)]
            upperbound_r = [self.resource.processors[j] for j in range(J)]
            equality_l = [[[[int(k ==_k and i ==_i) for j in range(J)] for i in range(n(k))] for k in range(K)] for _k in range(K) for _i in range(n(_k))]
            equality_r = [1 for k in range(K) for i in range(n(k))]
            bounds = [[[(0, 1) for j in range(J)] for i in range(n(k))] for k in range(K)]

            _flatten = lambda x: [y for _x in x for y in _flatten(_x)] if isinstance(x, list) else [x]
            _flatten_= lambda x: [_flatten(y) for y in x]
        
        band_allocate = {}

        # print(upperbound_l)

        for epoch in range(task_total):
            if mode != "schedule":
                (_k, _i), _j = random_assign[epoch]
            else:
                res = opt.linprog(
                    c=np.array(_flatten(function)), 
                    A_ub=np.array(_flatten_(upperbound_l)), 
                    b_ub=np.array(upperbound_r), 
                    A_eq=np.array(_flatten_(equality_l)), 
                    b_eq=np.array(equality_r), 
                    bounds=tuple(_flatten(bounds)),
                    method="simplex"
                )
                _x, _k, _i, _j = 0, -1, -1, -1
                index = 0
                for k in range(K):
                    for i in range(n(k)):
                        for j in range(J):
                            x = res.x[index] * (C(k, i, j) + E(k, i, j))
                            if x > _x: _x, _k, _i, _j = x, k, i, j
                            index += 1
                
                A_k_i_j = A(_k, _i, _j)
                function[_k] = [[A_k_i_j for j in range(J)]for i in range(n(_k))]
                function[_k][_i] = [0 for j in range(J)]
                
                for j in range(J): upperbound_l[j][_k][_i][_j] = 0
                upperbound_r[_j] -= 1
                
                index_1 = sum([n(k) for k in range(_k)]) + _i
                equality_l[index_1][_k][_i] = [0 for j in range(J)]
                equality_r[index_1] = 0
                
                bounds[_k][_i] = [(0, 0) for j in range(J)]

            # self.showAssign(_k, _i, _j)
            self.solutions[(_k, _i)] = _j

            for db_ID, demand in self.jobOfStage(_k).dataneed[_i]:
                dc_ID = self.resource.datalocat[db_ID]
                for p in self.resource.tidyPath(dc_ID, _j):
                    if p in band_allocate:
                        band_allocate[p].append((_k, _i, dc_ID, demand))
                    else:
                        band_allocate[p] = [(_k, _i, dc_ID, demand)]

        task_time = {}
        for u, v in band_allocate.keys():
            bandwidth = 1000 / self.resource.bandwidths_sav[u][v]
            demand_sum = sum([item[3] for item in band_allocate[(u, v)]])
            for k, i, dc_ID, demand in band_allocate[(u, v)]:
                time = demand_sum / bandwidth
                # print('{}, {} -> {}: {}, {}, {}, {}, {}'.format(
                #     self.jobOfStage(k).tasks[i].task_name,
                #     self.resource.datacenters[dc_ID],
                #     self.resource.datacenters[self.solutions[(k, i)]],
                #     (u, v), demand, demand_sum, bandwidth, time
                # ))
                if (k, i) in task_time:
                    if dc_ID in task_time[(k, i)]:
                        task_time[(k, i)][dc_ID] += time
                    else:
                        task_time[(k, i)][dc_ID] = time
                else:
                    task_time[(k, i)] = {dc_ID: time}

        time_delta = 0
        for (k, i), times in task_time.items():
            tran_time = max(times.values())
            exec_time = self.jobOfStage(k).tasks[i].exec_time
            time_final = tran_time + exec_time
            time_delta = max(time_delta, time_final)
            # print(f"{self.jobOfStage(k).tasks[i].task_name}: {time_final:.2f}={tran_time:.2f}+{exec_time:.2f}")

        self.time_point += time_delta


def scheduleTest():
    resource = Resource()
    # resource.showPath(3, 2)
    dags = DAGScheduler(resource)
    task_scheduler = TaskScheduler(dags)
    while task_scheduler.task_pool:
        task_scheduler.schedule("schedule")
        task_scheduler.refreshPool()
    avg_time = sum(task_scheduler.finish_time) / len(task_scheduler.finish_time)
    print(f"All job done at {task_scheduler.time_point:.2f}s")
    print(f"Average job finish time: {avg_time:.2f}s")
    return avg_time

def randomTest():
    resource = Resource()
    # resource.showPath(3, 2)
    dags = DAGScheduler(resource)
    task_scheduler = TaskScheduler(dags)
    while task_scheduler.task_pool:
        task_scheduler.schedule("random")
        task_scheduler.refreshPool()
    return sum(task_scheduler.finish_time) / len(task_scheduler.finish_time)

if __name__ == '__main__':
    filePath = FilePath("./")
    _x = scheduleTest()
    x = sorted([randomTest() for i in range(50)])
    y = [0] * len(x)
    plt.subplot(2, 1, 1)
    plt.yticks([])
    plt.scatter(x, y, s=75, c="red", alpha=.3)
    plt.scatter(_x, 0, s=75, c="green", alpha = .3)

    filePath = FilePath("data/data1/")
    _x = scheduleTest()
    x = sorted([randomTest() for i in range(50)])
    y = [0] * len(x)
    plt.subplot(2, 1, 2)
    plt.yticks([])
    plt.scatter(x, y, s=75, c="red", alpha=.3)
    plt.scatter(_x, 0, s=75, c="green", alpha = .3)
    plt.show()
