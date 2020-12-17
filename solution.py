# -*- coding: utf-8
# -*- author: Shangyu Liu

from scipy import optimize as opt
import numpy as np
import copy
import sys

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
        with open("datacenter_slot.dat", "r") as reader:
            for line in reader.readlines():
                datacenter, processor = line.strip().split()
                datacenter, processor = datacenter.strip(), int(processor.strip())
                self.datacenters.append(datacenter)
                self.processors.append(processor)
        
        self.databases = []
        self.datalocat = []
        with open("databases.dat", "r") as reader:
            for line in reader.readlines():
                database, dataloct = line.strip().split()
                self.databases.append(database.strip())
                self.datalocat.append(self.dcID(dataloct.strip()))
        
        self.bandwidths = [[1e10 for i in range(len(self.datacenters))] for i in range(len(self.datacenters))]
        self.flag = [[-1 for j in range(len(self.datacenters))] for i in range(len(self.datacenters))]
        self.path = [[[] for j in range(len(self.datacenters))] for i in range(len(self.datacenters))]
        with open("bandwidth.dat", "r") as reader:
            for line in reader.readlines():
                dc_1, dc_2, bandwidth = line.strip().split()
                dc_1, dc_2, bandwidth = self.dcID(dc_1.strip()), self.dcID(dc_2.strip()), float(bandwidth.strip())
                self.bandwidths[dc_1][dc_2] = 1 / bandwidth
        self.extendLink()
        for i in range(len(self.datacenters)):
            for j in range(len(self.datacenters)):
                self.buildPath(i, j, self.path[i][j])
                self.path[i][j] = self.path[i][j][:-1]
                print(f"{i}->{j}: {self.path[i][j]}")
    
    def extendLink(self):
        for k in range(len(self.datacenters)):
            for i in range(len(self.datacenters)):
                for j in range(len(self.datacenters)):
                    if self.bandwidths[i][j] > 2 * max(self.bandwidths[i][k], self.bandwidths[k][j]):
                        self.bandwidths[i][j] = 2 * max(self.bandwidths[i][k], self.bandwidths[k][j])
                        self.flag[i][j] = k
    
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
        with open("task_exec_time.dat", "r") as reader:
            for line in reader.readlines():
                job_name, tasks = line.strip().split(':')
                job_name, tasks = job_name.strip(), tasks.strip().split(',')
                job = Job(job_name)
                for task in tasks:
                    task_name, exec_time = task.strip().split(' ')
                    task_name, exec_time = task_name.strip(), float(exec_time.strip())
                    job.addTask(Task(task_name, exec_time))
                self.addJob(job)
        with open("task_precedence.dat", "r") as reader:
            for line in reader.readlines():
                job_name, precedences = line.strip().split(':')
                job_name, precedences = job_name.strip(), precedences.strip().split(',')
                for precedence in precedences:
                    task_1, task_2, demand = precedence.strip().split()
                    task_1, task_2, demand = task_1.strip(), task_2.strip(), float(demand.strip())
                    self.addPrecedence(job_name, task_1, task_2, demand)

        with open("task_dataDemand.dat", "r") as reader:
            for line in reader.readlines():
                job_name, dataDemands = line.strip().split(':')
                job_name, dataDemands = job_name.strip(), dataDemands.strip().split(',')
                for precedence in dataDemands:
                    task, database, demand = precedence.strip().split()
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
                stage = []
                for i in taskres:
                    if not parents[i]:
                        stage.append(i)
                for i in stage:
                    for j in job.children[i]:
                        parents[j] = list(filter(lambda parent: parent[0] != i, parents[j]))
                taskres -= set(stage)
                if not stage: break   
                self.jobs_stages[k].append(Stage(k, len(self.jobs_stages[k]), stage))

    def getStage(self, job_ID, stage_ID):
        if stage_ID >= len(self.jobs_stages[job_ID]):
            return None
        return self.jobs_stages[job_ID][stage_ID]


    def print(self):
        for job in self.jobs:
            print(job.job_name)
            for i in range(len(job.tasks)):
                print(job.tasks[i].task_name, job.tasks[i].exec_time, job.parents[i])


class TaskScheduler:

    def __init__(self, dags):
        self.schedule_pool = []
        self.stages_launch = []
        self.dag_scheduler = dags
        self.resource = dags.resource
        self.initialPool()
        self.time_point = 0

    def initialPool(self):
        for stages in self.dag_scheduler.jobs_stages:
            self.schedule_pool.append(stages[0])

    def refreshPool(self):
        new_pool = []
        for stage in self.stages_launch:
            self.schedule_pool[stage].finish = True
        self.stages_launch.clear()
        for stage in self.schedule_pool:
            if stage.finish:
                stage_next = self.dag_scheduler.getStage(stage.job_ID, stage.stage_ID + 1)
                if stage_next:
                    new_pool.append(stage_next)
            else:
                stage.wait += 1
                new_pool.append(stage)
        self.schedule_pool = sorted(new_pool, key=lambda s: max(s.wait, len(self.dag_scheduler.jobs_stages[s.job_ID])), reverse=True)

    def jobOfStage(self, k):
        return self.dag_scheduler.jobs[self.schedule_pool[k].job_ID]

    def printAssign(self, k, i, j):
        print("assign job {}'s task {} to datacenter {}".format(
            self.jobOfStage(k).job_name,
            self.jobOfStage(k).tasks[i].task_name,
            self.resource.datacenters[j]
        ))

    def schedule(self):
        print(f"Start Scheduling at {self.time_point}s")

        task_total, slot_total = 0, sum(self.resource.processors)
        for i in range(len(self.schedule_pool)):
            if task_total + len(self.schedule_pool[i].tasks) > slot_total: break
            self.stages_launch.append(i)
            task_total += len(self.schedule_pool[i].tasks)

        J, K = len(self.resource.datacenters), len(self.stages_launch)
        n = lambda k: len(self.schedule_pool[self.stages_launch[k]].tasks)
        M = J * sum([n(k) for k in range(K)])
        C = lambda k, i, j: max([
            demand * self.resource.bandwidths[self.resource.datalocat[database]][j] 
            for database, demand in self.jobOfStage(k).dataneed[i] 
        ])
        E = lambda k, i, j: self.jobOfStage(k).tasks[i].exec_time
        A = lambda k, i, j: M ** (C(k, i, j) + E(k, i, j)) - 1 

        time_delta = 0

        function = [[[A(k, i, j) for j in range(J)] for i in range(n(k))] for k in range(K)]
        upperbound_l = [[[[int(j ==_j) for j in range(J)] for i in range(n(k))] for k in range(K)] for _j in range(J)]
        upperbound_r = [self.resource.processors[j] for j in range(J)]
        equality_l = [[[[int(k ==_k and i ==_i) for j in range(J)] for i in range(n(k))] for k in range(K)] for _k in range(K) for _i in range(n(_k))]
        equality_r = [1 for k in range(K) for i in range(n(k))]
        bounds = [[[(0, 1) for j in range(J)] for i in range(n(k))] for k in range(K)]

        _flatten = lambda x: [y for _x in x for y in _flatten(_x)] if isinstance(x, list) else [x]
        _flatten_= lambda x: [_flatten(y) for y in x]
        
        for _ in range(task_total):
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
            
            self.printAssign(_k, _i, _j)
            
            A_k_i_j = A(_k, _i, _j)
            function[_k] = [[A_k_i_j for j in range(J)]for i in range(n(_k))]
            function[_k][_i] = [0 for j in range(J)]
            
            for j in range(J): upperbound_l[j][_k][_i][_j] = 0
            upperbound_r[_j] -= 1
            
            index_1 = sum([n(k) for k in range(_k)]) + _i
            equality_l[index_1][_k][_i] = [0 for j in range(J)]
            equality_r[index_1] = 0
            
            bounds[_k][_i] = [(0, 0) for j in range(J)]
            
            time_delta = max(time_delta, _x)
        self.time_point += time_delta


if __name__ == '__main__':
    resource = Resource()
    dag_scheduler = DAGScheduler(resource)
    dag_scheduler.partition()
    task_scheduler = TaskScheduler(dag_scheduler)
    while True:
        task_scheduler.schedule()
        task_scheduler.refreshPool()
        if not task_scheduler.schedule_pool:
            break
