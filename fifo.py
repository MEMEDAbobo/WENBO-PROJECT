def MapFunction(data):
    cur_results = []
    words = data.split()
    for word in words:
        cur_results.append((word, 1))
    return cur_results

def ReduceFunction(key, values):
    result = sum(values)
    return (key, result)


def MapReduce(data):
    cur_results = []
    for sentence in data:
        cur_results.extend(MapFunction(sentence))

    FinalResults = []
    key_values = {}
    for key, value in cur_results:
        if key not in key_values:
            key_values[key] = []
        key_values[key].append(value)

    for key, values in key_values.items():
        FinalResults.append(ReduceFunction(key, values))

    return FinalResults

# 测试代码
# data1 = ["reviewing development work produced by third party agencies"," This will give you a deeper opportunity to take your skills and turn them to practical use in assisting real-world clients", "the internship will give the successful candidate the chance for more practical","The internship pays the Glasgow Living Wage","his has an option for hybrid working in our City Centre office in Glasgow"]
# data2 = ["Hello world", "Hello frank", "frank is awesome","Hello jack"]

import threading
import time

class Node(threading.Thread):
    def __init__(self, node_id):
        threading.Thread.__init__(self)
        self.node_id = node_id
        self.tasks = []
        self.busy = False

    def run(self):
        print("Node", self.node_id, "started")
        while True:
            if len(self.tasks) > 0:
                self.busy = True
                task = self.tasks.pop(0)  # get a task
                # print("Node", self.node_id, "processing task:", task)
                results = MapReduce([task])  # process the task
                # print("Node", self.node_id, "results:", results)
                time.sleep(10)  # simulate time-consuming task
                # print("Node", self.node_id, "finished processing task:", task)
                self.busy = False


class TaskScheduler:
    def __init__(self, nodes):
        self.nodes = nodes

    def assign_task(self, tasks):
        # Assign each task to a node
        for task in tasks:
            while True:  # keep trying until the task is assigned
                for node in self.nodes:
                    if not node.busy:  # find an idle node
                        node.tasks.append(task)  # assign the task
                        print("Assigned task:", task, "to node", node.node_id)
                        break
                else:  # if no idle node is found
                    continue  # try again
                break  # if the task is assigned, move on to the next task


data = ["task1 happy join happy", "task2 like hi hi hi hi", "task3 like dede", "task4 hello hi", "task5 hi"]
num_nodes = 2

nodes = [Node(i) for i in range(num_nodes)]
scheduler = TaskScheduler(nodes)

for node in nodes:
    node.start()
scheduler.assign_task(data)

for node in nodes:
    node.join()
