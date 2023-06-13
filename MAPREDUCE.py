
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

class Node(threading.Thread):
    def __init__(self, node_id):
        threading.Thread.__init__(self)
        self.node_id = node_id
        self.data = []

    def run(self):
        print("Node", self.node_id, "processing data:", self.data)
        results = MapReduce(self.data)
        print("Node", self.node_id, "results:", results)
        print("Node", self.node_id, "finished processing")


class TaskScheduler:
    def __init__(self, nodes):
        self.nodes = nodes

    def assign_task(self, data):
        for node in self.nodes:
            if not node.data:
                node.data.append(data)
                return

data = ["Hello world", "Hello jack", "jack is awesome", "Welcome to MapReduce", "Distributed computing is powerful", "FCFS scheduling algorithm", "Node-based processing", "Data parallelism", "Task distribution", "Results aggregation"]
num_nodes = 3


nodes = []
for i in range(num_nodes):
    node = Node(i)
    nodes.append(node)

scheduler = TaskScheduler(nodes)


for d in data:
    scheduler.assign_task(d)


for node in nodes:
    node.start()

for node in nodes:
    node.join()
