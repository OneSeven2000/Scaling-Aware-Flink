import subprocess
import requests
import os
import json
import time


def restart(cpu, memory, slot):
    subprocess.Popen("kubectl delete deployment/my-first-flink-cluster", stdout=subprocess.PIPE, shell=True).communicate()
    set_taskmanager_memory(memory)
    set_taskmanager_slots(slot)
    subprocess.Popen("./bin/kubernetes-session.sh -Dkubernetes.cluster-id=my-first-flink-cluster "
                    "-Dkubernetes.taskmanager.cpu=%s" % (cpu,), stdout=subprocess.PIPE, shell=True).communicate()
    return


def get_pod_resource_load(base_url, pod_name):
    url = base_url + "apis/metrics.k8s.io/v1beta1/namespaces/default/pods/" + pod_name
    response = requests.get(url)
    cpu = json.dumps(response.json()["containers"][0]["usage"]["cpu"])
    memory = json.dumps(response.json()["containers"][0]["usage"]["memory"])
    return cpu[1:len(cpu)-1], memory[1:len(memory)-1]


def set_taskmanager_memory(memory):
    data = ''
    with open('/home/yuan/flink/conf/flink-conf.yaml', 'r+') as f:
        for line in f.readlines():
            if line.find('taskmanager.memory.process.size:') == 0:
                line = 'taskmanager.memory.process.size: %sm' % (memory,) + '\n'
            data += line
    with open('/home/yuan/flink/conf/flink-conf.yaml', 'r+') as f:
        f.writelines(data)
    return


def set_taskmanager_slots(slot):
    data = ''
    with open('/home/yuan/flink/conf/flink-conf.yaml', 'r+') as f:
        for line in f.readlines():
            if line.find('taskmanager.numberOfTaskSlots:') == 0:
                line = 'taskmanager.numberOfTaskSlots: %s' % (slot,) + '\n'
            data += line
    with open('/home/yuan/flink/conf/flink-conf.yaml', 'r+') as f:
        f.writelines(data)
    return


def submit_job(base_url, jar_id):
    url = base_url + "jars/" + jar_id + "/run"
    response = requests.post(url)
    value = json.dumps(response.json()['jobid'])
    return value[1:len(value)-1]


def terminate_job(base_url, job_id):
    url = base_url + "jobs/" + job_id
    requests.patch(url)
    return


def upload_jar(base_url, path):
    url = base_url + "/jars/upload"
    myfile = {"jarfile": (
        os.path.basename(path),
        open(path, "rb"),
        "application/x-java-archive"
    )}
    response = requests.post(url, files=myfile)
    value = json.dumps(response.json()['filename'])
    return value[1:len(value)-1]


def delete_jar(base_url, jar_id):
    url = base_url + "jars/" + jar_id
    requests.delete(url)
    return


def get_all_jobs_overview(base_url):
    url = base_url + "jobs/overview"
    response = requests.get(url)
    job_cnt = response.text.count("jid")
    job_list = []
    for i in range(job_cnt):
        job_name = json.dumps(response.json()['jobs'][i - 1]['jid'])
        job_list.append(job_name[1:len(job_name) - 1])
    return job_cnt, job_list


def get_job_overview(base_url, job_id):
    url = base_url + "jobs/" + job_id
    value = requests.get(url)
    return value


def get_all_jars(base_url):
    url = base_url + "jars"
    value = requests.get(url)
    return value


def get_flink_cluster_overview(base_url):
    url = base_url + "overview"
    value = requests.get(url)
    return value


def open_file(filePathAndName):
    file = open(filePathAndName, 'a')
    file.truncate()
    file.close()
    file = open(filePathAndName, 'a')
    return file


def get_taskmanager_CPU_load(base_url, taskmanager_name):
    url = base_url + "taskmanagers/" + taskmanager_name + "/metrics?get=Status.JVM.CPU.Load"
    response = requests.get(url)
    value = json.dumps(response.json()[0]['value'])
    return value[1:len(value)-1]


def get_taskmanager_memory_load(base_url, taskmanager_name):
    url = base_url + "taskmanagers/" + taskmanager_name + "/metrics?get=Status.JVM.Memory.Heap.Used"
    response = requests.get(url)
    value = json.dumps(response.json()[0]['value'])
    return value[1:len(value)-1]


def get_all_taskmanagers_overview(base_url):
    url = base_url + "taskmanagers"
    response = requests.get(url)
    taskmanager_cnt = response.text.count("id")
    taskmanager_list = []
    for i in range(taskmanager_cnt):
        taskmanager_name = json.dumps(response.json()['taskmanagers'][i-1]['id'])
        taskmanager_list.append(taskmanager_name[1:len(taskmanager_name)-1])
    return taskmanager_cnt, taskmanager_list


def main():
    pod_status = subprocess.Popen("kubectl get pods -A", stdout=subprocess.PIPE, shell=True).communicate()
    k8s_base_url = "http://localhost:8080/"
    flink_base_url = "http://localhost:8081/"
    path = "/home/yuan/flink/examples/streaming/SessionWindowing.jar"


if __name__ == "__main__":
    main()
