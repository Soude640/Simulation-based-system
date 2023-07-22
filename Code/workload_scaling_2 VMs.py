import asyncio
import datetime
import time
from datetime import timedelta

import docker
import requests
from docker.types import ServiceMode
from prometheus_client import push_to_gateway, CollectorRegistry, Counter, Summary, Gauge

url = 'http://localhost:5001/get/'
registry = CollectorRegistry()
fail_response_count = Counter('fail_response_counter', 'count fail responses')
number_of_requests = Counter('number_of_requests', 'count number of requests')
online_user_count = Gauge('online_user_counter', 'count online users')
alive_worker_count = Gauge('alive_worker_counter', 'count alive workers')
success_response_count = Counter('success_response_counter', 'count success responses')
process_time_summary = Summary('process_time_summery', 'process time')
registry.register(fail_response_count)
registry.register(number_of_requests)
registry.register(online_user_count)
registry.register(alive_worker_count)
registry.register(success_response_count)
registry.register(process_time_summary)


def push_metrics():
    global registry
    push_to_gateway('http://localhost:9091', job="spark", registry=registry)


client = docker.APIClient()
service_worker_node_1 = 'test_masterspark-worker'
service_worker_node_2 = 'test_spark-worker'

node_workers = {
    "1": {
        service_worker_node_1: 1,
        service_worker_node_2: 0
    },
    "2": {
        service_worker_node_1: 1,
        service_worker_node_2: 1
    },
    "3": {
        service_worker_node_1: 2,
        service_worker_node_2: 1
    },
    "4": {
        service_worker_node_1: 2,
        service_worker_node_2: 2
    }
}


def scale_workers(prev_worker_counts: int, worker_counts: int):
    for node, node_worker_counts in node_workers.get(str(worker_counts)).items():
        prev_node_worker_counts = node_workers.get(str(prev_worker_counts)).get(node)
        if prev_node_worker_counts != node_worker_counts:
            print(f'scale service {node} from {prev_node_worker_counts} to {node_worker_counts} replicas starting')
            inspect = client.inspect_service(node)
            svc_version = inspect['Version']['Index']
            svc_id = inspect['ID']
            update_config = docker.types.UpdateConfig(parallelism=1, failure_action='rollback', order='start-first')
            client.update_service(
                svc_id, svc_version, name=node,
                mode=ServiceMode('replicated', replicas=node_worker_counts),
                fetch_current_spec=True, update_config=update_config
            )
            print(f'scale service {node} from {prev_node_worker_counts}  to {node_worker_counts} replicas done')


def get_req(headers: dict):
    requests.get(url, {})


async def main():
    loop = asyncio.get_event_loop()
    users_count = [2, 7, 5, 4, 5, 10, 8, 12, 15, 17, 13, 14]
    workers_number = [1, 2, 1, 1, 1, 2, 2, 3, 3, 4, 3, 3]
    is_need_spark_session_stop = False
    try:
        for index in range(len(users_count)):
            user_count = users_count[index]
            print(f'number of users: {user_count}')
            if index > 0 and workers_number[index - 1] != workers_number[index]:
                is_need_spark_session_stop = True
                scale_workers(workers_number[index - 1], workers_number[index])
                time.sleep(33)
            start_time = datetime.datetime.now()
            while datetime.datetime.now() - start_time < timedelta(minutes=5):
                print(datetime.datetime.now() - start_time)
                online_user_count.set(user_count)
                alive_worker_count.set(workers_number[index])
                request_list = list()
                before = time.time()
                for i in range(user_count):
                    request_list.append(
                        loop.run_in_executor(None, requests.get, url, {
                            'is_need_spark_session_stop': is_need_spark_session_stop
                        })
                    )
                    is_need_spark_session_stop = False
                    number_of_requests.inc()
                for i in range(len(request_list)):
                    response = await request_list[i]
                    if "Row" in response.text:
                        process_time_summary.observe(time.time() - before)
                        success_response_count.inc()
                    else:
                        fail_response_count.inc()
                push_metrics()
    except Exception as e:
        print(e)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
