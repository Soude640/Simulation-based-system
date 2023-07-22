import asyncio
import datetime
import time
from datetime import timedelta

import requests
from prometheus_client import push_to_gateway, CollectorRegistry, Counter, Summary, Gauge

url = 'http://localhost:5001/get/'
registry = CollectorRegistry()
fail_response_count = Counter('fail_response_counter', 'count fail responses')
online_user_count = Gauge('online_user_counter', 'count online users')
success_response_count = Counter('success_response_counter', 'count success responses')
process_time_summary = Summary('process_time_summery', 'process time')
registry.register(fail_response_count)
registry.register(online_user_count)
registry.register(success_response_count)
registry.register(process_time_summary)


def push_metrics():
    global registry
    push_to_gateway('http://localhost:9091', job="spark", registry=registry)


async def main():
    loop = asyncio.get_event_loop()
    users_count = [1, 3, 5, 7, 10, 7, 5, 3, 1]
    while True:
        for user_count in users_count:
            start_time = datetime.datetime.now()
            while datetime.datetime.now() - start_time < timedelta(minutes=5):
                online_user_count.set(user_count)
                request_list = list()
                before = time.time()
                for i in range(user_count):
                    request_list.append(loop.run_in_executor(None, requests.get, url))
                for i in range(len(request_list)):
                    response = await request_list[i]
                    if "Row" in response.text:
                        process_time_summary.observe(time.time() - before)
                        success_response_count.inc()
                    else:
                        fail_response_count.inc()
                push_metrics()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
