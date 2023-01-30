from listener.celery import celery
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import celery as celeryapp
import os

retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    backoff_factor=1
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http_client = requests.Session()
http_client.mount("https://", adapter)
http_client.mount("http://", adapter)

@celery.task(ignore_result=False, acks_late=True)
def do_task(message):
    url = "https://localhost:8081/jstor_harvester/do_task"
    response = http_client.post(url, json = message, verify=False)
    celeryapp.execute.send_task("tasks.tasks.do_task", args=[message], kwargs={}, queue=os.getenv('NEXT_QUEUE_NAME'))
    return response.json()
