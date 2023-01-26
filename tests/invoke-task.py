from celery import Celery

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

# Send a simple task (create and send in 1 step)
res = app1.send_task('tasks.tasks.do_task', args=[{"job_ticket_id":"123","hello":"world"}], kwargs={}, queue="harvest_jstorforum")
