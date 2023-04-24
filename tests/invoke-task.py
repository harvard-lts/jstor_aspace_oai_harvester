from celery import Celery

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

# Send a simple task (create and send in 1 step)
#res = app1.send_task('tasks.tasks.do_task', args=[{"job_ticket_id":"123","jstorforum":True,"harvestset":"713","harvesttype":"full"}], kwargs={}, queue="harvest_jstorforum")
#test for until, will result in only 1 rec
res = app1.send_task('tasks.tasks.do_task', args=[{"job_ticket_id":"20230424A","jstorforum":True,"harvestset":"811","harvestdate":"2023-04-18", "until":"2023-04-20"}], kwargs={}, queue="harvest_jstorforum")
