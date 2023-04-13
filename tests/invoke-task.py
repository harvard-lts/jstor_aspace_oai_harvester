from celery import Celery

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

# Send a simple task (create and send in 1 step)
#res = app1.send_task('tasks.tasks.do_task', args=[{"job_ticket_id":"123","jstorforum":True,"harvestset":"713","harvesttype":"full"}], kwargs={}, queue="harvest_jstorforum")
#res = app1.send_task('tasks.tasks.do_task', args=[{"job_ticket_id":"20230407","jstorforum":True, "harvestdate":"2023-04-07"}], kwargs={}, queue="harvest_jstorforum")
#res = app1.send_task('tasks.tasks.do_task', args=[{"job_ticket_id":"20230410","aspace":True, "harvestdate":"2023-04-10"}], kwargs={}, queue="harvest_jstorforum")
#res = app1.send_task('tasks.tasks.do_task', args=[{"job_ticket_id":"20230411","jstorforum":True, "harvestset":"720", "harvesttype":"full"}], kwargs={}, queue="harvest_jstorforum")
res = app1.send_task('tasks.tasks.do_task', args=[{"job_ticket_id":"20230413A","jstorforum":True, "harvestset":"720"}], kwargs={}, queue="harvest_jstorforum")
#res = app1.send_task('tasks.tasks.do_task', args=[{"job_ticket_id":"20230412","jstorforum":True, "harvestdate":"2023-04-12"}], kwargs={}, queue="harvest_jstorforum")
