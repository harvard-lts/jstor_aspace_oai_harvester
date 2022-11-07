from celery import Celery

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

# Send a simple task (create and send in 1 step)
res = app1.send_task('tasks.tasks.do_task', args=[{"job_ticket_id":"123","hello":"world"}], kwargs={}, queue="michael_first_queue")
#print('%r: Got %s' %(res, res.get(timeout=5)))

'''
# Better: send a task using a uniform delay/get pattern.
# Essentially, it creates a subtask i.e a task signature
t = app1.signature('foo.adda', args=(1,8,2,-3,5))
res = t.delay()
print '%r: Got %s' %(res, res.get(timeout=5))

# Send a long-running task
res = app1.send_task('foo.waste_time', args=(5,))
print '%r: This will waste some time' %(res)
'''
