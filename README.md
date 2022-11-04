# jstor_aspace_oai_harvester

### To run and test component:
- clone repository
- cp .env.example to .env
- cp celeryconfig.py.example to celeryconfig.py and put in credentials
- make sure logs/jstor_forum directory exists (need to fix)
- bring up docker
- - docker-compose -f docker-compose-local.yml up --build -d --force-recreate
- run test script to send hello world message to first queue
- - if needed install celery for the test script > pip install -U celery
- - python3 ./tests/invoke-task.py
- view rabbitmq ui, watch hello world message progress to second_queue (you can view the messge)
