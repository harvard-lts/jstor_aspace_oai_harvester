# jstor_aspace_oai_harvester

### To run and test component:
- clone repository
- cp .env.example to .env
- cp celeryconfig.py.example to celeryconfig.py and put in credentials
- make sure logs/jstor_harvester directory exists (need to fix)
- bring up docker
  - docker-compose -f docker-compose-local.yml up --build -d --force-recreate
- Send a message to the queue, edit tests/invoke_task with desired message, or uncomment one of the exmample lines
  - if needed install celery for the test script > pip install -U celery
  - python3 ./tests/invoke-task.py
  - this kicks off the pipeline

- Pytests (one pytest)
  - `> pytest`
  - (alternatively, if you don't want to install pytest locally, exec -it <containerid> bash, then run pytest)

  - to run this test there must be a test mongo collection named test_jstor_harvested_summary in dev, with .env using dev credentials 
  - test collection must include data in ./tests/data/test_jstor_harvested_summary.json

