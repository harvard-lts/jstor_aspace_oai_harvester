import sys, os, os.path, json, requests, traceback, time
from tenacity import retry, retry_if_result, wait_random_exponential, retry_if_not_exception_type
from datetime import datetime
from flask import Flask, request, jsonify, current_app, make_response
from random import randint
from time import sleep

class JstorHarvester():
    def __init__(self):
        self.child_running_jobs = []
        self.child_error_jobs = []
        self.child_success_jobs = []
        self.parent_job_ticket_id = None
        self.child_error_count = 0
        self.max_child_errors = int(os.getenv("CHILD_ERROR_LIMIT", 10))

    # Write to error log update result and update job tracker file
    def handle_errors(self, result, error_msg, exception_msg = None, set_job_failed = False):
        exception_msg = str(exception_msg)
        current_app.logger.error(exception_msg)
        current_app.logger.error(error_msg)
        result['error'] = error_msg
        result['message'] = exception_msg
        # Append error to job tracker file errors_encountered list
        if self.parent_job_ticket_id:
            job_tracker.append_error(self.parent_job_ticket_id, error_msg, exception_msg, set_job_failed)

        return result

    def do_task(self, request_json):
        """\
Get job tracker file
Append job ticket id to jobs in process list in the tracker file
Update job timestamp file"""

        result = {
          'success': False,
          'error': None,
          'message': ''
        }

        #Get the job ticket which should be the parent ticket
        current_app.logger.error("**************JStor Harvester: Do Task**************")
        current_app.logger.error("WORKER NUMBER " + str(os.getenv('CONTAINER_NUMBER')))

        result['success'] = True
        # altered line so we can see request json coming through properly
        result['message'] = 'Job ticket id {} has completed '.format(request_json['job_ticket_id'])

        sleep_s = os.getenv("TASK_SLEEP_S", 1)

        current_app.logger.info("Sleep " + str(sleep_s) + "seconds")
        sleep(sleep_s)
        
        return result

    def revert_task(self, job_ticket_id, task_name):
        return True
