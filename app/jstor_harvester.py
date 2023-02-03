import sys, os, os.path, json, requests, traceback, time
from tenacity import retry, retry_if_result, wait_random_exponential, retry_if_not_exception_type
from datetime import datetime
from flask import Flask, request, jsonify, current_app, make_response
from random import randint
from time import sleep
from pymongo import MongoClient
from sickle import Sickle
#import lxml.etree as ET

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

        result = {
          'success': False,
          'error': None,
          'message': ''
        }

        #Get the job ticket which should be the parent ticket
        current_app.logger.info("**************JStor Harvester: Do Task**************")
        current_app.logger.info("WORKER NUMBER " + str(os.getenv('CONTAINER_NUMBER')))

        sleep_s = int(os.getenv("TASK_SLEEP_S", 1))

        current_app.logger.info("Sleep " + str(sleep_s) + " seconds")
        sleep(sleep_s)

        jstorforum = False
        if 'jstorforum' in request_json:
            current_app.logger.info("running jstorforum harvest")
            jstorforum = request_json['jstorforum']
        if jstorforum:
            self.do_harvest('jstorforum')

        aspace = False
        if 'aspace' in request_json:
            current_app.logger.info("running aspace harvest")
            aspace = request_json['aspace']
        if aspace:
            self.do_harvest('aspace')

        #integration test: write small record to mongo to prove connectivity
        integration_test = False
        if ('integration_test' in request_json):
            integration_test = request_json['integration_test']
        if (integration_test):
            current_app.logger.info("running integration test")
            self.do_harvest('jstorforum', True)
            try:
                mongo_url = os.environ.get('MONGO_URL')
                mongo_dbname = os.environ.get('MONGO_DBNAME')
                mongo_collection = os.environ.get('MONGO_COLLECTION_ITEST')
                mongo_client = MongoClient(mongo_url, maxPoolSize=1)

                mongo_db = mongo_client[mongo_dbname]
                integration_collection = mongo_db[mongo_collection]
                job_ticket_id = str(request_json['job_ticket_id'])
                test_id = "harvester-" + job_ticket_id
                test_record = { "id": test_id, "status": "inserted" }
                integration_collection.insert_one(test_record)
                mongo_client.close()
            except Exception as err:
                current_app.logger.error("Error: unable to connect to mongodb, {}", err)
                
        result['success'] = True
        # altered line so we can see request json coming through properly
        result['message'] = 'Job ticket id {} has completed '.format(request_json['job_ticket_id'])
        return result

    def do_harvest(self, jobname, itest=False):
        if itest:
            configfile = "harvestjobs_test.json"
        else:
            configfile = "harvestjobs.json"
        current_app.logger.info("configfile: " + configfile)

        with open(configfile) as f:
            harvjobsjson = f.read()
        harvestconfig = json.loads(harvjobsjson)
        #current_app.logger.debug("harvestconfig")        
        #current_app.logger.debug(harvestconfig) 
        harvestDir = os.getenv("jstor_harvest_dir")        
        for job in harvestconfig:     
            if jobname == 'jstorforum' and jobname == job["jobName"]:   
                for set in job["harvests"]["sets"]:
                    setSpec = "{}".format(set["setSpec"])
                    opDir = set["opDir"]
                    if not os.path.exists(harvestDir + opDir):
                        os.makedirs(harvestDir + opDir)
                    current_app.logger.info("Harvesting set:" + setSpec + ", output dir: " + opDir)
                    sickle = Sickle(os.getenv("jstor_oai_url"))
                    records = sickle.ListRecords(metadataPrefix='oai_ssio', set=setSpec)
                    for item in records:
                        current_app.logger.info(item.header.identifier)
                        with open(harvestDir + opDir + "/" + item.header.identifier + ".xml", "w") as f:
                            f.write(item.raw)
            if jobname == 'aspace' and jobname == job["jobName"]:  
                ns = {'ead': 'urn:isbn:1-931666-22-9'}
                sickle = Sickle(os.getenv("aspace_oai_url"))
                if not os.path.exists(harvestDir + 'aspace'):
                    os.makedirs(harvestDir + 'aspace')
                records = sickle.ListRecords(**{'metadataPrefix':'oai_ead', 'from':'2023-02-03'})
                for item in records:
                    current_app.logger.info(item.header.identifier)
                    current_app.logger.info(type(item.xml))
                    eadid = item.xml.xpath("//ead:eadid", namespaces=ns)[0].text
                    #with open(harvestDir + "aspace/" + item.header.identifier + ".xml", "w") as f:
                    with open(harvestDir + "aspace/" + eadid + ".xml", "w") as f:
                        f.write(item.raw)

    def revert_task(self, job_ticket_id, task_name):
        return True
