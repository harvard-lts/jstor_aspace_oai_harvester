import sys, os, os.path, json, requests, traceback, time
from tenacity import retry, retry_if_result, wait_random_exponential, retry_if_not_exception_type
from flask import Flask, request, jsonify, current_app, make_response
from random import randint
from time import sleep
from pymongo import MongoClient
from sickle import Sickle
from datetime import date, timedelta, datetime

class JstorHarvester():
    def __init__(self):
        self.child_running_jobs = []
        self.child_error_jobs = []
        self.child_success_jobs = []
        self.parent_job_ticket_id = None
        self.child_error_count = 0
        self.max_child_errors = int(os.getenv("CHILD_ERROR_LIMIT", 10))
        self.repositories = self.load_repositories()

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

        job_ticket_id = str(request_json['job_ticket_id'])
        if job_ticket_id == None:
            job_ticket_id = '999999999'
        
        #dump json
        current_app.logger.info("json message: " + json.dumps(request_json))

        harvestdate_datetime = date.today() - timedelta(days = 1)
        harvestdate = harvestdate_datetime.strftime('%Y-%m-%d')
        if 'harvesttype' in request_json:
            if request_json["harvesttype"] == "full":
                harvestdate = None
        elif 'harvestdate' in request_json:
            #to do: check format and throw error if not YYYY-MM-DD
            harvestdate = request_json["harvestdate"]

        harvestset = None
        if 'harvestset' in request_json:
            harvestset = request_json["harvestset"]

        jstorforum = False
        if 'jstorforum' in request_json:
            current_app.logger.info("running jstorforum harvest")
            jstorforum = request_json['jstorforum']
        if jstorforum:
            self.do_harvest('jstorforum', harvestdate, harvestset, "harvestjobs.json", job_ticket_id)

        aspace = False
        if 'aspace' in request_json:
            current_app.logger.info("running aspace harvest")
            aspace = request_json['aspace']
        if aspace:
            self.do_harvest('aspace', harvestdate, None,  "harvestjobs.json", job_ticket_id)

        #integration test: write small record to mongo to prove connectivity
        integration_test = False
        if ('integration_test' in request_json):
            integration_test = request_json['integration_test']
        if (integration_test):
            current_app.logger.info("running integration mongo test")
            self.do_harvest('jstorforum', None, None,  "harvestjobs_test.json", job_ticket_id)
            # a second call for testing exception and mongo error
            self.do_harvest('jstorforum', None, None,  "harvestjobs_test.json", job_ticket_id + "_error", True)
            #to do - make aspace date configurable, from and until
            self.do_harvest('aspace', '2023-02-06', None,  "harvestjobs_test.json", job_ticket_id)
            try:
                mongo_url = os.environ.get('MONGO_URL')
                mongo_dbname = os.environ.get('MONGO_DBNAME')
                mongo_collection = os.environ.get('MONGO_COLLECTION_ITEST')
                mongo_client = MongoClient(mongo_url, maxPoolSize=1)

                mongo_db = mongo_client[mongo_dbname]
                integration_collection = mongo_db[mongo_collection]
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

    def do_harvest(self, jobname, harvestdate, harvestset, configfile, job_ticket_id, errortest=False):

        with open(configfile) as f:
            harvjobsjson = f.read()
        harvestconfig = json.loads(harvjobsjson)
        #current_app.logger.debug("harvestconfig")        
        #current_app.logger.debug(harvestconfig)
        mongo_url = os.environ.get('MONGO_URL')
        mongo_dbname = os.environ.get('MONGO_DBNAME')
        harvest_collection_name = os.environ.get('HARVEST_COLLECTION', 'jstor_harvested_summary')
        repository_collection_name = os.environ.get('REPOSITORY_COLLECTION', 'jstor_repositories')
        record_collection_name = os.environ.get('JSTOR_HARVESTED_RECORDS', 'jstor_harvested_records')
        mongo_url = os.environ.get('MONGO_URL')
        mongo_client = None
        mongo_db = None

        check_last_successful = False
        if os.environ.get('CHECK_LAST_SUCCESSFUL') == 'true':
            check_last_successful = True

        try:
            mongo_client = MongoClient(mongo_url, maxPoolSize=1)
            mongo_db = mongo_client[mongo_dbname]
        except Exception as err:
            current_app.logger.error("Error: unable to connect to mongodb, {}", err)
                                                                                              

        harvestDir = os.getenv("jstor_harvest_dir") + "/"        
        for job in harvestconfig:     
            if jobname == 'jstorforum' and jobname == job["jobName"]:  
                for set in job["harvests"]["sets"]:
                    setSpec = "{}".format(set["setSpec"])
                    repository_name = self.repositories[setSpec]["displayname"]
                    repo_short_name = self.repositories[setSpec]["shortname"]
                    opDir = set["opDir"]
                    totalHarvestCount = 0
                    harvest_successful = True

                    if harvestset is None:
                        if not os.path.exists(harvestDir + opDir + "_oaiwrapped"):
                            os.makedirs(harvestDir + opDir + "_oaiwrapped")
                        current_app.logger.info("Harvesting set:" + setSpec + ", output dir: " + opDir)
                        sickle = Sickle(os.getenv("jstor_oai_url"))
                        try:
                            if errortest:
                                sickle = Sickle(os.getenv("oai_error_url"))
                            if harvestdate == None: # must be a full harvest
                                records = sickle.ListRecords(metadataPrefix='oai_ssio', set=setSpec)
                            else:
                                if check_last_successful:
                                    coll= mongo_db[harvest_collection_name]
                                    rec = coll.find({"repository_id":setSpec, "success":True}, {"harvest_date":1}).sort("harvest_date", -1).limit(1)
                                    if rec is not None:
                                        lastsuccessfuldate = rec[0]['harvest_date'] 
                                        if harvestdate.strftime('%Y-%m-%d') > lastsuccessfuldate.strftime('%Y-%m-%d'):
                                            current_app.logger.info("Trying reharvest from: " + lastsuccessfuldate.strftime('%Y-%m-%d'))
                                            harvestdate = lastsuccessfuldate.strftime('%Y-%m-%d')

                                records = sickle.ListRecords(**{'metadataPrefix':'oai_ssio', 'from':harvestdate, 'set':setSpec}) 
                                
                            for item in records:
                                current_app.logger.info(item.header.identifier)
                                with open(harvestDir + opDir + "_oaiwrapped/" + item.header.identifier + ".xml", "w") as f:
                                    f.write(item.raw)
                                try:
                                    status = "harvested"
                                    self.write_record(job_ticket_id, item.header.identifier, harvestdate, setSpec, repository_name, 
                                        repo_short_name, status, record_collection_name, True, mongo_db)
                                    totalHarvestCount = totalHarvestCount + 1    
                                except Exception as e:
                                    current_app.logger.error(e)
                                    current_app.logger.error("Mongo error writing " + setSpec + " record: " +  item.header.identifier)
                        except Exception as e:
                            #to do: use narrower exception for NoRecordsMatch
                            current_app.logger.info(e)
                            if str(e) == "No Records Match":
                                current_app.logger.info("No records for: " + setSpec + ", output dir: " + opDir)
                            else:
                                current_app.logger.info("Error harvesting")
                                harvest_successful = False  
                        try:
                            self.write_harvest(job_ticket_id, harvestdate, setSpec, 
                                repository_name, repo_short_name, totalHarvestCount, harvest_collection_name, mongo_db, jobname, harvest_successful)
                        except Exception as e:
                            current_app.logger.error(e)
                            current_app.logger.error("Mongo error writing harvest record for : " +  setSpec)        

                                
                    elif  setSpec == harvestset:
                        current_app.logger.info("Harvesting for one set only: " + setSpec)        
                        if not os.path.exists(harvestDir + opDir + "_oaiwrapped"):
                            os.makedirs(harvestDir + opDir + "_oaiwrapped")
                        current_app.logger.info("Harvesting set:" + setSpec + ", output dir: " + opDir)
                        sickle = Sickle(os.getenv("jstor_oai_url"))
                        try:
                            if harvestdate == None: # must be a full harvest
                                records = sickle.ListRecords(metadataPrefix='oai_ssio', set=setSpec)
                            else:
                                current_app.logger.info(check_last_successful)
                                if check_last_successful:
                                    coll= mongo_db[harvest_collection_name]
                                    rec = coll.find({"repository_id":setSpec, "success":True}, {"harvest_date":1}).sort("harvest_date", -1).limit(1)
                                    if rec is not None:
                                        lastsuccessfuldate = rec[0]['harvest_date'] 
                                        if harvestdate.strftime('%Y-%m-%d') > lastsuccessfuldate.strftime('%Y-%m-%d'):
                                            current_app.logger.info("Trying reharvest from: " + lastsuccessfuldate.strftime('%Y-%m-%d'))
                                            harvestdate = lastsuccessfuldate.strftime('%Y-%m-%d') 
                                records = sickle.ListRecords(**{'metadataPrefix':'oai_ssio', 'from':harvestdate, 'set':setSpec}) 
                            for item in records:
                                current_app.logger.info(item.header.identifier)
                                with open(harvestDir + opDir + "_oaiwrapped/" + item.header.identifier + ".xml", "w") as f:
                                    f.write(item.raw)
                                try:
                                    status = "harvested"
                                    self.write_record(job_ticket_id, item.header.identifier, harvestdate, setSpec, repository_name, 
                                        repo_short_name, status, record_collection_name, True, mongo_db)
                                    totalHarvestCount = totalHarvestCount + 1    
                                except Exception as e:
                                    current_app.logger.error(e)
                                    current_app.logger.error("Mongo error writing " + setSpec + " record: " +  item.header.identifier)    
                        except Exception as e:
                            #to do: use narrower exception for NoRecordsMatch
                            current_app.logger.info(e)
                            if str(e) == "No Records Match":
                                current_app.logger.info("No records for: " + setSpec + ", output dir: " + opDir)
                            else:
                                current_app.logger.info("Error harvesting: " + str(e))
                                harvest_successful = False    
                        try:
                            self.write_harvest(job_ticket_id, harvestdate, setSpec, 
                                repository_name, repo_short_name, totalHarvestCount, harvest_collection_name, mongo_db, jobname, harvest_successful)
                        except Exception as e:
                            current_app.logger.error(e)
                            current_app.logger.error("Mongo error writing harvest record for : " +  setSpec)


            if jobname == 'aspace' and jobname == job["jobName"]:  
                ns = {'ead': 'urn:isbn:1-931666-22-9'}
                sickle = Sickle(os.getenv("aspace_oai_url"))
                if not os.path.exists(harvestDir + 'aspace'):
                    os.makedirs(harvestDir + 'aspace')
                totalAspaceHarvestCount = 0
                harvest_successful = True
                try:    
                    if harvestdate == None:    
                        records = sickle.ListRecords(metadataPrefix='oai_ead')
                    else:    
                        if configfile == 'harvestjobs_test.json':
                            harvest_date_obj = datetime.strptime(harvestdate, 
                                "%Y-%m-%d")  + timedelta(days = 1)
                            records = sickle.ListRecords(**{'metadataPrefix':'oai_ead', 'from':harvestdate, 
                                'until': harvest_date_obj.strftime("%Y-%m-%d") })
                        else:
                            records = sickle.ListRecords(**{'metadataPrefix':'oai_ead', 'from':harvestdate})
                    for item in records:
                        current_app.logger.info(item.header.identifier)
                        eadid = item.xml.xpath("//ead:eadid", namespaces=ns)[0].text

                        with open(harvestDir + "aspace/" + eadid + ".xml", "w") as f:
                            f.write(item.raw)
                        totalAspaceHarvestCount = totalAspaceHarvestCount + 1
                        #add record to mongo
                        try:
                            status = "harvested"
                            self.write_record(job_ticket_id, item.header.identifier, harvestdate, "0000", "aspace", "ASP",
                                status, record_collection_name, True, mongo_db)
                        except Exception as e:
                                current_app.logger.error(e)
                                current_app.logger.error("Mongo error writing aspace record: " + eadid)
                except Exception as e:
                    #to do: use narrower exception for NoRecordsMatch
                    current_app.logger.info(e)
                    current_app.logger.info("No records for aspace" )
                    harvest_successful = False 
                try:
                    self.write_harvest(job_ticket_id, harvestdate, "0000", "aspace", "ASP", 
                        totalAspaceHarvestCount, harvest_collection_name, mongo_db, "aspace", harvest_successful)
                except Exception as e:
                    current_app.logger.error(e)
                    current_app.logger.error("Mongo error writing harvest record for : aspace")

        if (mongo_client is not None):            
            mongo_client.close()

    def write_harvest(self, harvest_id, harvest_date, repository_id, repository_name, repo_short_name, 
            total_harvested, collection_name, mongo_db, jobname, success):
        if mongo_db == None:
            current_app.logger.info("Error: mongo db not instantiated")
            return
        try:
            if harvest_date == None: #set harvest date to today if harvest date is None
                harvest_date = datetime.today().strftime('%Y-%m-%d') 
            harvest_date_obj = datetime.strptime(harvest_date, "%Y-%m-%d")
            last_update = datetime.now()
            harvest_record = { "id": harvest_id, "last_update": last_update, "harvest_date": harvest_date_obj, 
                "repository_id": repository_id, "repository_name": repository_name, "repo_short_name": repo_short_name, 
                "total_harvest_count": total_harvested, "jobname": jobname, "success": success }
            harvest_collection = mongo_db[collection_name]
            harvest_collection.insert_one(harvest_record)
            current_app.logger.info(repository_name + " harvest for " + harvest_date + " written to mongo ")
        except Exception as err:
            current_app.logger.info("Error: unable to connect to mongodb, {}", err)
        return

    def write_record(self, harvest_id, record_id, harvest_date, repository_id, repository_name, repo_short_name, 
            status, collection_name, success, mongo_db, error=None):
        err_msg = ""
        if mongo_db == None:
            current_app.logger.info("Error: mongo db not instantiated")
            return
        try:
            if error != None:
                err_msg = error
            if harvest_date == None: #set harvest date to today if harvest date is None
                harvest_date = datetime.today().strftime('%Y-%m-%d')  
            harvest_date_obj = datetime.strptime(harvest_date, "%Y-%m-%d")
            last_update = datetime.now()
            harvest_record = { "harvest_id": harvest_id, "last_update": last_update, "harvest_date": harvest_date_obj, "record_id": record_id, 
                "repository_id": repository_id, "repository_name": repository_name, "repo_short_name": repo_short_name, 
                "status": status, "success": success, "error": err_msg }
            record_collection = mongo_db[collection_name]
            record_collection.insert_one(harvest_record)
            current_app.logger.info("record " + str(record_id) + " of repo " + str(repository_id) + " written to mongo ")
        except Exception as err:
            current_app.logger.info("Error: unable to connect to mongodb, {}", err)
        return

    def load_repositories(self):
        repositories = {}
        try:
            mongo_url = os.environ.get('MONGO_URL')
            mongo_dbname = os.environ.get('MONGO_DBNAME')
            repository_collection_name = os.environ.get('REPOSITORY_COLLECTION', 'jstor_repositories')
            mongo_url = os.environ.get('MONGO_URL')
            mongo_dbname = os.environ.get('MONGO_DBNAME')
            mongo_client = MongoClient(mongo_url, maxPoolSize=1)

            mongo_db = mongo_client[mongo_dbname]
            repository_collection = mongo_db[repository_collection_name]
            repos = repository_collection.find({})
            for r in repos:
                k = r["_id"]
                v = { "displayname": r["displayname"], "shortname": r["shortname"] }
                repositories[k] = v 
            mongo_client.close()
            return repositories
        except Exception as err:
            current_app.logger.info("Error: unable to load repository table from mongodb, {}", err)
            return repositories

    #add more sophisticated healthchecking later
    def healthcheck(self):
        hc = "OK"
        return hc
        
    def revert_task(self, job_ticket_id, task_name):
        return True
