from flask_restx import Resource, fields, Api, reqparse
from flask import Flask, request, jsonify, current_app, make_response
import os
import requests
import celery

# Import worker
from . import jstor_harvester

parser = reqparse.RequestParser()
parser.add_argument("batchSize", type=int)

def define_resources(app):

    api = Api(app, version='1.0', title='JStor Harvester', description='JStor Harvester')
    # Resource definitions
    # Namespace
    iiif = api.namespace('jstor_harvester', description="Manages queues, dispatches tasks, keeps track of workers and restarts if necessary.")

    # Heartbeat/health check route
    @iiif.route('/version', endpoint="version")
    class Version(Resource):
        def get(self):
            version = os.environ.get('APP_VERSION', "NOT FOUND")
            return {"version": version}

    # Standard 'do_task' route that performs the task
    @iiif.route('/do_task', endpoint="do_task", methods=['POST'])
    class WorkerDoTask(Resource):

        def post(self, *args, **kwargs):
            worker = jstor_harvester.JstorHarvester()
            return worker.do_task(request.json)

    # Standard 'revert_task' route that reverts the task
    @iiif.route('/revert_task', endpoint="revert_task", methods=['POST'])
    class WorkerRevertTask(Resource):

        def post(self, *args, **kwargs):
            worker = jstor_harvester.JstorHarvester()
            return worker.revert_task(request.json)

    # Route for adding-to-queue testing purposes, not a part of solution
    @iiif.route('/celery', endpoint='celery', methods=['GET'])
    class CeleryTest(Resource):
        @api.expect(parser)
        def get(self, *args, **kwargs):
            batchSize = parser.parse_args()['batchSize'] if 'batchSize' in parser.parse_args() else 100
            current_app.logger.info("batchSize " + batchSize)
            for item in range(batchSize):
                ticket_id = 'job: '+ str(item)
                celery.execute.send_task("tasks.tasks.do_task", args=[{'job_ticket_id': ticket_id}], kwargs={}, queue=os.getenv('QUEUE_NAME'))
            return str(batchSize) + " jobs started..." + str(type(batchSize))

    # Heartbeat/health check route  
    @iiif.route('/healthcheck', endpoint="healthcheck")
    class Healthcheck(Resource):
        def get(self):
            worker = jstor_harvester.JstorHarvester()
            healthcheck = worker.healthcheck()
            return {"system": healthcheck}