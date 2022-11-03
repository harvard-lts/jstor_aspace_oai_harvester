
import os, click, sys
from flask import Flask, current_app
from flask.cli import with_appcontext
# Import custom modules from the local project
from . import jstor_harvester
# Import API resources
from . import resources

# App factory
def create_app():
    # Create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    # App config
    app.config.from_mapping(
        ROOT_ROUTE = '/jstor_harvester'
    )
    # App logger
    app.logger.setLevel(os.environ.get('APP_LOG_LEVEL', 'INFO'))

    # App context
    with app.app_context():

        # Resources
        resources.define_resources(app)

        return app
