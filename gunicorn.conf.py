from datetime import datetime
import logging
import os
import re
import socket
import structlog

class RequestPathFilter(logging.Filter):
    '''Filter class for exempting paths from access log'''
    def __init__(self, *args, path_re, **kwargs):
        super().__init__(*args, **kwargs)
        self.path_filter = re.compile(path_re)

    def filter(self, record):
        req_path = record.args['U']
        return not self.path_filter.match(req_path)

# Hook run at start of gunicorn server process
def on_starting(server):
    # omit healthcheck URL from access logging
    server.log.access_log.addFilter(RequestPathFilter(path_re=r'^/jstor_harvester/version'))

# Reload
if os.environ.get('ENV') == 'development':
    reload = True

# Pre-processing
pre_chain = [
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
    structlog.processors.UnicodeDecoder(),
    structlog.processors.TimeStamper(fmt='iso', utc=True),
]

# Create a log folder for this container if it doesn't exist
container_id = socket.gethostname()
if not os.path.exists(f'/home/jstorforumadm/logs/jstor_harvester/{container_id}'):
    os.makedirs(f'/home/jstorforumadm/logs/jstor_harvester/{container_id}')

# Get timestamp
timestamp = datetime.today().strftime('%Y-%m-%d')

# Log config
logconfig_dict = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "json_formatter": {
            "()": structlog.stdlib.ProcessorFormatter,
            "processor": structlog.processors.JSONRenderer(),
            "foreign_pre_chain": pre_chain,
        }
    },
    "handlers": {
        "error_console": {
            "class": "logging.FileHandler",
            "formatter": "json_formatter",
            "filename": f"/home/jstorforumadm/logs/jstor_harvester/{container_id}/error_console_{container_id}_{timestamp}.log",
            "mode": "a"
        },
        "console": {
            "class": "logging.FileHandler",
            "formatter": "json_formatter",
            "filename": f"/home/jstorforumadm/logs/jstor_harvester/{container_id}/console_{container_id}_{timestamp}.log",
            "mode": "a"
        }
    },
    "loggers": {
        'gunicorn.error': {
            'handlers': ['console'],
            'level': os.environ.get('APP_LOG_LEVEL', 'INFO'),
            'propagate': False,
        },
        'gunicorn.access': {
            'handlers': ['console'],
            'level': os.environ.get('APP_LOG_LEVEL', 'INFO'),
            'propagate': False,
        }
    }
}
