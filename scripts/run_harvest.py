from celery import Celery
import argparse
import json
import re

app1 = Celery('tasks')
app1.config_from_object('celeryconfig')

# List of valid setSpecs
setList = [
706, 709, 711, 712, 713, 714, 715, 716, 717, 718, 719,
720, 721, 722, 812, 821, 822, 823, 824, 710, 732, 733,
735, 736, 737, 734, 739, 740, 741, 742, 809, 810, 811,
813, 814, 815, 816, 817, 818, 819, 745, 738, 998, 2968
]

# Define the argument parser
parser = argparse.ArgumentParser(description='Example script that demonstrates how to use argparse to build a dictionary and convert it to a JSON-formatted string.')

# Add some arguments
parser.add_argument('--jobid', type=str, required = True, help='id for the job, such as 20230428_SCH')
parser.add_argument('--harvestset', type=int, required = True, choices=setList, help='the setSpec for the repository')
parser.add_argument('--harvesttype', type=str, choices=['full'], help='the harvesttype is full')
parser.add_argument('--fromdate', type=str, help='the start date for harvest')
parser.add_argument('--untildate', type=str, help='the end date for harvest')


# Parse the arguments
args = parser.parse_args()

# Build up a dictionary based on the parsed arguments
message_dict = {}
message_dict['jstorforum'] = True
message_dict['job_ticket_id'] = args.jobid
message_dict['harvestset'] = str(args.harvestset)
if args.harvesttype:
    if args.fromdate:
        parser.error('please use fromdate and/or untildate or harvesttype, not date(s) and harvesttype"')
    if args.untildate:
        parser.error('please use fromdate and/or untildate or harvesttype, not date(s) and harvesttype"')
    message_dict['harvesttype'] = args.harvesttype
if args.fromdate:
    if re.match(r'^\d{4}-\d{2}-\d{2}$', args.fromdate):
        message_dict['harvestdate'] = args.fromdate
    else:
        parser.error('fromdate must be a date with format of "YYYY-MM-DD"')
if args.untildate:
    if re.match(r'^\d{4}-\d{2}-\d{2}$', args.untildate):
        if args.fromdate:
            if args.untildate < args.fromdate:
                parser.error("untildate must be later than fromdate")
        message_dict['untildate'] = args.untildate
    else:
        parser.error('untildate must be a date with format of "YYYY-MM-DD"')

# Print the resulting string
res = app1.send_task('tasks.tasks.do_task', args=[message_dict], kwargs={}, queue="harvest_jstorforum")

print(message_dict)

