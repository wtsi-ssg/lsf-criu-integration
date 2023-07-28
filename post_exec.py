#!/path/to/venv/bin/python

"""
SUMMARY:
    This script runs after a job has exited, and if it was overrunning and was
    checkpointed, then it will restart the job.


USERS:
    This script will be run by normal users through LSF, with their normal
    privileges and capabilities.


DOCUMENTATION:
    admin facing:
    https://ssg-confluence.internal.sanger.ac.uk/pages/viewpage.action?pageId=153044333

    user facing:
    https://ssg-confluence.internal.sanger.ac.uk/pages/viewpage.action?pageId=153053165
"""

from pythonlsf import lsf
import logging
import os
import sys
import json
import subprocess
import datetime
import pathlib

CLUSTER_NAME = lsf.ls_getclustername()
JOB_ID = os.environ['LSB_JOBID']


def fmt_runtime(runtime):
    tdelta = datetime.timedelta(seconds=runtime)
    hours, rem = divmod(tdelta.seconds, 3600)
    mins, secs = divmod(rem, 60)
    return f"{tdelta.days} days, {hours} hours, {mins} minutes, {secs} seconds"


def generate_summary(json_filepath: str) -> (str, str):
    output_str = ""
    try:
        with open(json_filepath, 'r') as f:
            raw_json = f.read()
    except Exception as err:
        output_str = (f"could not open file to generate summary, error: {err}."
                      " Please log a ticket with ISG if this persists, via "
                      "emailing <servicedesk@sanger.ac.uk> with FAO ISG in the"
                      " title. Please include the error and the job ID in the "
                      " email.\n")
        return (output_str, None)
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError as err:
        output_str = ("could not decode JSON to generate summary, it may be "
                      f"corrupted. error: {err}. Please log a ticket with ISG "
                      "if this persists, via emailing "
                      "<servicedesk@sanger.ac.uk> with FAO ISG in the title. "
                      "Please include this message and the job ID in the email.\n")
        return (output_str, None)

    summary_json = data['summary_info']

    output_str += ("Checkpoint / restore summary for overrunning job, which "
                   f"originally had an ID of: {summary_json[0]['id']}\n")

    output_str += "Job information for restarted jobs:\n"
    total_runtime = 0
    for job in summary_json:
        start_time = datetime.datetime.fromtimestamp(job['start_time'])
        output_str += (f"Job {job['id']} ran for {fmt_runtime(job['runtime'])}"
                       f" and started at {start_time}\n")
        total_runtime += job['runtime']

    output_str += "\n********** Aggregated Stats **********\n"
    output_str += f"Total runtime: {fmt_runtime(total_runtime)}\n"
    output_str += f"Restarts due to overrunning: {data['overrun_restarts']}\n"

    return (output_str, summary_json[0]['id'])


def has_chkpnt_dir() -> bool:
    return 'LSB_CHKPNT_DIR' in os.environ


def get_data_dump_path() -> str:
    """
        Return the path this job should use for writing the checkpoint state

        something like CHECKPOINT_DIR/.checkpoint-state-j{JOB_ID}.json
    """
    return (f'{os.environ["LSB_CHKPNT_DIR"]}'
            f'/.checkpoint-state.json')


def get_data() -> dict | None:
    path = get_data_dump_path()
    logging.debug(f"reading checkpoint state info from: {path}")
    try:
        with open(path, 'r') as f:
            raw_data = f.read()
    except FileNotFoundError:
        logging.debug("json data does not exist, job was not overrunning "
                      "checkpointed, doing nothing.")
        return None
    except Exception as err:
        logging.debug(f"unexpected error: {err}")
        return None

    logging.debug(f"raw checkpoint data read in: {raw_data}")

    try:
        data = json.loads(raw_data)
    except json.JSONDecodeError as err:
        logging.error(f"loaded {path}, but an error occured while parsing the "
                      "JSON data, the file must be corrupted. Error given: "
                      f"{err}")
        return None
    except Exception as err:
        logging.debug(f"unexpected error: {err}")
        return None

    if data['version'] == 'V1':
        logging.debug("loading data using v1 json")
        return data

    logging.error("could not load data, unknown version")
    return None


if __name__ == "__main__":
    print("execution of post exec beginning")
    if not has_chkpnt_dir():
        sys.exit(1)

    logging.basicConfig(format=f"%(levelname)s: %(asctime)s (job  {JOB_ID}):"
                               " %(message)s",
                        datefmt="%Y/%m/%d %H:%M:%S",
                        filename=(f"{os.environ['LSB_CHKPNT_DIR']}/"
                                  f"post-exec-{JOB_ID}.log"),
                        level=logging.DEBUG,
                        encoding='utf-8')

    job_data = get_data()
    if job_data is None:
        sys.exit(0)

    logging.debug(f"checkpoint state info successfully read in: {job_data}")

    if (job_data['chkpnt_state'] == 0x8 and job_data['should_restart'] and
            job_data['job_id'] == int(os.environ['LSB_JOBID'])):
        logging.debug("should restart job")
        path = pathlib.PurePath(os.environ['LSB_CHKPNT_DIR'])
        path = path.parent
        cmd = f". /usr/local/lsf/conf/profile.lsf; brestart {path} {JOB_ID}"
        logging.debug(cmd)
        out = subprocess.run(cmd, shell=True, capture_output=True)
        logging.debug(f"brestart cmd output: {out}")
    else:
        logging.debug("should not restart job")
        if os.environ['LSB_OUTPUTFILE'] == "/dev/null":
            logging.info("output is /dev/null, not writing a summary.")
            sys.exit(0)
        else:
            (summary, job_id) = generate_summary(get_data_dump_path())

            splits = os.environ['LSB_OUTPUTFILE'].rpartition('/')
            if job_id is None:
                output_file = splits[0] + '/' + 'overrun-summary-ERR.txt'
            else:
                output_file = splits[0] + '/' + f'overrun-summary-{job_id}.txt'

            try:
                with open(output_file, 'w') as f:
                    f.write(summary)
            except OSError as err:
                logging.error(f"could not write summary, error: {err}")

    logging.debug("finish post_exec")
