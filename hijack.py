#!/path/to/venv/bin/python

"""
SUMMARY:
    This script is intended to perform checks on terminating jobs to determine
    whether they are overrunning, have overunning checkpointing enabled and
    a few other checks. If so it will jump through the required hoops to
    checkpoint and restart the job.


USERS:
    This script will be run by normal users through LSF, with their normal
    privileges and capabilities.


DOCUMENTATION:
    admin facing:
    https://ssg-confluence.internal.sanger.ac.uk/pages/viewpage.action?pageId=153044333

    user facing:
    https://ssg-confluence.internal.sanger.ac.uk/pages/viewpage.action?pageId=153053165

EXIT VALUES:
    0 - job has been killed or checkpointed successfully
    1 - job is still alive at the end of the script or this script has errored
    2 - waiting for some state change that requires leaving this script
"""

from enum import Enum
from pythonlsf import lsf
import logging
import os
import sys
import subprocess
import time
import json

CLUSTER_NAME = lsf.ls_getclustername()

# really unlikely this would ever happen, but the check is here in case someone
# tries to run this script interactively.
try:
    JOB_ID = os.environ['LSB_JOBID']
except KeyError:
    print("could not get environment variable LSB_JOBID, likely not executed "
          "from an LSF JOB_CONTROLS script.", file=sys.stderr)
    sys.exit(1)


# maximum amount of times a job can automatically restart after overrunning
# N.B. This is restarts, not runs. The amount of runs is this number + 1
# default it to 2 restarts, but a user can override this by setting the
# environment variable
ORUN_RESTART_LIMIT = int(os.environ.get('ORUN_RESTART_LIMIT', default=2))

# max wall time allowed from 1st start of this script to chkpnt in seconds
TIMEOUT = int(os.environ.get('ORUN_TIMEOUT_SECONDS', default=180))

CHKPNT_JSON_VERSION = 'V1'


class CheckpointState(Enum):
    NONE = 0x1      # begin checkpointing
    STARTED = 0x2   # do nothing
    FAILED = 0x4    # checkpointing failed, kill the job
    SUCCESS = 0x8   # checkpointing succeeded, exit with an exit value of 0


class CheckpointData():
    version: str
    job_id: int
    timestamp: int
    chkpnt_state: CheckpointState
    overrun_restarts: int
    should_restart: bool
    summary_info: list[dict]

    def __init__(self, job_id: int, timestamp: int,
                 chkpnt_state: CheckpointState, overrun_restarts: int,
                 should_restart: bool, summary_info: list[dict],
                 version: str) -> None:
        self.version = version
        self.job_id = job_id
        self.timestamp = timestamp
        self.chkpnt_state = chkpnt_state
        self.overrun_restarts = overrun_restarts
        self.should_restart = should_restart
        self.summary_info = summary_info

    def update_and_dump_json(self) -> str:
        """
        This function takes the current checkpointing state, updates it and
        then returns a JSON string representing it. the updates that happen:

        job_id is set to the current job's ID, replacing the previous job's ID.

        timestamp is a simple timestamp in seconds since epoch.

        should_restart tells the post_exec script whether or not we want to
        restart the job, if possible.

        overrun_restarts is incremented if we want to restart, if not it is
        irrelevant so there is no need to increment it.

        summary_info is a list of dicts that keep info on the previous restarts
        of this job, in order to generate a summary of what it did at the end.

        d is a dictionary of the attributes of the CheckpointData class. We do
        this so that we can set chkpnt_state to a simple number rather than
        an enum.

        the chkpnt_state key/value pair is set to a number rather than an enum
        so that it can be serialised and deserialised more easily.

        example:
            {
              "job_id": 1022,
              "timestamp": 1689766404,
              "chkpnt_state": 1,
              "overrun_restarts": 2,
              "should_restart": true,
              "summary_info": [
                {
                  "id": 1021,
                  "start_time": 1689765619,
                  "runtime": 115
                },
              ]
            }
        becomes something like
            {
              "job_id": 1023,
              "timestamp": 1689766604,
              "chkpnt_state": 1,
              "overrun_restarts": 3,
              "should_restart": true,
              "summary_info": [
                {
                  "id": 1021,
                  "start_time": 1689765619,
                  "runtime": 115
                },
                {
                  "id": 1022,
                  "start_time": 1689766610,
                  "runtime": 125
                },
              ]
            }
        """
        self.job_id = int(JOB_ID)
        self.timestamp = int(time.time())
        self.should_restart = (self.overrun_restarts < ORUN_RESTART_LIMIT
                               and env_var_matches('ENABLE_ORUN_RESTART', 'Y'))
        if self.should_restart:
            self.overrun_restarts += 1

        jobinfo = get_job_info(int(JOB_ID))
        self.summary_info.append({'id': self.job_id,
                                  'start_time': jobinfo.startTime,
                                  'runtime': jobinfo.runTime})
        d = self.__dict__
        # this is necessary to simplify dumping and loading of the json
        d['chkpnt_state'] = d['chkpnt_state'].value

        return json.dumps(d)


def env_var_exists(var) -> bool:
    """
        Return true if the variable `var` exists and is not blank
    """
    env_var = os.environ.get(var, default="")
    return env_var != ""


def env_var_matches(var, match) -> bool:
    """
        Return true if the variable `var` exists and matches the `match` string
    """
    env_var = os.environ.get(var)
    if env_var is None:
        return False

    return env_var == str(match)


def is_job_overrunning() -> bool:
    """
        Return true if the job is being terminated due to overrunning
    """
    return (env_var_matches('LSB_SUSP_REASONS', lsf.SUSP_RES_LIMIT) and
            env_var_matches('LSB_SUSP_SUBREASONS', lsf.SUB_REASON_RUNLIMIT))


def is_job_dead() -> bool:
    """
    Return true if the job is dead.

    Uses the PGID of the processes for this job to check if the whole process
    group is dead, and also checks all other PIDS tied to the job

    Done this way instead of talking to LSF because LSF is inconsistent and
    too slow in terms of when it updates process status for this to work.

    Returns:
        `True` if the job is dead, `False` otherwise
    """
    pgid = os.environ['LSB_JOBPGIDS'].strip()
    cmd_output = subprocess.run(['ps', '-p', pgid], capture_output=True)

    if cmd_output.returncode == 0:
        # pgid still exists, so surely the job must
        return False

    pids = os.environ['LSB_JOBPIDS'].split()

    for pid in pids:
        cmd = subprocess.run(['ps', '-p', pid], capture_output=True)
        if cmd.returncode == 0:  # this process exists
            return False

    logging.debug("job is dead")
    return True


def check_chkpnt_vars() -> bool:
    """
    Return True if the job has LSB_CHKPNT_DIR and LSB_ECHKPNT_METHOD set

    Uses the `LSB_CHKPNT_DIR` and `LSB_ECHKPNT_METHOD` environment variables
    set by LSF to verify a checkpoint directory has been set, and that the
    method of checkpointing is criu.

    There is no need to check access to the checkpoint directory, LSF will do
    that for us
    """
    logging.debug(f"LSB_CHKPNT_DIR={os.environ['LSB_CHKPNT_DIR']}, "
                  f"LSB_ECHKPNT_METHOD={os.environ['LSB_ECHKPNT_METHOD']}")
    if env_var_exists('LSB_CHKPNT_DIR'):
        return env_var_matches('LSB_ECHKPNT_METHOD', 'criu')

    return False


def emulate_bkill(write_log=True):
    """
    Emulates the regular LSF `bkill` command, since we cannot call it here.

    We cannot call the normal LSF `bkill` command since that will just call
    this script again, creating a deadlock, and thus bad things will happen.

    We send SIGINT then SIGTERM then SIGKILL because thats what LSF does, and
    users may have built things around expecting LSF to do that.
    """
    if check_chkpnt_vars() and write_log:
        data = load_chkpnt_state()
        if data is not None:
            data.should_restart = False
            dump_chkpnt_state(data)
    pgid = os.environ['LSB_JOBPGIDS'].strip()
    signals = ['INT', 'TERM', 'KILL']
    poll_tries = int(os.environ.get('ORUN_KILL_POLL_TRIES', 10))
    for s in signals:
        cmd = f"kill -{s} -{pgid}"
        logging.debug(f"sending kill command: {cmd}")
        subprocess.run(cmd, shell=True, capture_output=True)

        for i in range(poll_tries):
            if is_job_dead():
                logging.info(f"job killed by {s} signal")
                return
            time.sleep(1)  # small delay to allow job to clean up

        logging.debug(f"job is still alive after {s} signal")

    logging.error("job could not be killed by any signal - it may be stuck, "
                  "log a ticket with 'FAO ISG' in it by emailing "
                  "<servicedesk@sanger.ac.uk> if this persists.")


def checkpoint():
    """
    Starts the checkpointing process

    Note that LSF will not actually start the checkpoint until this script
    exits.
    """
    logging.debug("sending checkpoint command")

    lsf.lsb_chkpntjob(int(JOB_ID), 0, lsf.LSB_CHKPNT_KILL)


def get_job_info(jobid: int) -> lsf.jobInfoEnt:
    """
    Get the jobInfoEnt struct for a single lsf job using its jobId
    This function does not work for array jobs, only single jobs, but since we
    would not get to this point with a job array, its fine to use here.
    """
    # openjobinfo opens a connection to mbatchd and returns the amount of
    # jobs in the connection.
    num_jobs_found = lsf.lsb_openjobinfo(jobid, "", "all", "", "", 0x2000)

    # make and assign an int pointer to the record of the jobs found
    int_ptr = lsf.new_intp()
    lsf.intp_assign(int_ptr, num_jobs_found)

    # read the info at int_ptr and assign to a python object so we can read it.
    job_info = lsf.lsb_readjobinfo(int_ptr)

    # close the connection to avoid a memory leak
    lsf.lsb_closejobinfo()

    return job_info


def get_chkpnt_state_path() -> str | None:
    """
        Return the path this job should use for writing the checkpoint state

        a path like like CHECKPOINT_DIR/.checkpoint-state.json
    """
    if 'LSB_CHKPNT_DIR' in os.environ:
        return (f'{os.environ["LSB_CHKPNT_DIR"]}'
                '/.checkpoint-state.json')

    return None


def dump_chkpnt_state(current_data: CheckpointData):
    """
        Dumps JSON data about the state of checkpointing to the given filepath

        Updates the restart counter and appends to the summary info also.
    """
    path = get_chkpnt_state_path()
    if path is None:
        return
    logging.debug(f"dumping checkpoint state to: {path}")
    json_data = current_data.update_and_dump_json()

    with open(path, 'w') as f:
        logging.debug(f"attempting to dump chkpnt data: {json_data}")
        f.write(json_data)
        logging.debug(f"dumped {json_data} to {path}")


def initial_chkpnt_data() -> CheckpointData:
    """
        Generate and return some initial checkpoint data to be used.
    """
    data = CheckpointData(
        job_id=int(JOB_ID),
        timestamp=0,  # will be set when checkpoint starts
        chkpnt_state=CheckpointState.NONE,
        overrun_restarts=0,  # had it restarted, the json would exist
        should_restart=True,
        summary_info=[],
        version=CHKPNT_JSON_VERSION
    )
    return data


def load_chkpnt_state() -> CheckpointData | None:
    """
        Load checkpoint state either from a file, or generate some dummy state

        In the event of this being the first run of the job, it will generate
        dummy state, otherwise it will load the state from the previous run.
    """
    path = get_chkpnt_state_path()
    if path is None:
        return initial_chkpnt_data()

    try:
        with open(path, 'r') as f:
            s = f.read()
    except FileNotFoundError:
        # in the case of the file not existing, we need to load some dummy data
        # which will be replaced when dumped.
        logging.debug("data doesnt exist yet, normal for this to occur on "
                      "the first restart of a job series.")
        return initial_chkpnt_data()
    except OSError as err:
        logging.error("could not load checkpoint logging information, "
                      f"you may not have permission to read it. Error : {err}")
        return None

    try:
        data = json.loads(s)
    except json.JSONDecodeError as err:
        logging.error(f"loaded {path}, but an error occured while parsing the "
                      "JSON data, the file must be corrupted. Error given: "
                      f"{err}")
        return None

    if data['version'] == 'V1':
        if data['job_id'] != int(JOB_ID):
            # this is a job from a previous chkpnt, we need to update the state
            # information to reflect it being a new job - set the job id to the
            # current one, and the state to 'has not checkpointed yet'
            data['job_id'] = int(JOB_ID)
            data['chkpnt_state'] = CheckpointState.NONE

        # check we have all the fields required:
        required_fields = ['job_id', 'timestamp', 'chkpnt_state',
                           'overrun_restarts', 'should_restart', 'summary_info']

        for field in required_fields:
            if field not in data:
                logging.error(f"checkpoint state is missing field: {field}. cannot"
                              " checkpoint.")
                return None

        # all fields have been loaded in, but since the CheckpointState is
        # serialised and deserialised as a number, parse it back to the enum format
        # for code clarity

        try:
            chkpnt_state = CheckpointState(data['chkpnt_state'])
        except ValueError:
            logging.error("could not parse chkpnt_state, must be corrupted.")
            return None

        # we now have the data loaded as a dict, load it into a CheckpointData
        # object, for ease of access.

        data = CheckpointData(
            job_id=int(JOB_ID),
            timestamp=int(time.time()),
            chkpnt_state=chkpnt_state,
            overrun_restarts=data['overrun_restarts'],
            should_restart=data['should_restart'],
            summary_info=data['summary_info'],
            version=data.get('version', None)
        )

        return data

    logging.error("unsupported json state version. the file may be "
                  "corrupted, or be too old.")
    return None


def main():
    logging.debug(f"starting checks for {JOB_ID}")

    if lsf.lsb_init(None) > 0:
        logging.error("Could not init lsf python api. cannot checkpoint, "
                      "killing without checkpointing")
        emulate_bkill(write_log=False)
        return
    else:
        logging.debug("lsf python api initialised successfully")

    if not env_var_matches('LSB_JOBINDEX', '0'):
        # this is part of a job array, dont checkpoint it for 2 reasons:
        # 1. checkpointing an entire array might be enough to knock over
        #    filesystems / nodes / lsf itself
        # 2. my way of keeping track of state does not work for job arrays
        logging.info("job array job detected. checkpointing not supported. "
                     "killing job")
        emulate_bkill(write_log=False)
        return

    if env_var_matches('LSB_INTERACTIVE', 'Y'):
        logging.info("interactive job detected. checkpointing not "
                     "supported. killing job.")
        emulate_bkill(write_log=False)
        return

    if not is_job_overrunning():
        logging.debug("job is not overrunning")
        emulate_bkill(write_log=False)
        return

    if not env_var_matches('ENABLE_ORUN_CHKPNT', 'Y'):
        logging.info("Overrun Checkpoint not enabled, not checkpointing")
        emulate_bkill(write_log=False)
        return

    if not check_chkpnt_vars():
        logging.debug("job is not checkpointable")
        emulate_bkill(write_log=False)
        return

    chkpnt_data = load_chkpnt_state()

    if chkpnt_data is None:
        logging.error("could not load job chkpnt data, due to above error, "
                      "unknown state, killing job.")
        emulate_bkill()
        return

    if chkpnt_data.chkpnt_state == CheckpointState.NONE:
        if chkpnt_data.overrun_restarts < ORUN_RESTART_LIMIT:
            logging.info("overrunning job detected that has not exceeded its "
                         "restart limit yet. It has restarted "
                         f"{chkpnt_data.overrun_restarts} times and the limit "
                         f"is {ORUN_RESTART_LIMIT}. Checkpointing now. This "
                         "will be restart number "
                         f"{chkpnt_data.overrun_restarts + 1}")
            checkpoint()
            chkpnt_data.chkpnt_state = CheckpointState.STARTED
            dump_chkpnt_state(chkpnt_data)
            logging.info("checkpoint started.")
            logging.debug("Exit with code: 2")
            sys.exit(2)
            # We have to exit the script here because being in a job_action
            # (like this script) blocks LSF from initiating checkpointing that
            # is started via the API or `bchkpnt`. We exit with a non-0 error
            # value to let LSF know that the job is not dead. The checkpoint
            # should start immidiately, and then when it is finished, another
            # of these scripts should fire.

        logging.info("Overunning job detected - has already restarted "
                     f"{chkpnt_data.overrun_restarts} times, limit is "
                     f"{ORUN_RESTART_LIMIT}. Not Checkpointing.")
        emulate_bkill()
        return

    if chkpnt_data.chkpnt_state == CheckpointState.STARTED:
        # this state can occur when LSF tries to submit a termination
        # action while already in a checkpointing action, so just exit
        # with a non-0 error code to indicate termination is not done
        # yet. We also check to see if the job has been checkpointing
        # for longer than our timeout period, and if it has, assume it
        # has hung, or errored out and been unable to update the checkpoint
        # state file and will not checkpoint, so kill the job.
        logging.debug("checkpointing is already in progress...")
        logging.debug(f"json load: {load_chkpnt_state()}")
        event_time = chkpnt_data.timestamp
        now = int(time.time())
        timed_out = event_time + TIMEOUT < now
        if timed_out:
            logging.warning("the checkpointing process appears to have hung, "
                            f"timeout of {TIMEOUT}s reached. killing job.")
            emulate_bkill()
            return

        time.sleep(2)
        logging.info("Checkpointing is in progress...")
        logging.debug("Exit with code: 2")
        sys.exit(2)

    if chkpnt_data.chkpnt_state == CheckpointState.SUCCESS:
        logging.info("Checkpoint completed successfully.")
        # sometimes the process tree doesnt die but did get
        # checkpointed successfully, so we need to make sure it is
        # manually killed.
        if not is_job_dead():
            logging.warning("Checkpointing exited successfully, but the"
                            "job is still alive, killing job manually")
            emulate_bkill()
        return

    if chkpnt_data.chkpnt_state == CheckpointState.FAILED:
        # sometimes the checkpointing may fail, and this will get
        # written. this should kill the job when this script picks it
        # back up
        logging.warning("Checkpoint failed, attempting normal kill")
        emulate_bkill()


if __name__ == "__main__":
    if 'LSB_CHKPNT_DIR' in os.environ:
        # only log checkpointing events if a checkpointing directory is set,
        # otherwise we should be killing the job normally. This also puts the
        # log in an intuitive place, allowing users to clean up more easily
        logging.basicConfig(format=("%(levelname)s: %(asctime)s %(message)s"),
                            datefmt="%Y/%m/%d %H:%M:%S",
                            filename=f'{os.environ["LSB_CHKPNT_DIR"]}/'
                                     f'job_{JOB_ID}_end.log',
                            level=logging.DEBUG,
                            encoding='utf-8')
    else:
        # write log events to stderr (invisible to the user, since LSF discards
        # stderr for job_controls unless it is redirected within bqueues)
        logger = logging.getLogger('no_chkpnt')
        logger.setLevel(logging.DEBUG)
        stderr_handler = logging.StreamHandler()
        stderr_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(levelname)s: %(asctime)s %(message)s")
        logger.addHandler(stderr_handler)

    main()

    if not is_job_dead():
        logging.error("job is still alive at end of script")
        logging.debug("Exit with code: 1")
        sys.exit(1)

    logging.debug("job has been killed or checkpointed, success")
    logging.debug("Exit with code: 0")
    sys.exit(0)
