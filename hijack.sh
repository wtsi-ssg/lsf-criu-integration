#!/bin/bash

# this script is intended to be a final safety net for the hijack script, in
# case it exits with a value of 1, which it should only do if it tried to kill
# the job and failed, or an unexpected / unrecoverable error occurs, since not
# being able to kill the job will cause it to just sit there forever.
#
# this script will be run by normal users, through LSF with their normal
# capabilities and privileges

export ORUN_KILL_POLL_TRIES={{chkpnt_kill_poll_tries}}

$LSF_SERVERDIR/hijack.py &> /dev/null

if [[ $? -eq 1 ]]; then
    kill -9 $LSB_JOBPIDS
fi

exit 0
