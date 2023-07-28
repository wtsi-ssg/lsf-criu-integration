# criu and LSF integration

This repo contains a few scripts to integrate LSF with the checkpointing
program `criu`.

The scripts should be placed in `$LSF_SERVERDIR`, and the following added to
a queue to enable the automating checkpoint & restore of overrunning jobs:

```
JOB_CONTROLS         = TERMINATE["$LSF_SERVERDIR/hijack.sh"]
POST_EXEC           = $LSF_SERVERDIR/post_exec.py
```

In this example, the `criu` program is stored at `/usr/local/bin/criu` on every
machine in the cluster. In order to successfully checkpoint an LSF job, it must 
be given these capabilities as file capabilities:

```
cap_sys_ptrace,cap_sys_admin=ep
```

A virtual environment should be setup with the packages in `requirements.txt`
installed into it. The scripts that have shebangs in them should be pointed
at this virtual environment.



## Why these specific capabilities

`cap_sys_ptrace` is required due to `criu` using `ptrace` to stop execution of 
the target process. The `cap_sys_ptrace` capability is required when 
`proc/sys/kernel/yama/ptrace_scope` is set to 1 which is the default. If you 
want to keep `ptrace_scope` as 1 to keep the extra security it provides then you
must give `criu` `cap_sys_ptrace`. For more information, see the 
`/proc/sys/kernel/yama/ptrace_scope` section of the `ptrace(2)` man-page. If
your system has `proc/sys/kernel/yama/ptrace_scope` set to 0, then this 
capability should not be required, although this has not been tested. Similarly,
higher numbers in `proc/sys/kernel/yama/ptrace_scope` have not been tested.

`cap_checkpoint_restore` is unfortunately not enough to fully checkpoint & 
restore a process, due to needing to lock/unlock `termios` structs of the `pty` 
that the process is using for stdin/stdout/stderr which is currently a strict 
requirement of `criu` when restoring a process that uses any of 
stdin/stdout/stderr, which all LSF jobs must be able to do, even if they do not
usefully use any of them. It can successfully dump the process, but it cannot 
be restored due to needing to lock/unlock a `pty` it does not own in the 
process of restoring.

`cap_sys_admin` is required due to being a superset of `cap_checkpoint_restore` 
and also being able to lock/unlock `termios` structs, which allows `criu` to 
set up the `pty` correctly for the restored job. This is not ideal, since it 
grants extra permissions than necessary to `criu`, but since the capabilities 
are not in the inheritable set, the process will not gain any capabilities 
after restore, so it's not too bad.


## Usage

When submitting a job, in order for it to be checkpointable at all, you must
submit it with the `-k` option (for more info see the LSF bsub docs).

In order to enable automatic checkpointing and restoring of overrunning jobs,
you must set -k in addition to having `ENABLE_ORUN_CHKPNT` and 
`ENABLE_ORUN_RESTART` set in the environment of the job. Additionally, the 
automatic checkpoints and restarts cap at 2 restarts (3 runs) by default. You
can change this by setting `ORUN_RESTART_LIMIT` to any integer. Checkpointing 
in this manner is subject to a timeout, which by default is 180 seconds. You 
can change this by setting `ORUN_TIMEOUT_SECONDS` to any integer.

### Debugging

There are multiple log files that are written during checkpointing. They are
written to the directory that the checkpoints for that job are in.


### Other useful links

- [criu homepage](https://criu.org/Main_Page)
- [IBM's echkpnt and restart scripts](https://github.com/IBMSpectrumComputing/lsf-utils/tree/master/criu) 
(these ones are adapted from theirs)


### Authors

- Anna Singleton (ws1015@york.ac.uk - university email) (annabeths111@gmail.com -
personal email)
