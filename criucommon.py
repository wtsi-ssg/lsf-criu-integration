import logging
import logging.handlers
import os


def set_log(logfile, loglevel) -> logging.Logger:
    if logfile is None:
        # syslog
        handler = logging.handlers.SysLogHandler(address='/dev/log')
        # dont log the time, syslog already does this
        logformat = logging.Formatter("%(levelname)s %(name)s job "
                                      f"{os.environ['LSB_JOBID']}: %(message)s")
        handler.setFormatter(logformat)
    else:
        # write to a file
        handler = logging.FileHandler(logfile)
        logformat = logging.Formatter("%(levelname)s %(asctime)s %(name)s job "
                                      f"{os.environ['LSB_JOBID']}: %(message)s")
        handler.setFormatter(logformat)

    log = logging.getLogger('echkpnt.criu')
    log.setLevel(loglevel)
    log.addHandler(handler)
    return log
