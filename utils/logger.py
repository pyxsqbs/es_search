"""A simple Google-style logging wrapper."""

from ConfigParser import SafeConfigParser
import logging
import logging.handlers
import time
import traceback
import os


def format_message(record):
    try:
        record_message = '%s' % (record.msg % record.args)
    except TypeError:
        record_message = record.msg
    return record_message


class GlogFormatter(logging.Formatter):
    LEVEL_MAP = {
        logging.FATAL: 'F',  # FATAL is alias of CRITICAL
        logging.ERROR: 'E',
        logging.WARN: 'W',
        logging.INFO: 'I',
        logging.DEBUG: 'D'
    }

    def __init__(self):
        logging.Formatter.__init__(self)

    def format(self, record):
        try:
            level = GlogFormatter.LEVEL_MAP[record.levelno]
        except KeyError:
            level = '?'
        date = time.localtime(record.created)
        date_usec = (record.created - int(record.created)) * 1e6
        record_message = '%c%02d%02d %02d:%02d:%02d.%06d %s %s:%d] %s' % (
            level, date.tm_mon, date.tm_mday, date.tm_hour, date.tm_min,
            date.tm_sec, date_usec,
            record.process if record.process is not None else '?????',
            record.filename,
            record.lineno,
            format_message(record))
        record.getMessage = lambda: record_message
        return logging.Formatter.format(self, record)


# oss_logger is solely for logging image urls in query
oss_logger = logging.getLogger('oss')


def set_oss_file_handler(file_name):
    file_handler = logging.FileHandler(file_name)
    file_handler.setFormatter(GlogFormatter())
    oss_logger.addHandler(file_handler)
    oss_logger.setLevel(INFO)


logger = logging.getLogger()
handler = logging.StreamHandler()


def set_logger(config_path, log_path):
    config = SafeConfigParser()
    config.read(config_path)

    if not os.path.exists(log_path):
        os.mkdir(log_path)
    log_name = os.path.join(log_path, config.get("log", "log_name"))
    # In debug mode, we want to keep the stream handler
    set_file_handler(log_name,
                     config.getint("log", "max_size"),
                     config.getint("log", "backup_count"),
                     config.getboolean("server", "debug"))
    set_level(config.get("log", "log_level"))


def set_level(newlevel):
    logger.setLevel(newlevel)
    logger.debug('Log level set to %s', newlevel)


def set_file_handler(file_name, max_size, backup_count, keep_stream_handler):
    if not keep_stream_handler:
        logger.removeHandler(handler)

    file_handler = logging.handlers.RotatingFileHandler(
        file_name,
        maxBytes=1024 * 1024 * 1024 * max_size,
        backupCount=backup_count)
    file_handler.setFormatter(GlogFormatter())
    logger.addHandler(file_handler)


debug = logging.debug
info = logging.info
warning = logging.warning
warn = logging.warning
error = logging.error
exception = logging.exception
fatal = logging.fatal
log = logging.log

DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
WARN = logging.WARN
ERROR = logging.ERROR
FATAL = logging.FATAL

_level_names = {
    DEBUG: 'DEBUG',
    INFO: 'INFO',
    WARN: 'WARN',
    ERROR: 'ERROR',
    FATAL: 'FATAL'
}

_level_letters = [name[0] for name in _level_names.values()]

GLOG_PREFIX_REGEX = (
                        r"""
                        (?x) ^
                        (?P<severity>[%s])
                        (?P<month>\d\d)(?P<day>\d\d)\s
                        (?P<hour>\d\d):(?P<minute>\d\d):(?P<second>\d\d)
                        \.(?P<microsecond>\d{6})\s+
                        (?P<process_id>-?\d+)\s
                        (?P<filename>[a-zA-Z<_][\w._<>-]+):(?P<line>\d+)
                        \]\s
                        """) % ''.join(_level_letters)
"""Regex you can use to parse glog line prefixes."""

handler.setFormatter(GlogFormatter())
logger.addHandler(handler)


# Define functions emulating C++ glog check-macros
# https://htmlpreview.github.io/?https://github.com/google/glog/master/doc/glog.html#check

def format_stacktrace(stack):
    """Print a stack trace that is easier to read.

    * Reduce paths to basename component
    * Truncates the part of the stack after the check failure
    """
    lines = []
    for _, f in enumerate(stack):
        fname = os.path.basename(f[0])
        line = "\t%s:%d\t%s" % (fname + "::" + f[2], f[1], f[3])
        lines.append(line)
    return lines


class FailedCheckException(AssertionError):
    """Exception with message indicating check-failure location and values."""


def check_failed(message):
    stack = traceback.extract_stack()
    stack = stack[0:-2]
    stacktrace_lines = format_stacktrace(stack)
    filename, line_num, _, _ = stack[-1]

    try:
        raise FailedCheckException(message)
    except FailedCheckException:
        log_record = logger.makeRecord('CRITICAL', 50, filename, line_num,
                                       message, None, None)
        handler.handle(log_record)

        log_record = logger.makeRecord('DEBUG', 10, filename, line_num,
                                       'Check failed here:', None, None)
        handler.handle(log_record)
        for line in stacktrace_lines:
            log_record = logger.makeRecord('DEBUG', 10, filename, line_num,
                                           line, None, None)
            handler.handle(log_record)
        raise


def check(condition, message=None):
    """Raise exception with message if condition is False."""
    if not condition:
        if message is None:
            message = "Check failed."
        check_failed(message)


def check_eq(obj1, obj2, message=None):
    """Raise exception with message if obj1 != obj2."""
    if obj1 != obj2:
        if message is None:
            message = "Check failed: %s != %s" % (str(obj1), str(obj2))
        check_failed(message)


def check_ne(obj1, obj2, message=None):
    """Raise exception with message if obj1 == obj2."""
    if obj1 == obj2:
        if message is None:
            message = "Check failed: %s == %s" % (str(obj1), str(obj2))
        check_failed(message)


def check_le(obj1, obj2, message=None):
    """Raise exception with message if not (obj1 <= obj2)."""
    if obj1 > obj2:
        if message is None:
            message = "Check failed: %s > %s" % (str(obj1), str(obj2))
        check_failed(message)


def check_ge(obj1, obj2, message=None):
    """Raise exception with message unless (obj1 >= obj2)."""
    if obj1 < obj2:
        if message is None:
            message = "Check failed: %s < %s" % (str(obj1), str(obj2))
        check_failed(message)


def check_lt(obj1, obj2, message=None):
    """Raise exception with message unless (obj1 < obj2)."""
    if obj1 >= obj2:
        if message is None:
            message = "Check failed: %s >= %s" % (str(obj1), str(obj2))
        check_failed(message)


def check_gt(obj1, obj2, message=None):
    """Raise exception with message unless (obj1 > obj2)."""
    if obj1 <= obj2:
        if message is None:
            message = "Check failed: %s <= %s" % (str(obj1), str(obj2))
        check_failed(message)


def check_notnone(obj, message=None):
    """Raise exception with message if obj is None."""
    if obj is None:
        if message is None:
            message = "Check failed: Object is None."
        check_failed(message)
