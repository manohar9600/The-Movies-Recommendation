import os
import sys
import gzip
import time
import shutil
import logging
import logging.handlers
from os import path, makedirs
import textwrap
# from pythonjsonlogger import jsonlogger


class CompressedRotatingFileHandler(logging.handlers.RotatingFileHandler):
    # Logger Zipping Mechanism
    def doRollover(self):

        if self.stream:
            self.stream.close()
        if self.backupCount > 0:
            for i in range(self.backupCount - 1, 0, -1):
                sfn = "%s.%d.gz" % (self.baseFilename, i)
                dfn = "%s.%d.gz" % (self.baseFilename, i + 1)
                if os.path.exists(sfn):
                    if os.path.exists(dfn):
                        os.remove(dfn)
                    os.rename(sfn, dfn)
            dfn = self.baseFilename + ".1.gz"

            if os.path.exists(dfn):
                os.remove(dfn)
            with open(self.baseFilename, 'rb') as f_in, gzip.open(dfn, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        self.mode = 'w'
        self.stream = self._open()


class Formatter(logging.Formatter):
    def __init__(self, format_str):
        super(Formatter, self).__init__(fmt=format_str)

    def format(self, record):
        message = record.msg
        record.msg = ''
        # level_indent = 7 - len(record.levelname)
        # record.levelname = ' ' * level_indent + record.levelname
        header = super(Formatter, self).format(record)
        msg = textwrap.indent(message, ' ' * len(header)).strip()
        record.msg = message
        return header + msg


class loggerClass:

    name = 'MR'
    full_log_file_name = 'all.log'
    error_log_file_name = 'error.log'
    stats_file_name = 'stats.txt'

    def __init__(self):
        outputPath = 'logs'    # Create Log file if not exist
        if not path.exists(outputPath):
            makedirs(outputPath)
        pass

    def setup_logger(self):
        # format_str = '%(threadName)s %(name)s %(thread)d %(created)f %(process)d %(processName)s %(relativeCreated)d %(module)s %(funcName)s %(levelno)s %(msecs)d %(pathname)s %(lineno)d %(asctime)s %(message)s %(filename)s %(levelname)s '
        format_str = '[%(asctime)s] [%(levelname)s] %(filename)s %(message)s'
        formatter = Formatter(format_str)
        logger = logging.getLogger(self.name)

        # full log
        file_path = 'logs/' + self.full_log_file_name
        handler = CompressedRotatingFileHandler(
            filename=file_path, maxBytes=1000000*5, backupCount=1000)  # zips log file if it reaches 5 MB
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)

        # error log
        file_path = 'logs/' + self.error_log_file_name
        error_handler = CompressedRotatingFileHandler(
            filename=file_path, maxBytes=100000*5, backupCount=1000)  # zips log file if it reaches 5 MB
        error_handler.setFormatter(formatter)
        error_handler.setLevel(logging.ERROR)

        # streaming
        console_handler = logging.StreamHandler(sys.stdout)
        format_str = '[%(levelname)s] %(message)s'
        formatter = Formatter(format_str)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.DEBUG)
        
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        logger.addHandler(error_handler)
        logger.addHandler(console_handler)
        

        return logger


custom_logger = loggerClass()
logger = custom_logger.setup_logger()

'''To check time log for each function "Attach @timing decorator above the function definition'''
# time_info_logger = custom_logger.setup_logger('time_info', 'logs/time_info.log', logging.INFO)
#
#
# def timing(f):
#     def wrap(*args):
#         time1 = time.time()
#         ret = f(*args)
#         time2 = time.time()
#         time_info_logger.info('{:s} function took {: f} s'.format(f.__name__, (time2 - time1)))
#
#         return ret
#
#     return wrap
