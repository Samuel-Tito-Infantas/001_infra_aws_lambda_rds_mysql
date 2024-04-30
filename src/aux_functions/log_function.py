import logging

class LoggerJob:
    def __init__(self):
        log_format = (
            "{'log_time':'%(asctime)s',"
            "'level_name':'%(levelname)s',"
            "'message':'%(message)s'}"
        )

        self.logger = logging.getLogger(__name__)
        syslog = logging.StreamHandler()

        formatter = logging.Formatter(log_format)
        syslog.setFormatter(formatter)

        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(syslog)

    def log(self):
        return self.logger
