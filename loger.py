import logging
def logs(name,):
    logger = logging.getLogger(name)
    formater = logging.Formatter( "[%(asctime)s] %(message)s", '%Y-%m-%d %H:%M:%S' )
    logger.setLevel(logging.DEBUG)
    file_log = logging.FileHandler(name, )
    file_log.setFormatter(formater)
    logger.addHandler(file_log)
    ch = logging.StreamHandler()
    ch.setFormatter(formater)
    logger.addHandler(ch)
    return logger.info
class log:
    log:logs = None