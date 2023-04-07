import logging


factory_logger = logging.getLogger('factory')
factory_logger.addHandler(logging.StreamHandler())
factory_logger.setLevel(logging.DEBUG)

timer_logger = logging.getLogger('timer')
timer_logger.addHandler(logging.StreamHandler())
timer_logger.setLevel(logging.INFO)
