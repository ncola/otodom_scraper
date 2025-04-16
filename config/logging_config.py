import logging, colorlog
from datetime import datetime

def setup_logger():
    log_filename = datetime.now().strftime("logs/otodom_app_log_%Y-%m-%d_%H-%M-%S.log")

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)

    console_handler = colorlog.StreamHandler()
    console_handler.setFormatter(
        colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',  # Format z kolorami
            datefmt='%Y-%m-%d %H:%M:%S',  # Format daty
            log_colors={
                'DEBUG': 'green',
                'INFO': 'blue',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            }
        )
    )

    # Dodajemy handler do loggera
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
