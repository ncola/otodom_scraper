import logging, colorlog, os
from datetime import datetime, timedelta


def _delete_old_logs():
    # delete main log files older than 30 days
    logs_dir = "logs"
    cutoff = datetime.now() - timedelta(days=30)

    for filename in os.listdir(logs_dir):
        if not filename.startswith("otodom_") or not filename.endswith(".log"):
            continue
        filepath = os.path.join(logs_dir, filename)
        file_mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
        if file_mtime < cutoff:
            os.remove(filepath)
            logging.debug(f"deleted old log file: {filename}")


def setup_logger():
    os.makedirs("logs", exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    fmt = '%(asctime)s - %(levelname)s - %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'

    # one file per run
    file_handler = logging.FileHandler(f"logs/otodom_{timestamp}.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(fmt, datefmt=datefmt))

    # colored output in terminal
    console_handler = colorlog.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(
        colorlog.ColoredFormatter(
            '%(log_color)s' + fmt,
            datefmt=datefmt,
            log_colors={
                'DEBUG':    'green',
                'INFO':     'blue',
                'WARNING':  'yellow',
                'ERROR':    'red',
                'CRITICAL': 'bold_red',
            }
        )
    )

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    _delete_old_logs()

    return logger


def setup_failed_offers_logger():
    # separate logger for offers that failed to fetch
    # one file per month, format: timestamp | listing_id | link | error
    os.makedirs("logs", exist_ok=True)

    month = datetime.now().strftime("%Y-%m")
    failed_logger = logging.getLogger("failed_offers")
    failed_logger.setLevel(logging.ERROR)

    # don't pass logs up to the main logger — would cause duplicates
    failed_logger.propagate = False

    # add handler only once — prevents duplicates on repeated calls
    if not failed_logger.handlers:
        handler = logging.FileHandler(f"logs/failed_offers_{month}.txt")
        handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
        failed_logger.addHandler(handler)

    return failed_logger
