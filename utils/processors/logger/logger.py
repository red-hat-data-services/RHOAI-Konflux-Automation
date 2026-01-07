import logging
import os


class ColoredFormatter(logging.Formatter):
    """
    Custom log formatter that inherits from `logging.Formatter` to add ANSI escape codes
    for colorizing log messages based on their severity level.
    """
    
    # Dictionary to define colors for different log levels
    COLORS = {
        logging.DEBUG: "\033[95m",      # Magenta
        logging.INFO: "\033[97m",       # White
        logging.WARNING: "\033[93m",    # Yellow
        logging.ERROR: "\033[91m",      # Red
        logging.CRITICAL: "\033[91m",   # Red
    }
    RESET = "\033[0m"  # Reset color

    def format(self, record):
        color = self.COLORS.get(record.levelno, self.RESET)
        message = super().format(record)
        return f"{color}{message}{self.RESET}"



def getLogger(name: str) -> logging.Logger:
    """
    Configures a logger with both console and file handlers.

    - Console logs are colorized and use a configurable log level.
    - File logs are written to a specified directory with a configurable log level.

    The following environment variables are used:
    - `CONSOLE_LOG_LEVEL`: Log level for console output. Defaults to 'INFO'.
    - `FILE_LOG_LEVEL`: Log level for file output. Defaults to 'DEBUG'.
    - `LOG_FILE_DIR`: Directory path for log file output. Defaults to the current directory.
    - `LOG_FILE_NAME`: Filename for log output. Defaults to app.log
    """
    console_log_level = os.environ.get('CONSOLE_LOG_LEVEL', 'INFO').upper()
    file_log_level = os.environ.get('FILE_LOG_LEVEL', 'DEBUG').upper()
    log_file_dir = os.environ.get('LOG_FILE_DIR', '.')
    log_file_name = os.environ.get('LOG_FILE_NAME', 'app.log')

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Set the minimum logging level for the logger

    # Create console handler and set its level and formatter
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_log_level)
    console_formatter = ColoredFormatter('{levelname} - {message}', style='{')
    console_handler.setFormatter(console_formatter)

    # Create file handler and set its level and formatter
    file_handler = logging.FileHandler(filename=os.path.join(log_file_dir, log_file_name), mode='w')
    file_handler.setLevel(file_log_level)
    file_formatter = logging.Formatter('{levelname} - {message}', style='{')
    file_handler.setFormatter(file_formatter)

    # Add handlers to the logger
    if not logger.hasHandlers():  # Avoid adding multiple handlers
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger


