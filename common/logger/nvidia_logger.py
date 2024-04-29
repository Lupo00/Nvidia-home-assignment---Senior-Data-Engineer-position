import logging
import os
import sys
import time
from functools import wraps


class NvidiaLogger:
    def __init__(self, name='SystemLogger', log_level=logging.INFO, log_file='disable'):
        # Create a logger object
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)

        # Ensure no other handlers are added
        if not self.logger.hasHandlers():
            # Create file handler which logs even debug messages
            if log_file == 'disable':
                fh = logging.NullHandler()
            elif log_file == 'std':
                fh = logging.StreamHandler()
            else:
                fh = logging.FileHandler(log_file)

            fh.setLevel(log_level)

            # Create formatter
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

            # Add formatter to fh
            fh.setFormatter(formatter)

            # Add fh to logger
            self.logger.addHandler(fh)

        # Log initial system information


    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)

    def log_execution_time(self, func):
        """Decorator to log the execution time of a method."""

        def wrapper(*args, **kwargs):
            self.logger.info(f"----------------------------------- Start to run {func.__name__} -----------------------------------")

            start_time = time.time()  # Capture the start time
            result = func(*args, **kwargs)  # Execute the function
            end_time = time.time()  # Capture the end time
            # Log the function execution time
            self.logger.info(f"----------------------------------- {func.__name__} executed in {end_time - start_time:.4f} seconds -----------------------------------")
            self.logger.info("--------------------------------------------------------------------------------------------------------------")
            self.logger.info("")

            return result

        return wrapper

    def is_on_debug_mode(self) -> bool:
        return self.logger.level == logging.DEBUG

    def mark_start_and_end_run(self, param):
        """Decorator to log the execution time of a method."""

        def decorator(func):

            def wrapper(*args, **kwargs):
                self.logger.info(f"-----------------------------------------------------------------------------------")
                self.logger.info(f"----------------------------------- Start to run {param} -----------------------------------")

                result = func(*args, **kwargs)  # Execute the function
                self.logger.info(f"----------------------------------- End to run {param} -----------------------------------")
                self.logger.info(f"-----------------------------------------------------------------------------------")
                self.logger.info("")
                return result

            return wrapper

        return decorator