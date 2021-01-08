import os
import logging

import telegram
from telegram import Bot


def get_logger_from_self(self):
    return logging.getLogger(f"{type(self).__name__}_logger")


def get_standardized_str(str_to_convert):
    return str_to_convert.lower().strip('\n').strip(' ')


class BaseLogger:
    def __init__(self):
        self.chat_id = os.getenv('CHAT_ID')
        token = os.getenv('BOT_TOKEN')
        self.bot = Bot(token) if token and self.chat_id else False
        self.logger = get_logger_from_self(self)

    def _send_log_info(self, message, log_level='info', to_telegram=True):
        if log_level == 'debug':
            self.logger.debug(message)
        elif log_level == 'info':
            self.logger.info(message)
        elif log_level == 'error':
            self.logger.error(message)
        elif log_level == 'exception':
            self.logger.exception(message, exc_info=True)
        if to_telegram and self.bot:
            try:
                self.bot.send_message(self.chat_id, message, timeout=2)
            except telegram.error.TelegramError as e:
                self.logger.info(f"Telegram error {e} to Telegram while "
                                 f"sending message {message}")
