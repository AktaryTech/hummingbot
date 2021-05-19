import logging
import string
import uuid
import time
from typing import Optional, List, Dict, Any, AsyncIterable


from hummingbot.connector.exchange.zebpay.zebpay_resolve import get_zebpay_rest_url
from hummingbot.logger import HummingbotLogger

ia_logger = None


class ZebpayAuth:

    HEX_DIGITS_SET = set(string.hexdigits)

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global za_logger
        if za_logger is None:
            za_logger = logging.getLogger(__name__)
        return za_logger

    def __init__(self, client_id: str, secret_key: str, access_token: str):
        self._client_id = client_id or ''
        self._secret_key = secret_key or ''
        self._access_token = access_token or ''

    @staticmethod
    def generate_nonce() -> str:
        """generate uuid1 and return it as a string. Example return: cf7989e0-2030-11eb-8473-f1ca5eaaaff1"""
        return str(uuid.uuid1())

    def get_headers(self) -> Dict[str, Any]:
        request_id = self.generate_nonce()

        request_header = {
            "client_id": self._client_id,
            "timestamp": time.time(),
            "Content-Type": "application/json",
            "Authorization": self._access_token,
            "RequestId": request_id
        }
        return request_header



