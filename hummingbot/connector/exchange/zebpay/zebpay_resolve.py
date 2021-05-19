#!/usr/bin/env python

# API Feed adjusted to sandbox url
from hummingbot.core.event.events import TradeType, OrderType
from hummingbot.core.utils.asyncio_throttle import Throttler


_ZEBPAY_REST_URL_SANDBOX = "https://www.zebpay.co"
_ZEBPAY_REST_URL_PROD = "https://www.zebapi.com"

_ZEBPAY_WS_FEED_PROD = "wss://ws-feed.zebpay.com/marketdata"

_IS_ZEBPAY_SANDBOX = None

# Websocket stream constants
BUY_MSG = "BUY"
BUY_ACTION_TYPE = 1
SELL_MSG = "SELL"
SELL_ACTION_TYPE = 0
TRADE_MSG = "MATCH"
DIFF_MSG = "DIFF"
ADD_MSG = "ADD"
DELETE_MSG = "DELETE"

ORDER_FAILED = -1
ORDER_PENDING = 10
ORDER_FINISHED = 1
ORDER_CANCELLED = 2


def set_domain(domain):
    """Save user selected domain so we don't have to pass around domain to every method"""
    global _IS_ZEBPAY_SANDBOX

    if domain == "co":  # TODO: confirm domain name is appropriate for sandbox and prod
        _IS_ZEBPAY_SANDBOX = True
    elif domain == "com":
        _IS_ZEBPAY_SANDBOX = False
    else:
        raise Exception(f'Bad configuration of domain "{domain}"')


def get_rest_url_for_domain(domain):
    if domain == "co":  # sandbox
        return _ZEBPAY_REST_URL_SANDBOX
    elif domain == "com":  # prod
        return _ZEBPAY_REST_URL_PROD
    else:
        raise Exception(f'Bad configuration of domain "{domain}"')


def get_ws_url_for_domain(domain):
    if domain == "com":
        return _ZEBPAY_WS_FEED_PROD
    else:
        raise Exception(f'Bad configuration of domain "{domain}"')


def is_zebpay_sandbox():
    """Late loading of user selection of using sandbox from configuration"""
    if _IS_ZEBPAY_SANDBOX is None:
        return False
    return _IS_ZEBPAY_SANDBOX


def get_zebpay_rest_url(domain=None):
    """Late resolution of zebpay rest url to give time for configuration to load"""
    if domain is not None:
        # we need to pass the domain only if the method is called before the market is instantiated
        return get_rest_url_for_domain(domain)
    if is_zebpay_sandbox():
        return _ZEBPAY_REST_URL_SANDBOX
    else:
        return _ZEBPAY_REST_URL_PROD


def get_zebpay_ws_feed(domain=None):
    """Late resolution of zebpay WS url to give time for configuration to load"""
    if domain is not None:
        # we need to pass the domain only if the method is called before the market is instantiated
        return get_ws_url_for_domain(domain)
    else:
        return _ZEBPAY_WS_FEED_PROD


_throttler = None


def get_throttler() -> Throttler:
    global _throttler
    if _throttler is None:
        _throttler = Throttler(rate_limit=(4, 1.0))  # rate_limit=(weight, t_period)
    return _throttler
