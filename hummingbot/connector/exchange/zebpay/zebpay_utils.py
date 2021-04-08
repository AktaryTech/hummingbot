from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce


CENTRALIZED = False

USE_ETHEREUM_WALLET = False

EXAMPLE_PAIR = "DAI-INR"

DEFAULT_FEES = [0.15, 0.25]

USE_ETH_GAS_LOOKUP = False  # Is this required?

HUMMINGBOT_GAS_LOOKUP = False  # Is this required?

HBOT_BROKER_ID = "HBOT-"

EXCHANGE_NAME = "zebpay"


# Example: HBOT-B-DIL-ETH-64106538-8b61-11eb-b2bb-1e29c0300f46
def get_new_client_order_id(is_buy: bool, trading_pair: str) -> str:
    side = "B" if is_buy else "S"
    return f"{HBOT_BROKER_ID}{side}-{trading_pair}-{get_tracking_nonce()}"


KEYS = {
    "idex_api_key":
        ConfigVar(key="zebpay_api_key",
                  prompt="Enter your Zebpay API key >>> ",
                  required_if=using_exchange(EXCHANGE_NAME),
                  is_secure=True,
                  is_connect_key=True),
    "idex_api_secret_key":
        ConfigVar(key="idex_api_secret_key",
                  prompt="Enter your Zebpay API secret key>>> ",
                  required_if=using_exchange(EXCHANGE_NAME),
                  is_secure=True,
                  is_connect_key=True),
}

DEBUG = True
