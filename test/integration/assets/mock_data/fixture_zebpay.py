import json


class FixtureZebpay:
    # General Exchange Info
    MARKETS = [
        {
            "buy": "2590.54",
            "sell": "2590.54",
            "market": "2590.54",
            "volume": "1.28374",
            "pricechange": "0",
            "24hoursHigh": "2590.54",
            "24hoursLow": "2590.54",
            "pair": "ETH-AUD",
            "virtualCurrency": "ETH",
            "currency": "AUD",
            "instantBuy": "2622.31",
            "instantSell": "2595.78"
        },
        {
            "buy": "73622.41",
            "sell": "73622.41",
            "market": "73622.41",
            "volume": "0.02801102",
            "pricechange": "0",
            "24hoursHigh": "73622.41",
            "24hoursLow": "73622.41",
            "pair": "BTC-AUD",
            "virtualCurrency": "BTC",
            "currency": "AUD",
            "instantBuy": "74421.97",
            "instantSell": "73675.26"
        },
    ]


    # Trade fees found in the exchange api request
    '''
    TRADE_FEES = {
        "timeZone": "UTC",
        "serverTime": 1590408000000,
        "ethereumDepositContractAddress": "0x...",
        "ethUsdPrice": "206.46",
        "gasPrice": 7,
        "volume24hUsd": "10416227.98",
        "makerFeeRate": "0.001",
        "takerFeeRate": "0.002",
        "makerTradeMinimum": "0.15000000",
        "takerTradeMinimum": "0.05000000",
        "withdrawalMinimum": "0.04000000"
    }
    '''

    # ORDERS_STATUS = []

    LISTEN_KEY = None

    # User Trade Info
    # Sample snapshot for trading pair "dai-inr"

    SNAPSHOT_1 = {
        "asks": [
            {"price": "1964.44",
             "amount": 45434},
            {"price": "1965.24",
             "amount": 5435},
            {"price": "1966.32",
             "amount": 76765},
        ],
        "bids": [
            {"price": "1962.44",
             "amount": 34323},
            {"price": "1961.57",
             "amount": 5432},
            {"price": "1959.34",
             "amount": 4535},
        ],
        "pair": "dai-inr"
    }

    # Sample snapshot for trading pair "btc-aud"
    SNAPSHOT_2 = {
        "asks": [
            {"price": "14964.44",
             "amount": 20044680},
            {"price": "14965.24",
             "amount": 38363629},
            {"price": "14966.32",
             "amount": 1245454},
        ],
        "bids": [
            {"price": "14962.44",
             "amount": 20044680},
            {"price": "14961.57",
             "amount": 23454},
            {"price": "14959.34",
             "amount": 65344},
        ],
        "pair": "btc-aud"
    }

    TRADING_PAIR_TRADES = [
        {
            "trans_id": 13,
            "fill_qty": 34354,
            "fill_price": 0.0005,
            "fill_flags": 1,
            "currencyPair": "BTC-AUD",
            "lastModifiedDate": 1538576785865
        },
        {
            "trans_id": 12,
            "fill_qty": 65465,
            "fill_price": 0.0009,
            "fill_flags": 1,
            "currencyPair": "BTC-AUD",
            "lastModifiedDate": 1538576785346
        },
        {
            "trans_id": 9,
            "fill_qty": 54654,
            "fill_price": 0.001,
            "fill_flags": 1,
            "currencyPair": "BTC-AUD",
            "lastModifiedDate": 1538576785123
        },
    ]

    TRADING_PAIR_TICKER = {
        "buy": "14965.69",
        "sell": "14962.39",
        "market": "14964.39",
        "volume": 4.95532,
        "24hoursHigh": "14965.69",
        "24hoursLow": "14962.39",
        "pricechange": "3.30",
        "pair": "btc-aud",
        "virtualCurrency": "btc",
        "currency": "aud"
    }

    ORDER_BOOK_LEVEL2 = {
        "asks": [
            {"price": "14964.44",
             "amount": 20044680},
            {"price": "14965.24",
             "amount": 38363629},
            {"price": "14966.32",
             "amount": 1245454},
        ],
        "bids": [
            {"price": "14962.44",
             "amount": 20044680},
            {"price": "14961.57",
             "amount": 23454},
            {"price": "14959.34",
             "amount": 65344},
        ],
        "pair": "btc-aud"
    }

    '''
    WS_PRICE_LEVEL_UPDATE_1 = json.dumps({
        "type": "l2orderbook",
        "data": {
            "m": "ETH-USDC",
            "t": 1590393540000,
            "u": 71228110,
            "b": [["202.00100000", "10.00000000", 1]],
            "a": []
        }
    })

    WS_PRICE_LEVEL_UPDATE_2 = json.dumps({
        "type": "l2orderbook",
        "data": {
            "m": "BAL-ETH",
            "t": 1590383943830,
            "u": 73848374,
            "b": [["198.00100000", "8.00000000", 2]],
            "a": []
        }
    })

    WS_SUBSCRIPTION_SUCCESS = json.dumps({
        "type": "subscriptions",
        "subscriptions": [{"name": "l2orderbook",
                           "markets": ["ETH-USDC"]
                           }]
    })

    WS_TRADE_1 = json.dumps({
        "type": "trades",
        "data": {
            "m": "ETH-USDC",
            "i": "a0b6a470-a6bf-11ea-90a3-8de307b3b6da",
            "p": "202.74900000",
            "q": "10.00000000",
            "Q": "2027.49000000",
            "t": 1590394500000,
            "s": "sell",
            "u": 848778
        }
    })

    WS_TRADE_2 = json.dumps({
        "type": "trades",
        "data": {
            "m": "QNT-ETH",
            "i": "d357a470-a6bf-11ea-90a3-8de3034936da",
            "p": "154.82400000",
            "q": "8.00000000",
            "Q": "1163.53000000",
            "t": 1590387400000,
            "s": "buy",
            "u": 921943
        }
    })

    # Group A
    BUY_MARKET_ORDER = {
            "market": "DIL-ETH",
            "orderId": "92782120-a775-11ea-aa55-4da1cc97a06d",
            "clientOrderId": "10001",
            "wallet": "0xA71C4aeeAabBBB8D2910F41C2ca3964b81F7310d",
            "time": 1590394500000,
            "status": "active",
            "type": "market",
            "side": "buy",
            "originalQuantity": "0.02000000",
            "executedQuantity": "0.00000000",
            "selfTradePrevention": "dc"
        }

    SELL_MARKET_ORDER = {
            "market": "DIL-ETH",
            "orderId": "92782120-a775-11ea-aa55-4da1cc97a06d",
            "clientOrderId": "10001",
            "wallet": "0xA71C4aeeAabBBB8D2910F41C2ca3964b81F7310d",
            "time": 1590394500000,
            "status": "active",
            "type": "market",
            "side": "sell",
            "originalQuantity": "0.02000000",
            "executedQuantity": "0.00000000",
            "selfTradePrevention": "dc"
        }

    # Group A
    WS_AFTER_MARKET_BUY_2 = {
        "type": "orders",
        "data": {
            "m": "ETH-USDC",
            "i": "92782120-a775-11ea-aa55-4da1cc97a06d",
            "c": "10001",
            "w": "0xA71C4aeeAabBBB8D2910F41C2ca3964b81F7310d",
            "t": 1590394700000,
            "T": 1590394700000,
            "x": "fill",
            "X": "filled",
            "u": 71228108,
            "o": "market",
            "S": "buy",
            "q": "0.02000000",
            "z": "0.02000000",
            "Z": "0.02000000",
            "v": "12.00200000",
            "F": [
                {
                    "i": "974480d0-a776-11ea-895b-bfcbb5bdaa50",
                    "p": "12.00200000",
                    "q": "0.02000000",
                    "Q": "19.58344815",
                    "t": 1590394700000,
                    "s": "sell",
                    "u": 981372,
                    "f": "0.00756017",
                    "a": "ETH",
                    "l": "taker",
                    "T": "0x01d28c33271cf1dd0eb04249617d3092f24bd9bad77ffb57a0316c3ce5425158",
                    "S": "mined"
                },
                ...
            ]
        }
    }

    # Group B
    OPEN_BUY_LIMIT_ORDER = {
        "market": "ETH-USDC",
        "orderId": "3a9ef9c0-a779-11ea-907d-23e999279287",
        "clientOrderId": "10001",
        "wallet": "0xA71C4aeeAabBBB8D2910F41C2ca3964b81F7310d",
        "time": 1590394500000,
        "status": "active",
        "type": "limit",
        "side": "buy",
        "originalQuantity": "0.02000000",
        "executedQuantity": "0.00000000",
        "cumulativeQuoteQuantity": "0.00000000",
        "price": "190.00000000",
    }

    # Group B
    OPEN_SELL_LIMIT_ORDER = {
        "market": "ETH-USDC",
        "orderId": "3a9ef9c0-a779-11ea-907d-23e999279287",
        "clientOrderId": "10001",
        "wallet": "0xA71C4aeeAabBBB8D2910F41C2ca3964b81F7310d",
        "time": 1590394500000,
        "status": "active",
        "type": "limit",
        "side": "sell",
        "originalQuantity": "0.02000000",
        "executedQuantity": "0.00000000",
        "cumulativeQuoteQuantity": "0.00000000",
        "price": "190.00000000",
    }

    # Group B
    WS_ORDER_OPEN = {
        "type": "orders",
        "data": {
            "m": "DIL-ETH",
            "i": "3a9ef9c0-a779-11ea-907d-23e999279287",
            "c": "10001",
            "w": "0xA71C4aeeAabBBB8D2910F41C2ca3964b81F7310d",
            "t": 1590394500000,
            "T": 1590394500000,
            "x": "new",
            "X": "active",
            "o": "limit",
            "S": "buy",
            "q": "0.02000000",
            "z": "0.00000000",
            "V": "dc",
        }
    }

    # Group B
    WS_ORDER_CANCELLED = {
        "type": "orders",
        "data": {
            "m": "DIL-ETH",
            "i": "3a9ef9c0-a779-11ea-907d-23e999279287",
            "c": "10001",
            "w": "0xA71C4aeeAabBBB8D2910F41C2ca3964b81F7310d",
            "t": 1590394500000,
            "T": 1590394500000,
            "x": "cancelled",
            "X": "cancelled",
            "o": "market",
            "S": "buy",
            "q": "0.02000000",
            "z": "0.00000000",
            "V": "dc",
        }
    }
    '''
