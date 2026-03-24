from enum import Enum


class Topic(str, Enum):
    PRICE_UPDATE = "price_update"
    ORDER_UPDATE = "order_update"