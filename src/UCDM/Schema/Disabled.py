import logging
from src.UCDM.Schema.BaseSchema import BaseSchema


class Disabled(BaseSchema):
    def __init__(self):
        super().__init__()
