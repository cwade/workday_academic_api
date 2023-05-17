import json
from decimal import Decimal
from datetime import date


class StringEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, date):
            return str(obj)
        return json.JSONEncoder.default(self, obj)
