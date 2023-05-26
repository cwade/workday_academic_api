import json
import decimal
import datetime


class WorkdayJSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return
        return super(WorkdayJSONEncoder, self).default(obj)


def flatten_json(nested_json):
    out = {}

    def flatten(x, name=''):
        if type(x) == dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) == list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out
