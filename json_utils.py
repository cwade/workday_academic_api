import json
import decimal
import datetime
import pandas as pd


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

def get_key_value_for_types(list_of_dicts, types, key='_value_1'):
    if type(list_of_dicts) != list and pd.isnull(list_of_dicts):
        return
    if type(list_of_dicts) != list:
        print("Warning: was looking for {} in {}".format(types, list_of_dicts))
        return(list_of_dicts)
    values = [x[key] for x in list_of_dicts if x['type'] in types]
    if len(values) == 1:
        return(values[0])
    elif len(values) == 0:
        print("Warning: didn't find {} in {}".format(types, list_of_dicts))
        return
    else:
        raise Exception('got unexpected values {}'.format(values))

def extract_sup_orgs(data):
    if type(data) != list:
        return data
    else:
        orgs = [get_key_value_for_types(
            x['Organization_Reference']['ID'], ['Organization_Reference_ID'])[12:].replace(
            '_', ' ') for x in data]
        return ' > '.join(orgs)
