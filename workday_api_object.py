from zeep import helpers, Client
from zeep.wsse.username import UsernameToken
import json
import getpass
import yaml
import pandas as pd
from json_utils import StringEncoder


class WorkdayAPIObject(object):

    def __init__(self, object_name, config_file):
        self.object_name = object_name
        self.function = None

        with open(config_file, 'r') as ymlfile:
            config = yaml.load(ymlfile, yaml.FullLoader)

        user = config['user']
        wsdl_url = config['wsdl_url']
        binding_path = config['binding_path']

        if 'password' in config:
            password = config['password']
        else:
            password = getpass.getpass('Enter password for {}: '.format(user))

        client = Client(wsdl_url, wsse=UsernameToken(user, password))
        service = client.create_service(binding_path, wsdl_url)
        self.service = service

    def api_call(self, resp_filters, other_params):
        resp_filters['Page'] = 1
        resp_filters['Count'] = 100
        p1 = self.function(**other_params, Response_Filter=resp_filters)
        if p1.Response_Results.Total_Results == 0:
            return pd.DataFrame()
        else:
            results = [p1]

        total_pages = int(p1.Response_Results.Total_Pages)
        if total_pages > 1:
            for page in range(2, total_pages + 1):
                resp_filters['Page'] = page
                results.append(self.function(**other_params, Response_Filter=resp_filters))

        return self.parse_json_to_df(results)

    def parse_json_to_df(self, json_list):
        data = []
        for result in json_list:
            s = helpers.serialize_object(result.Response_Data)
            data = data + s[self.object_name]
        output_dict = json.loads(json.dumps(data, cls=StringEncoder))
        df = pd.json_normalize(output_dict)
        df.columns = self.change_column_names(self.object_name, df.columns)
        new_cols = [x for x in df.columns if not x.startswith('{}_Reference.'.format(self.object_name))]
        for col in new_cols:
            if col.endswith('_date'):
                df[col] = df[col].astype('datetime64[ns]')
        return df[new_cols]

    @staticmethod
    def change_column_names(name, columns):
        new_cols = []
        for col in columns:
            data_prefix = '{}_Data.'.format(name)

            if col.startswith(data_prefix):
                new_cols.append(col[len(data_prefix):].lower())
            else:
                new_cols.append(col)
        return new_cols

    def get_effective_dated_object(self, effdt, row):
        pass

    def get_all(self):
        return self.api_call({}, {})
