from zeep import helpers, Client
from zeep.wsse.username import UsernameToken
import json
import getpass
import yaml
import pandas as pd
from json_utils import WorkdayJSONEncoder, flatten_json


class WorkdayAPIObject(object):

    def __init__(self, object_name, config_file, effdt_col_name='effective_date'):
        self.object_name = object_name
        self.function = None
        self.effdt_col_name = effdt_col_name

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
        output_list = json.loads(json.dumps(data, cls=WorkdayJSONEncoder))
        flat_ref = [flatten_json(x['{}_Reference'.format(self.object_name)]) for x in output_list]
        flat_data = [flatten_json(x['{}_Data'.format(self.object_name)]) for x in output_list]
        df_ref = pd.json_normalize(flat_ref)
        df_data = pd.json_normalize(flat_data)

        df = pd.concat([df_ref, df_data], axis=1)
        df.columns = [x.lower() for x in df.columns]

        df = df.rename(columns={self.effdt_col_name: 'effective_date'})

        if 'effective_date' in df.columns:
            df['effective_date'] = pd.to_datetime(df['effective_date'], format='%Y-%m-%d').dt.date
        return df

    def get_effective_dated_object(self, effdt, row):
        pass

    def get_all(self):
        return self.api_call({}, {})
