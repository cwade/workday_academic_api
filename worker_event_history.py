from workday_api_object import WorkdayAPIObject
from zeep import helpers
import json
from json_utils import WorkdayJSONEncoder, flatten_json
import pandas as pd


class WorkerEventHistory(WorkdayAPIObject):

    def __init__(self, config_file):
        super().__init__('Worker_Event_History', config_file)
        self.function = self.service.Get_Worker_Event_History

    def get_worker_history(self, id, is_contingent=False):
        if is_contingent:
            worker_type = 'Contingent_Worker'
        else:
            worker_type = 'Employee'

        param = {'Worker_Reference': {'{}_Reference'.format(worker_type):
            {'Integration_ID_Reference': {'ID': {'_value_1': id, 'System_ID': 'WD-EMPLID'}}}}}

        result = self.function(**param)
        r = self.parse_json_to_df(result)
        r.to_csv('worker_hist_{}.csv'.format(id), index=False)
        return(r)

    def parse_json_to_df(self, result):
        data = []
        for r in result['Worker_Event_History_Data']['Event_Data']:
            s = helpers.serialize_object(r)
            data.append(s)
        output_list = json.loads(json.dumps(data, cls=WorkdayJSONEncoder))
        flat_events = [flatten_json(x) for x in output_list]
        events = pd.json_normalize(flat_events)
        events.columns = [x.lower() for x in events.columns]

        return events


