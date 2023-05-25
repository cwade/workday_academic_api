from workday_api_object_effdt import WorkdayAPIObjectEffdt
from workday_api_object import WorkdayAPIObject
from zeep import helpers
import json
from json_utils import WorkdayJSONEncoder
import pandas as pd

#class Worker(WorkdayAPIObjectEffdt):
class Worker(WorkdayAPIObject):

    def __init__(self, config_file):
        super().__init__('Worker', config_file)
        self.function = self.service.Get_Workers

    def get_all(self):
        r = self.api_call({}, {'Response_Group': {'Include_Additional_Jobs': True,
                                                     'Include_Reference': True,
                                                     'Include_Personal_Information': True,
                                                     'Include_Employment_Information': True,
                                                     'Include_Compensation': False,
                                                     'Include_Organizations': False,
                                                     'Include_Roles': False,
                                                     'Include_Transaction_Log_Data': False,
                                                     'Include_Employee_Contract_Data': True}})
        r.to_csv('workers.csv', index=False)
        # r = pd.read_csv('workers.csv', low_memory=False)
        return(r)

    def get_workers_by_date(self, empl_ids, cw_ids, eff_date):
        worker_ref = []
        for id in empl_ids:
            worker_ref.append({'ID': {'_value_1': id, 'type': 'Employee_ID'}})
        for id in cw_ids:
            worker_ref.append({'ID': {'_value_1': id, 'type': 'Contingent_Worker_ID'}})
        other_params = {'Request_References': {'Worker_Reference': worker_ref},
                        'Response_Group': {'Include_Additional_Jobs': True,
                                           'Include_Reference': True,
                                           'Include_Personal_Information': True,
                                           'Include_Employment_Information': True,
                                           'Include_Compensation': False,
                                           'Include_Organizations': False,
                                           'Include_Roles': False,
                                           'Include_Transaction_Log_Data': False,
                                           'Include_Employee_Contract_Data': True}}
        resp_filters = {'As_Of_Effective_Date': eff_date.strftime('%Y-%m-%d')}
        return self.api_call(resp_filters, other_params)
