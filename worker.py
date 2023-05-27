from workday_api_object_effdt import WorkdayAPIObjectEffdt
from workday_api_object import WorkdayAPIObject
from zeep import helpers
import json
from json_utils import WorkdayJSONEncoder, get_key_value_for_types
import pandas as pd
import numpy as np


class Worker(WorkdayAPIObject):

    def __init__(self, config_file):
        super().__init__('Worker', config_file)
        self.function = self.service.Get_Workers

    def get_all(self):
        r = self.api_call({}, {'Response_Group': {
            'Include_Additional_Jobs': True,
            'Include_Reference': True,
            'Include_Personal_Information': True,
            'Include_Employment_Information': True,
            'Include_Compensation': False,
            'Include_Organizations': False,
            'Include_Roles': False,
            'Include_Transaction_Log_Data': False,
            'Include_Employee_Contract_Data': False,
            'Include_Management_Chain_Data': True}})
        # r.to_csv('workers.csv', index=False)
        return(r)

    def get_workers_by_date(self, empl_ids, cw_ids, eff_date):
        worker_ref = []
        for id in empl_ids:
            worker_ref.append({'ID': {'_value_1': id, 'type': 'Employee_ID'}})
        for id in cw_ids:
            worker_ref.append({'ID': {'_value_1': id, 'type': 'Contingent_Worker_ID'}})
        other_params = {'Request_References': {'Worker_Reference': worker_ref},
                        'Response_Group': {
                            'Include_Additional_Jobs': True,
                            'Include_Reference': True,
                            'Include_Personal_Information': True,
                            'Include_Employment_Information': True,
                            'Include_Compensation': False,
                            'Include_Organizations': False,
                            'Include_Roles': False,
                            'Include_Transaction_Log_Data': False,
                            'Include_Employee_Contract_Data': False,
                            'Include_Management_Chain_Data': True}}
        resp_filters = {'As_Of_Effective_Date': eff_date.strftime('%Y-%m-%d')}
        return self.api_call(resp_filters, other_params)

    def parse_json_to_df(self, json_list):
        list_of_lists = [helpers.serialize_object(x.Response_Data.Worker) for x in json_list]
        data = [item for sublist in list_of_lists for item in sublist]

        workers = pd.json_normalize(data)
        workers['worker_id'] = workers['Worker_Reference.ID'].apply(
            lambda x: get_key_value_for_types(x, ['Employee_ID', 'Contingent_Worker_ID'], '_value_1'))
        workers['worker_id_type'] = workers['Worker_Reference.ID'].apply(
            lambda x: get_key_value_for_types(x, ['Employee_ID', 'Contingent_Worker_ID'], 'type'))

        workers = workers.rename(columns={
            'Worker_Data.Personal_Data.Name_Data.Preferred_Name_Data.Name_Detail_Data.Formatted_Name': 'name',
            'Worker_Data.Personal_Data.Name_Data.Preferred_Name_Data.Name_Detail_Data.First_Name': 'first_name',
            'Worker_Data.Personal_Data.Name_Data.Preferred_Name_Data.Name_Detail_Data.Last_Name': 'last_name',
            'Worker_Data.Employment_Data.Worker_Job_Data': 'job_data',
            'Worker_Data.Employment_Data.Worker_Status_Data.Active': 'worker_is_active',
            'Worker_Data.Employment_Data.Worker_Status_Data.Hire_Date': 'hire_date',
            'Worker_Data.Employment_Data.Worker_Status_Data.Original_Hire_Date': 'original_hire_date',
            'Worker_Data.Employment_Data.Worker_Status_Data.Rehire': 'rehire',
            'Worker_Data.Employment_Data.Worker_Status_Data.Terminated': 'worker_was_terminated',
            'Worker_Data.Employment_Data.Worker_Status_Data.Termination_Date': 'termination_date',
            'Worker_Data.Employment_Data.Worker_Status_Data.End_Employment_Date': 'employment_end_date'
        })

        workers = workers[['worker_id', 'worker_id_type', 'name', 'first_name', 'last_name', 'job_data',
                           'worker_is_active', 'hire_date', 'original_hire_date', 'worker_was_terminated',
                           'termination_date', 'employment_end_date', 'rehire']]
        return workers
