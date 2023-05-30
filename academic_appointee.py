from workday_api_object import WorkdayAPIObject
from zeep import helpers
import pandas as pd
from json_utils import get_key_value_for_types


class AcademicAppointee(WorkdayAPIObject):

    def __init__(self, config_file):
        super().__init__('Academic_Appointee', config_file)
        self.function = self.service.Get_Academic_Appointee

    def get_all(self):
        r = self.api_call({}, {'Response_Group': {
            'Include_Person_Name_Data': True,
            'Include_Personal_Information_Data': True,
            'Include_Appointment_Data': True}})
        return(r)

    def get_appointees_by_date(self, person_ids, id_types, eff_date):
        person_ref = []
        for id, id_type in zip(person_ids, id_types):
            person_ref.append({'ID': {'_value_1': id, 'type': id_type}})

        other_params = {'Request_References': {'Academic_Appointee_Reference': person_ref},
                        'Response_Group': {
                            'Include_Person_Name_Data': True,
                            'Include_Personal_Information_Data': True,
                            'Include_Appointment_Data': True}}
        resp_filters = {'As_Of_Effective_Date': eff_date.strftime('%Y-%m-%d')}
        return self.api_call(resp_filters, other_params)

    def parse_json_to_df(self, json_list):
        ref_list = []
        data_list = []
        for x in json_list:
            for y in x.Response_Data.Academic_Appointee:
                ref_list.append(helpers.serialize_object(y.Academic_Appointee_Reference))
                data_list.append(helpers.serialize_object(y.Academic_Appointee_Data))

        ref_df = pd.json_normalize(ref_list)
        data_df = pd.json_normalize(data_list)
        df = pd.concat([ref_df, data_df], axis=1)
        df = df.rename(columns={0: 'Academic_Appointee_Reference'})


        df['person_id'] = df['Academic_Appointee_Reference'].apply(
            lambda x: get_key_value_for_types(x['ID'],
                                              ['Employee_ID', 'Contingent_Worker_ID', 'Student_ID',
                                               'Academic_Affiliate_ID'], '_value_1'))
        df['person_id_type'] = df['Academic_Appointee_Reference'].apply(
            lambda x: get_key_value_for_types(x['ID'], ['Employee_ID', 'Contingent_Worker_ID', 'Student_ID',
                                                  'Academic_Affiliate_ID'], 'type'))
        df['gender'] = df['Personal_Information_Data.Gender_Reference.ID'].apply(
            lambda x: get_key_value_for_types(x, ['Gender_Code'], '_value_1'))

        def get_values_for_elem(e, etype):
            re = []
            for race_eth_obj in e:
                for d in race_eth_obj['ID']:
                    if d['type'] == etype:
                        re.append(d['_value_1'])
            return(', '.join(re))

        df['race'] = df['Personal_Information_Data.Ethnicity_Reference'].apply(
            lambda x: get_values_for_elem(x, 'Ethnicity_ID'))
        df['citz_status'] = df['Personal_Information_Data.Citizenship_Reference'].apply(
            lambda x: get_values_for_elem(x, 'Citizenship_Status_Code'))

        df = df.rename(columns={
            'Person_Data.Preferred_Name_Data.Name_Detail_Data.First_Name': 'first_name',
            'Person_Data.Preferred_Name_Data.Name_Detail_Data.Last_Name': 'last_name',
            'Person_Data.Preferred_Name_Data.Name_Detail_Data.Reporting_Name': 'name',
            'Appointment_Data': 'appointment_data'
        })

        df = df[['person_id', 'person_id_type', 'name', 'first_name', 'last_name', 'gender', 'race',
                 'citz_status', 'appointment_data']]
        return df