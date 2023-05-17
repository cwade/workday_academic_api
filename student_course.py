from workday_api_object_effdt import WorkdayAPIObjectEffdt
from zeep import helpers
import json
from json_utils import StringEncoder
import pandas as pd


class StudentCourse(WorkdayAPIObjectEffdt):

    def __init__(self, config_file):
        super().__init__('Student_Course', config_file)
        self.function = self.service.Get_Student_Course
        
    def parse_json_to_df(self, json_list):
        data = []

        for result in json_list:
            s = helpers.serialize_object(result.Response_Data.Student_Course)
            for i in s:
                data.append(i['Student_Course_Data'])

        output_dict = json.loads(json.dumps(data, cls=StringEncoder))
        df = pd.json_normalize(output_dict)
        df.columns = self.change_column_names('Student_Course_Data_Snapshot', df.columns)

        for col in df.columns:
            if col.endswith('_date'):
                df[col] = df[col].astype('datetime64[ns]')
        return df

    def get_effective_dated_object(self, effdt, row):
        params = {'Request_References':
                      {'Student_Course_Reference':
                           {'ID': [{'_value_1': row['ID'],
                                    'type': 'Student_Course_ID'}]}},
                  'Request_Criteria': {'Effective_Date': effdt.strftime('%Y-%m-%d')}}
        return self.api_call({}, params)
