from workday_api_object import WorkdayAPIObject
from zeep import helpers
import json
from json_utils import StringEncoder
import pandas as pd


class CourseSection(WorkdayAPIObject):

    def __init__(self, config_file):
        super().__init__('Course_Section', config_file)
        self.function = self.service.Get_Course_Sections
        
    def parse_json_to_df(self, json_list):
        data = []

        for result in json_list:
            s = helpers.serialize_object(result.Response_Data.Course_Section)
            for i in s:
                data.append(i['Course_Section_Data'])

        output_dict = json.loads(json.dumps(data, cls=StringEncoder))
        df = pd.json_normalize(output_dict)
        df.columns = self.change_column_names('Course_Section_Data', df.columns)

        for col in df.columns:
            if col.endswith('_date'):
                df[col] = df[col].astype('datetime64[ns]')

        return df
