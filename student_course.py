from workday_api_object_effdt import WorkdayAPIObjectEffdt
from zeep import helpers
import json
from json_utils import WorkdayJSONEncoder
import pandas as pd


class StudentCourse(WorkdayAPIObjectEffdt):

    def __init__(self, config_file):
        super().__init__('Student_Course', config_file)
        self.function = self.service.Get_Student_Course
        self.effdt_col_name = 'student_course_data_snapshot_data_effective_date'

    def get_effective_dated_object(self, effdt, row):
        params = {'Request_References':
                      {'Student_Course_Reference':
                           {'ID': [{'_value_1': row['id'],
                                    'type': 'Student_Course_ID'}]}},
                  'Request_Criteria': {'Effective_Date': effdt.strftime('%Y-%m-%d')}}
        return self.api_call({}, params)
