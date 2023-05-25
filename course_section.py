from workday_api_object import WorkdayAPIObject
from zeep import helpers
import json
from json_utils import WorkdayJSONEncoder
import pandas as pd


class CourseSection(WorkdayAPIObject):

    def __init__(self, config_file):
        super().__init__('Course_Section', config_file)
        self.function = self.service.Get_Course_Sections
