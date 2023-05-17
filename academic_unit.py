from workday_api_object_effdt import WorkdayAPIObjectEffdt


class AcademicUnit(WorkdayAPIObjectEffdt):

    def __init__(self, config_file):
        super().__init__('Academic_Unit', config_file)
        self.function = self.service.Get_Academic_Units

    def get_effective_dated_object(self, effdt, row):
        resp_filter = {'As_Of_Effective_Date': effdt.strftime('%Y-%m-%d')}
        params = {'Request_Criteria':
                      {'Effective_As_Of_Date': effdt.strftime('%Y-%m-%d'),
                       '{}_Name'.format(self.object_name): row['name']}}
        return self.api_call(resp_filter, params)
