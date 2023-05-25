from workday_api_object import WorkdayAPIObject
from datetime import timedelta
import pandas as pd

class WorkdayAPIObjectEffdt(WorkdayAPIObject):

    def __init__(self, obj_name, config_file, effdt_col_name='effective_date'):
        super().__init__(obj_name, config_file)

    def api_call_with_effdt_history(self, min_effdt, max_to_date):
        df = self.api_call({}, {})
        df['to_date'] = max_to_date

        new_data = []

        for i, row in df[df['effective_date'] > min_effdt].iterrows():
            prev_date = row['effective_date'] - timedelta(1)
            while prev_date >= min_effdt:
                x = self.get_effective_dated_object(prev_date, row)
                if len(x) == 0:
                    break
                else:
                    x['to_date'] = prev_date
                    new_data.append(x)
                    prev_date = x['effective_date'][0] - timedelta(1)

        obj_hist = pd.concat([df] + new_data, ignore_index=True)
        obj_hist.rename(columns={'effective_date': 'from_date'}, inplace=True)
        return obj_hist

    def get_all(self, min_effdt, max_to_date):
        return self.api_call_with_effdt_history(min_effdt, max_to_date)