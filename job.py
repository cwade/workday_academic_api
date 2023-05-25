from worker import Worker
import yaml
import requests
import getpass
from requests.auth import HTTPBasicAuth
import xmltodict
import pandas as pd
import numpy as np
from datetime import timedelta, date
import re


class Job:

    def __init__(self, hr_api_config_file, job_history_config_file):
        self.hr_api_config_file = hr_api_config_file
        self.job_history_config_file = job_history_config_file

    @staticmethod
    def get_job_change_info(job_hist_entry):
        # Split position into id and description
        i = job_hist_entry['wd:Position']['@wd:Descriptor'].find(' ')
        position_id = job_hist_entry['wd:Position']['@wd:Descriptor'][:i]
        descr = job_hist_entry['wd:Position']['@wd:Descriptor'][i+1:]
        proc = job_hist_entry['wd:Process']['@wd:Descriptor']
        return [position_id, descr,
                job_hist_entry['wd:Effective_Date'],
                proc[0:proc.find(':')]]

    @staticmethod
    def extract_jobs_from_worker_df(workers):

        workers = workers.rename(
            columns={'id_1__value_1': 'id',
                     'id_1_type': 'id_type',
                     'personal_data_name_data_preferred_name_data_name_detail_data_reporting_name': 'name',
                     'employment_data_worker_status_data_active': 'worker_is_active',
                     'employment_data_worker_status_data_terminated': 'worker_was_terminated',
                     'employment_data_worker_status_data_termination_date': 'termination_date',
                     'employment_data_worker_status_data_hire_date': 'hire_date',
                     'employment_data_worker_status_data_original_hire_date': 'original_hire_date',
                     'employment_data_worker_status_data_end_employment_date': 'employment_end_date'})
        index_vars = ['id', 'id_type', 'worker_id', 'name', 'worker_is_active',
                      'worker_was_terminated', 'termination_date', 'hire_date', 'original_hire_date',
                      'employment_end_date']

        job_data_cols = [x for x in workers.columns if x.startswith('employment_data_worker_job_data_')]
        job_info = workers[index_vars + job_data_cols]

        # Preprocessing the dataframe column names
        job_info.columns = job_info.columns.str.replace('employment_data_worker_job_data_', '')

        # Get the distinct column numbers
        groups = np.unique([x[:x.find('_')] for x in job_info.columns
                            if x not in index_vars])

        # Dividing the data into different dataframes based on the group number in column names
        column_sets = []
        df_list = []
        for group in groups:
            prefix = group + '_'
            cols = [x for x in job_info.columns if x.startswith(prefix)]
            group_data = job_info[index_vars + cols]
            group_data.columns = [re.sub(r'^' + prefix, '', x) for x in group_data.columns]
            df_list.append(group_data)
            column_sets.append(set(group_data.columns))

        if len(column_sets) == 0:
            return pd.DataFrame(columns = ['effective_date'] + index_vars + \
                ['position_start_date', 'position_end_date', 'position_id', 'position_title',
                 'business_title', 'primary_job', 'worker_type', 'time_type', 'job_exempt', 'scheduled_weekly_hours',
                 'default_weekly_hours', #'pay_rate_type',
                 'full_time_equivalent_percentage', 'specify_paid_fte',
                 'paid_fte', 'specify_working_fte', 'working_fte', 'exclude_from_headcount', 'job_profile_id'])

        common_cols = set.intersection(*column_sets)

        jobs = pd.concat([x[list(common_cols)] for x in df_list])

        # Remove the position_data_ prefix from column names
        prefix = 'position_data_'
        jobs.columns = [x.replace(prefix, '') for x in jobs.columns]

        jobs = jobs.rename(columns={'start_date': 'position_start_date',
                                    'end_employment_date': 'position_end_date',
                                    'worker_type_reference_id_1__value_1': 'worker_type',
                                    'position_time_type_reference_id_1__value_1': 'time_type',
                                    'pay_rate_type_reference_id_1__value_1': 'pay_rate_type',
                                    'job_classification_summary_data_1_job_group_reference_id_1__value_1':
                                        'job_classification',
                                    'job_profile_summary_data_job_profile_reference_id_1__value_1':
                                        'job_profile_id'
                                    })

        jobs.to_csv('first_version_of_jobs.csv', index=False)

        cols_to_keep = ['effective_date'] + index_vars + \
            ['position_start_date', 'position_end_date', 'position_id', 'position_title',
             'business_title', 'primary_job', 'worker_type', 'time_type', 'job_exempt', 'scheduled_weekly_hours',
             'default_weekly_hours', #'pay_rate_type',
             'full_time_equivalent_percentage', 'specify_paid_fte',
             'paid_fte', 'specify_working_fte', 'working_fte', 'exclude_from_headcount', 'job_profile_id']

        jobs = jobs[cols_to_keep]

        jobs = jobs[jobs['position_id'].notnull()]
        jobs = jobs.sort_values(by=['id', 'effective_date'])

        jobs['effective_date'] = pd.to_datetime(jobs['effective_date'], format='%Y-%m-%d').dt.date
        return jobs

    def get_current_jobs(self):
        w = Worker(self.hr_api_config_file)
        workers = w.get_all()
        jobs = Job.extract_jobs_from_worker_df(workers)
        return jobs

    def get_jobs_for_workers_by_date(self, empl_ids, cw_ids, eff_date):
        w = Worker(self.hr_api_config_file)
        workers = w.get_workers_by_date(empl_ids, cw_ids, eff_date)
        return Job.extract_jobs_from_worker_df(workers)

    def get_job_change_dates(self):

        # Fetch the Workday standard report called Job History
        with open(self.job_history_config_file, 'r') as ymlfile:
            config = yaml.load(ymlfile, yaml.FullLoader)

        username = config['username']
        report_url = config['report_url']

        if 'password' in config:
            password = config['password']
        else:
            password = getpass.getpass('Enter password for {}: '.format(username))

        res = requests.get(report_url, auth=HTTPBasicAuth(username, password))
        res_dict = xmltodict.parse(res.content)

        # Parse out the columns we want (name, worker ids, position id, effective date, and
        # description of what changed)
        # this is pretty messy and there's probably a much easier way, but it gets the job done
        data = []
        d = res_dict['wd:Report_Data']['wd:Report_Entry']

        for i in range(len(d)):
            name = d[i]['wd:Worker']['@wd:Descriptor']
            ids = d[i]['wd:Worker']['wd:ID']
            empl_ids = [x for x in ids if x['@wd:type'] == 'Employee_ID']
            cw_ids = [x for x in ids if x['@wd:type'] == 'Contingent_Worker_ID']

            if len(empl_ids) == 1:
                empl_id = empl_ids[0]['#text']
            else:
                empl_id = None
            if len(cw_ids) == 1:
                cw_id = cw_ids[0]['#text']
            else:
                cw_id = None

            if len(empl_ids) > 1 or len(cw_ids) > 1:
                raise(Exception('Employee has more ids than expected: {}'.format(empl_ids + cw_ids)))

            constants = [name, empl_id, cw_id]
            if type(d[i]['wd:Job_History']) == list:
                for j in range(len(d[i]['wd:Job_History'])):
                    data.append(constants + Job.get_job_change_info(d[i]['wd:Job_History'][j]))
            else:
                data.append(constants + Job.get_job_change_info(d[i]['wd:Job_History']))

        h = pd.DataFrame(data, columns=['name', 'employee_id', 'contingent_worker_id', 'position_id',
                                        'pos_descr', 'effective_date', 'process'])
        h['effective_date'] = pd.to_datetime(h['effective_date'], format='%Y-%m-%d-%H:%M').dt.date

        # We want to do an additional job row fetch for the day before each job move
        # - maybe for each termination too
        # These are rows resulting another position ending, so need different handling
        moves = h[h['process'].isin(['Promotion', 'Promote In', 'Lateral Move'])].copy(deep=True)
        moves['effective_date'] = moves['effective_date'] - timedelta(days=1)
        moves['process'] = moves['process'] + ' day before'

        h = pd.concat([h, moves]).reset_index()

        h.loc[h['employee_id'].notnull(), 'id_type'] = 'Employee_ID'
        h.loc[h['employee_id'].notnull(), 'id'] = h['employee_id']
        h.loc[h['contingent_worker_id'].notnull(), 'id_type'] = 'Contingent_Worker_ID'
        h.loc[h['contingent_worker_id'].notnull(), 'id'] = h['contingent_worker_id']

        h = h.drop(columns=['employee_id', 'contingent_worker_id'])

        # Separate the termination rows because they're handled differently
        t = h[h['process'].isin(['Terminate', 'End Contract'])]

        # Then h is everything but the termination rows
        h = h[~h['process'].isin(['Terminate', 'End Contract'])]

        condensed = []
        for data, cols in h.groupby(['name', 'id', 'id_type', 'position_id',
                                     'pos_descr', 'effective_date']):
            p = ', '.join([x for x in set(cols['process'])])
            condensed.append(list(data) + [p])

        new_h = pd.DataFrame(condensed, columns=['name', 'id', 'id_type', 'position_id',
                                                 'pos_descr', 'effective_date', 'processes'])

        job_move_end_dates = new_h.copy(deep=True)
        job_move_end_dates['prev_pos'] = job_move_end_dates['position_id'].shift(1)
        job_move_end_dates.loc[job_move_end_dates['processes'].str.contains(' day before'), 'position_id'] = \
            job_move_end_dates['prev_pos']
        job_move_end_dates = job_move_end_dates[job_move_end_dates['processes'].str.contains(' day before')]
        job_move_end_dates = job_move_end_dates[['id', 'id_type', 'position_id', 'effective_date']]

        new_h.to_csv('new_h.csv')

        return (new_h.drop_duplicates(), t, job_move_end_dates)

    def get_job_history(self):
        jobs = self.get_current_jobs()
        jobs['handled'] = 1
        ch, terminations, job_move_end_dates = self.get_job_change_dates()

        ch = ch.merge(jobs[['effective_date', 'id', 'id_type', 'position_id', 'handled']],
                      how='left', on=['effective_date', 'id', 'id_type', 'position_id'])
        ch['handled'] = ch['handled'].fillna(0)
        todo = ch[ch['handled'] == 0]

        job_rows_to_add = []
        for eff_date, workers in todo.groupby('effective_date'):
            empl_ids = list(workers[workers['id_type']=='Employee_ID']['id'].dropna())
            cw_ids = list(workers[workers['id_type']=='Contingent_Worker_ID']['id'].dropna())
            addtl_rows = self.get_jobs_for_workers_by_date(empl_ids, cw_ids, eff_date)
            job_rows_to_add.append(addtl_rows)

        new_jobs = pd.concat([jobs.drop(columns=['handled'])] + job_rows_to_add)
        jbs = new_jobs.sort_values(by=['id', 'effective_date']).drop_duplicates().reset_index().drop(columns=['index'])

        jbs = jbs.merge(ch[['effective_date', 'id', 'id_type', 'position_id', 'processes']],
                        how='left',
                        on=['effective_date', 'id', 'id_type', 'position_id'])

        # Define a function to transform each group
        def transform_group(group):
            group['valid_to'] = group['effective_date'].shift(-1)
            group['valid_to'] = group['valid_to'].fillna(date(2099, 12, 31)) - timedelta(days=1)
            group['valid_from'] = group['effective_date']
            return group

        # Apply the function to each group
        jbs = jbs.groupby(['id', 'position_id']).apply(transform_group)
        jbs.loc[jbs['termination_date'].notnull(), 'valid_to'] = jbs['termination_date']
        jbs['valid_to'] = jbs['valid_to'].astype('datetime64[ns]').dt.date

        # For every termination row, fill in the termination date and fix the valid to date
        print(jbs['valid_to'].dtype, terminations['effective_date'].dtype)
        for i, term_row in terminations.iterrows():
            print(term_row['effective_date'])
            print(type(term_row['effective_date']))
            jbs.loc[(jbs['id']==term_row['id']) &
                    (jbs['id_type']==term_row['id_type']) &
                    (jbs['position_id']==term_row['position_id']) &
                    (jbs['valid_to'] > term_row['effective_date']), 'termination_date'] = term_row['effective_date']
            jbs.loc[(jbs['id']==term_row['id']) &
                    (jbs['id_type']==term_row['id_type']) &
                    (jbs['position_id']==term_row['position_id']) &
                    (jbs['valid_to'] > term_row['effective_date']), 'valid_to'] = term_row['effective_date']

        # Fix end dates for promotions and job moves
        for i, row in job_move_end_dates.iterrows():
            jbs.loc[(jbs['id']==row['id']) & (jbs['id_type']==row['id_type']) &
                    (jbs['position_id']==row['position_id']) &
                    (jbs['valid_to'] == date(2099, 12, 30)), 'valid_to'] = row['effective_date']

        jbs['current_row'] = (jbs['valid_to'] == date(2099, 12, 30))

        # Reorder columns
        jbs = jbs[['valid_from', 'valid_to', 'current_row', 'id', 'id_type', 'worker_id', 'name', 'worker_is_active',
                   'worker_was_terminated', 'termination_date', 'hire_date', 'original_hire_date',
                   'employment_end_date',
                   'position_start_date', 'position_end_date', 'position_id',
                   'position_title', 'business_title', 'primary_job', 'worker_type', 'time_type', 'job_exempt',
                   'scheduled_weekly_hours', 'default_weekly_hours', #'pay_rate_type',
                   'full_time_equivalent_percentage', 'specify_paid_fte', 'paid_fte', 'specify_working_fte',
                   'working_fte', 'exclude_from_headcount', 'job_profile_id', 'processes']]

        return jbs.sort_values(['id', 'valid_from', 'valid_to'])
