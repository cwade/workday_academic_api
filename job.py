from worker import Worker
import yaml
import requests
import getpass
from requests.auth import HTTPBasicAuth
from lxml import etree
import pandas as pd
import numpy as np
from datetime import timedelta, date
import re
from json_utils import get_key_value_for_types, extract_sup_orgs


class Job:

    def __init__(self, hr_api_config_file, job_history_config_file):
        self.hr_api_config_file = hr_api_config_file
        self.job_history_config_file = job_history_config_file

    def get_current_jobs(self):
        w = Worker(self.hr_api_config_file)
        workers = w.get_all()
        return self.get_jobs_from_workers(workers)

    def get_jobs_for_workers_by_date(self, empl_ids, cw_ids, eff_date):
        w = Worker(self.hr_api_config_file)
        workers = w.get_workers_by_date(empl_ids, cw_ids, eff_date)
        return self.get_jobs_from_workers(workers)

    @staticmethod
    def get_jobs_from_workers(workers):
        job_list = []
        for i, row in workers.iterrows():
            for j, job in enumerate(row['job_data']):
                job['worker_id'] = row['worker_id']
                job['worker_id_type'] = row['worker_id_type']
                job['job_seq'] = j + 1
                job_list.append(job)

        final_cols = ['effective_date', 'worker_id', 'worker_id_type', 'job_seq', 'position_id', 'position_title',
                      'primary_job', 'business_title', 'position_start_date', 'position_end_date',
                      'end_employment_date', 'worker_type', 'time_type', 'pay_rate_type', 'job_exempt',
                      'scheduled_weekly_hours', 'default_weekly_hours', 'full_time_equivalent_percentage',
                      'specify_paid_fte', 'paid_fte', 'specify_working_fte', 'working_fte', 'exclude_from_headcount',
                      'job_profile_id', 'job_profile_name', 'manager_id', 'organization']
        if len(job_list) == 0:
            return pd.DataFrame(columns=final_cols)

        jobs = pd.json_normalize(job_list)

        jobs['organization'] = \
            jobs['Position_Management_Chains_Data.Position_Supervisory_Management_Chain_Data.Management_Chain_Data']. \
            apply(extract_sup_orgs)
        jobs['worker_type'] = jobs['Position_Data.Worker_Type_Reference.ID'].apply(
            lambda x: get_key_value_for_types(x, ['Employee_Type_ID', 'Contingent_Worker_Type_ID'], '_value_1'))
        jobs['time_type'] = jobs['Position_Data.Position_Time_Type_Reference.ID'].apply(
            lambda x: get_key_value_for_types(x, ['Position_Time_Type_ID'], '_value_1'))
        jobs['pay_rate_type'] = jobs['Position_Data.Pay_Rate_Type_Reference.ID'].apply(
            lambda x: get_key_value_for_types(x, ['Pay_Rate_Type_ID'], '_value_1') if type(x) == list else x)
        jobs['job_profile_id'] = jobs['Position_Data.Job_Profile_Summary_Data.Job_Profile_Reference.ID'].apply(
            lambda x: get_key_value_for_types(x, ['Job_Profile_ID'], '_value_1'))
        jobs['manager_id'] = jobs['Position_Data.Manager_as_of_last_detected_manager_change_Reference'].apply(
            lambda x: get_key_value_for_types(x[0]['ID'], ['Employee_ID', 'Contingent_Worker_ID'], '_value_1')
            if len(x) > 0 else np.nan)

        jobs.columns = [x.replace('Position_Data.', '').lower() for x in jobs.columns]

        jobs = jobs.rename(columns={
            'job_profile_summary_data.job_profile_name': 'job_profile_name',
            'start_date': 'position_start_date',
            'end_date': 'position_end_date'
        })

        jobs = jobs[final_cols]
        workers = workers.drop(columns=['job_data'])
        combo = workers.merge(jobs, how='left', on=['worker_id', 'worker_id_type'])

        combo['effective_date'] = pd.to_datetime(combo['effective_date'], format='%Y-%m-%d').dt.date
        combo = combo.sort_values(by=['worker_id', 'effective_date'])
        return combo

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
        xml_data = etree.fromstring(res.content)

        data = []
        report_entries = xml_data.xpath('//wd:Report_Entry', namespaces={'wd': 'urn:com.workday.report/Job_History'})
        for entry in report_entries:
            name = entry.xpath('./wd:Worker/@wd:Descriptor',
                               namespaces={'wd': 'urn:com.workday.report/Job_History'})[0]

            # Handle missing IDs
            worker_id_list = entry.xpath(
                './wd:Worker/wd:ID[@wd:type="Employee_ID" or @wd:type="Contingent_Worker_ID"]',
                namespaces={'wd': 'urn:com.workday.report/Job_History'})
            worker_id = worker_id_list[0].text
            id_type = worker_id_list[0].get('{urn:com.workday.report/Job_History}type')

            job_histories = entry.xpath('./wd:Job_History', namespaces={'wd': 'urn:com.workday.report/Job_History'})
            for job_history in job_histories:
                pos_descr = job_history.xpath('./wd:Position/@wd:Descriptor',
                                              namespaces={'wd': 'urn:com.workday.report/Job_History'})[0]
                pos_id = pos_descr[:pos_descr.find(' ')]
                process = job_history.xpath('./wd:Process/@wd:Descriptor',
                                            namespaces={'wd': 'urn:com.workday.report/Job_History'})[0]
                process = process[:process.find(':')]
                effdt = job_history.xpath('./wd:Effective_Date/text()',
                                          namespaces={'wd': 'urn:com.workday.report/Job_History'})[0]
                if id_type == 'Employee_ID':
                    data.append((name, worker_id, np.nan, pos_id, pos_descr, effdt, process))

        h = pd.DataFrame(data, columns=['name', 'employee_id', 'contingent_worker_id', 'position_id',
                                        'pos_descr', 'effective_date', 'process'])
        h['effective_date'] = pd.to_datetime(h['effective_date'], format='%Y-%m-%d-%H:%M').dt.date

        # We want to do an additional job row fetch for the day before each job move
        # These are rows resulting another position ending, so need different handling
        moves = h[h['process'].isin(['Promotion', 'Promote In', 'Lateral Move', 'Transfer'])].copy(deep=True)
        moves['effective_date'] = moves['effective_date'] - timedelta(days=1)
        moves['process'] = moves['process'] + ' day before'

        h = pd.concat([h, moves]).reset_index()

        h.loc[h['employee_id'].notnull(), 'worker_id_type'] = 'Employee_ID'
        h.loc[h['employee_id'].notnull(), 'worker_id'] = h['employee_id']
        h.loc[h['contingent_worker_id'].notnull(), 'worker_id_type'] = 'Contingent_Worker_ID'
        h.loc[h['contingent_worker_id'].notnull(), 'worker_id'] = h['contingent_worker_id']

        h = h.drop(columns=['employee_id', 'contingent_worker_id'])

        # Separate the termination rows because they're handled differently
        t = h[h['process'].isin(['Terminate', 'End Contract'])]

        # Then h is everything but the termination rows
        h = h[~h['process'].isin(['Terminate', 'End Contract'])]

        condensed = []
        for data, cols in h.groupby(['name', 'worker_id', 'worker_id_type', 'position_id',
                                     'pos_descr', 'effective_date']):
            seen = set()
            seen_add = seen.add
            p = ', '.join([x for x in cols['process'] if not (x in seen or seen_add(x))])
            condensed.append(list(data) + [p])

        new_h = pd.DataFrame(condensed, columns=['name', 'worker_id', 'worker_id_type', 'position_id',
                                                 'pos_descr', 'effective_date', 'processes'])
        new_h = new_h.sort_values(by=['worker_id', 'effective_date'])

        job_move_end_dates = new_h.copy(deep=True)
        job_move_end_dates['prev_pos'] = job_move_end_dates['position_id'].shift(1)
        job_move_end_dates.loc[job_move_end_dates['processes'].str.contains(' day before'), 'position_id'] = \
            job_move_end_dates['prev_pos']
        job_move_end_dates = job_move_end_dates[job_move_end_dates['processes'].str.contains(' day before')]
        job_move_end_dates = job_move_end_dates[['worker_id', 'worker_id_type', 'position_id', 'effective_date']]

        new_h.to_csv('new_h.csv')
        job_move_end_dates.to_csv('job_move_end_dates.csv')

        return new_h.drop_duplicates(), t, job_move_end_dates

    def get_job_history(self):
        jobs = self.get_current_jobs()
        jobs['handled'] = 1
        ch, terminations, job_move_end_dates = self.get_job_change_dates()

        ch = ch.merge(jobs[['effective_date', 'worker_id', 'worker_id_type', 'position_id', 'handled']],
                      how='left', on=['effective_date', 'worker_id', 'worker_id_type', 'position_id'])
        ch['handled'] = ch['handled'].fillna(0)
        todo = ch[ch['handled'] == 0]

        job_rows_to_add = []
        for eff_date, workers in todo.groupby('effective_date'):
            empl_ids = list(workers[workers['worker_id_type'] == 'Employee_ID']['worker_id'].dropna())
            cw_ids = list(workers[workers['worker_id_type'] == 'Contingent_Worker_ID']['worker_id'].dropna())
            addtl_rows = self.get_jobs_for_workers_by_date(empl_ids, cw_ids, eff_date)
            job_rows_to_add.append(addtl_rows)

        new_jobs = pd.concat([jobs.drop(columns=['handled'])] + job_rows_to_add).reset_index()

        jbs = new_jobs.sort_values(by=['worker_id', 'effective_date'])
        jbs = jbs.drop(columns=['index']).drop_duplicates()

        jbs = jbs.merge(ch[['effective_date', 'worker_id', 'worker_id_type', 'position_id', 'processes']],
                        how='left',
                        on=['effective_date', 'worker_id', 'worker_id_type', 'position_id'])

        # Define a function to transform each group
        def transform_group(group):
            group['valid_to'] = group['effective_date'].shift(-1)
            group['valid_to'] = group['valid_to'].fillna(date(2099, 12, 31)) - timedelta(days=1)
            group['valid_from'] = group['effective_date']
            return group

        # Apply the function to each group
        jbs = jbs.groupby(['worker_id', 'position_id']).apply(transform_group)
        jbs.loc[jbs['termination_date'].notnull(), 'valid_to'] = jbs['termination_date']
        jbs['valid_to'] = jbs['valid_to'].astype('datetime64[ns]').dt.date

        # For every termination row, fill in the termination date and fix the valid to date
        for i, term_row in terminations.iterrows():
            jbs.loc[(jbs['worker_id'] == term_row['worker_id']) &
                    (jbs['worker_id_type'] == term_row['worker_id_type']) &
                    (jbs['position_id'] == term_row['position_id']) &
                    (jbs['valid_to'] > term_row['effective_date']), 'termination_date'] = term_row['effective_date']
            jbs.loc[(jbs['worker_id'] == term_row['worker_id']) &
                    (jbs['worker_id_type'] == term_row['worker_id_type']) &
                    (jbs['position_id'] == term_row['position_id']) &
                    (jbs['valid_to'] > term_row['effective_date']), 'valid_to'] = term_row['effective_date']

        # Fix end dates for promotions and job moves
        for i, row in job_move_end_dates.iterrows():
            jbs.loc[(jbs['worker_id'] == row['worker_id']) & (jbs['worker_id_type'] == row['worker_id_type']) &
                    (jbs['position_id'] == row['position_id']) &
                    (jbs['valid_to'] == date(2099, 12, 30)), 'valid_to'] = row['effective_date']

        jbs['current_row'] = (jbs['valid_to'] == date(2099, 12, 30))

        first_cols = ['valid_from', 'valid_to', 'current_row', 'worker_id', 'worker_id_type']
        rest_of_cols = [x for x in jbs.columns if x not in first_cols]

        # Reorder columns
        jbs = jbs[first_cols + rest_of_cols]

        return jbs.sort_values(['worker_id', 'valid_from', 'job_seq'])
