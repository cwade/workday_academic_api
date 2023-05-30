from academic_appointee import AcademicAppointee
from json_utils import get_key_value_for_types
import pandas as pd
import numpy as np
from xml_utils import get_xml_report

class AcademicAppointment:

    def __init__(self, hr_api_config_file, appt_hist_config_file):
        self.hr_api_config_file = hr_api_config_file
        self.appt_hist_config_file = appt_hist_config_file

    def get_most_recent_appointments(self):
        # Gets the most recent academic appointment for every academic appointee, including inactive ones
        apte = AcademicAppointee(self.hr_api_config_file)
        appointees = apte.get_all()
        return self.get_appointments_from_appointees(appointees)

    def get_appointments_by_date(self, person_ids, id_types, eff_date):
        apte = AcademicAppointee(self.hr_api_config_file)
        appts = apte.get_appointees_by_date(person_ids, id_types, eff_date)
        return self.get_appointments_from_appointees(appts)

    @staticmethod
    def get_appointments_from_appointees(appointees):
        appt_list = []
        for i, row in appointees.iterrows():
            for j, appt in enumerate(row['appointment_data']):
                appt['person_id'] = row['person_id']
                appt['person_id_type'] = row['person_id_type']
                appt['appt_seq'] = j + 1
                appt_list.append(appt)

        if len(appt_list) == 0:
            return

        appts = pd.json_normalize(appt_list)

        appts = appts.rename(columns={
            'Appointment_Track_ID': 'track_id',
            'Appointment_Track_Inactive': 'track_is_inactive',
            'Roster_Percent': 'roster_percent',
            'Appointment_Start_Date': 'start_date',
            'Appointment_End_Date': 'end_date',
            'Appointment_Title': 'title',
            'Tenure_Award_Date': 'tenure_award_date'})

        vals_to_extract = []

        vals_that_may_or_may_not_be_present = [
            ['track_type', 'Track_Type_Reference.ID', ['Academic_Track_Type_ID']],
            ['appointment_identifier_id', 'Appointment_Identifier_Reference.ID', ['Academic_Appointment_Identifier_ID']],
            ['academic_unit_id', 'Academic_Unit_Reference.ID', ['Academic_Unit_ID']],
            ['academic_rank', 'Rank_Reference.ID', ['Academic_Rank_ID']],
            ['tenure_status', 'Tenure_Status_Reference.ID', ['Academic_Tenure_Status_ID']],
            ['tenure_home', 'Tenure_Home_Reference.ID', ['Academic_Unit_ID']],
            ['named_prof_id', 'Named_Professorship_Reference.ID', ['Named_Professorship_ID']],
            ['position_id', 'Position_Reference.ID', ['Position_ID']]]

        for new_var, old_var, elems in vals_that_may_or_may_not_be_present:
            if old_var in appts:
                vals_to_extract.append([new_var, old_var, elems])
            else:
                appts[new_var] = np.nan

        for new_var, old_var, elems in vals_to_extract:
            appts[new_var] = appts[old_var].apply(lambda x: get_key_value_for_types(x, elems))

        final_cols = ['start_date', 'end_date', 'track_id', 'track_is_inactive', 'person_id',
                      'person_id_type', 'appt_seq', 'title', 'position_id', 'academic_unit_id',
                      'academic_rank',  'roster_percent', 'track_type', 'tenure_status',
                      'tenure_award_date', 'tenure_home', 'appointment_identifier_id', 'named_prof_id']
        combo = appointees.merge(appts[final_cols], how='left', on=['person_id', 'person_id_type'])
        return combo

    def get_appointment_change_dates(self):

        # Fetch the Workday report called All Faculty Appointment History
        xml_data = get_xml_report(self.appt_hist_config_file)
        ns = {'wd': 'urn:com.workday.report/All_Faculty_Appointment_History'}

        data = []
        report_entries = xml_data.xpath('//wd:Report_Entry', namespaces=ns)

        for entry in report_entries:
            name = entry.xpath('./wd:Academic/@wd:Descriptor', namespaces=ns)[0]

            # Handle missing IDs
            person_id_list = entry.xpath(
                './wd:Academic/wd:ID[@wd:type="Employee_ID" or @wd:type="Contingent_Worker_ID" or @wd:type="Academic_Affiliate_ID" or @wd:type="Student_ID"]',
                namespaces=ns)
            person_id = person_id_list[0].text
            id_type = person_id_list[0].get('{urn:com.workday.report/All_Faculty_Appointment_History}type')
            start_date = entry.xpath('./wd:Start_Date', namespaces=ns)[0].text
            data.append([person_id, id_type, name, start_date])

        h = pd.DataFrame(data, columns=['person_id', 'person_id_type', 'name', 'start_date'])
        h['start_date'] = pd.to_datetime(h['start_date'], format='%Y-%m-%d-%H:%M').dt.date

        return h

    def get_academic_appointment_history(self):
        appts = self.get_most_recent_appointments()
        appts['handled'] = 1
        ch = self.get_appointment_change_dates()

        ch = ch.merge(appts[['start_date', 'person_id', 'person_id_type', 'handled']],
                      how='left', on=['start_date', 'person_id', 'person_id_type'])
        ch['handled'] = ch['handled'].fillna(0)
        todo = ch[ch['handled'] == 0]

        appt_rows_to_add = []
        for start_date, people in todo.groupby('start_date'):
            addtl_rows = self.get_appointments_by_date(people['person_id'], people['person_id_type'], start_date)
            appt_rows_to_add.append(addtl_rows)



        new_appts = pd.concat([appts.drop(columns=['handled'])] + appt_rows_to_add).reset_index()
        a = new_appts.sort_values(by=['person_id', 'start_date'])
        a = a.drop(columns=['index']) #.drop_duplicates()
        return a
