from academic_appointee import AcademicAppointee
from json_utils import get_key_value_for_types
import pandas as pd

class AcademicAppointment:

    def __init__(self, hr_api_config_file):
        self.hr_api_config_file = hr_api_config_file

    def get_all(self):
        apte = AcademicAppointee(self.hr_api_config_file)
        appointees = apte.get_all()
        return self.get_appointments_from_appointees(appointees)

    @staticmethod
    def get_appointments_from_appointees(appointees):
        appt_list = []
        for i, row in appointees.iterrows():
            for j, appt in enumerate(row['appointment_data']):
                appt['person_id'] = row['id']
                appt['person_id_type'] = row['id_type']
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

        vals_to_extract = [
            ['track_type', 'Track_Type_Reference.ID', ['Academic_Track_Type_ID']],
            ['appointment_identifier_id', 'Appointment_Identifier_Reference.ID', ['Academic_Appointment_Identifier_ID']],
            ['position_id', 'Position_Reference.ID', ['Position_ID']],
            ['academic_unit_id', 'Academic_Unit_Reference.ID', ['Academic_Unit_ID']],
            ['academic_rank', 'Rank_Reference.ID', ['Academic_Rank_ID']],
            ['tenure_home', 'Tenure_Home_Reference.ID', ['Academic_Unit_ID']],
            ['tenure_status', 'Tenure_Status_Reference.ID', ['Academic_Tenure_Status_ID']],
            ['named_prof_id', 'Named_Professorship_Reference.ID', ['Named_Professorship_ID']]]

        for new_var, old_var, elems in vals_to_extract:
            appts[new_var] = appts[old_var].apply(lambda x: get_key_value_for_types(x, elems))

        final_cols = ['start_date', 'end_date', 'track_id', 'track_is_inactive', 'person_id',
                      'person_id_type', 'appt_seq', 'title', 'position_id', 'academic_unit_id',
                      'academic_rank',  'roster_percent', 'track_type', 'tenure_status',
                      'tenure_award_date', 'tenure_home', 'appointment_identifier_id', 'named_prof_id']
        return appts[final_cols]
