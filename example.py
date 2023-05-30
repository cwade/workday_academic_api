from academic_unit import AcademicUnit
from academic_appointment import AcademicAppointment
from academic_appointee import AcademicAppointee
from student_course import StudentCourse
from course_section import CourseSection
from worker import Worker
from worker_event_history import WorkerEventHistory
from job import Job
from datetime import date

config_hr = 'config-workday-hr-demo.yml'
config_stu = 'config-workday-stu-demo.yml'
config_job_hist = 'config-workday-job-history-demo.yml'
config_acad_appointment_hist = '/Users/cwade/configs/config-workday-appt-history.yml'

min_effdt = date(1900, 1, 1)
max_to_date = date(2099, 12, 30)

# Gets a dataframe including an individual worker's event history using the
# Worker_Event_History API call
print('Getting worker history for worker 00219')
weh = WorkerEventHistory(config_hr)
history = weh.get_worker_history('00219', False)
history.to_csv('worker_event_history_00219.csv', index=False)

# Gets a dataframe of academic units including changes over time. Still needs a bunch of
# cleanup to make the output usable.
print('Getting academic units')
u = AcademicUnit(config_hr)
units = u.get_all(min_effdt, max_to_date)
units.to_csv('units.csv', index=False)

# Gets a dataframe of courses including changes over time. Still needs a bunch of
# cleanup to make the output usable.
print('Getting courses')
c = StudentCourse(config_stu)
courses = c.get_all(min_effdt, max_to_date)
courses.to_csv('courses.csv', index=False)

# Gets a dataframe of all course sections. Still needs a bunch of
# cleanup to make the output usable.
print('Getting course sections')
cs = CourseSection(config_stu)
sections = cs.get_all()
sections.to_csv('course_sections.csv', index=False)

# Gets a dataframe of all academic appointees. Not all that informative on its own
# but used for getting academic appointments.
print('Getting academic appointees')
a = AcademicAppointee(config_hr)
appointees = a.get_all()
appointees.to_csv('academic_appointees.csv', index=False)

# Gets a dataframe of all academic appointments. This returns all appointment history
# for active and inactive workers. Some of it is a little misleading because fields like
# race and gender aren't effective dated
print('Getting academic appointments')
aa = AcademicAppointment(config_hr, config_acad_appointment_hist)
aahist = aa.get_academic_appointment_history()
aahist.to_csv('academic_appointment_history.csv', index=False)

# Gets a dataframe of all job data, including changes over time. The output
# is pretty clean now, but it needs more testing to verify the data is correct.
print('Getting job history')
j = Job(config_hr, config_job_hist)
jh = j.get_job_history()
jh.to_csv('job_history.csv', index=False)