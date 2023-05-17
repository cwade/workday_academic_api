## Workday Academic API
This is very rough, minimally documented code using Python and zeep to extract some of the Workday academic data (Workday Student + some of the academic aspects of Workday HR) into pandas dataframes.

```
# Sample usage
from academic_unit import AcademicUnit
from student_course import StudentCourse
from course_section import CourseSection
from datetime import datetime

config_hr = 'config-workday-hr-demo.yml'
config_stu = 'config-workday-stu-demo.yml'

min_effdt = datetime(1900, 1, 1)
max_to_date = datetime(2223, 1, 1)

u = AcademicUnit(config_hr)
units = u.get_all(min_effdt, max_to_date)

c = StudentCourse(config_stu)
courses = c.get_all(min_effdt, max_to_date)

cs = CourseSection(config_stu)
course_sects = cs.get_all()
```

The methods above extract all the academic units, courses, and course sections into pandas dataframes. At the moment, these data frames include many typical columns but also some unparsed JSON.
