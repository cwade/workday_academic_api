## Workday Academic API
This is very rough, minimally documented code using Python and zeep to extract some of the Workday academic data (Workday Student + some of the academic aspects of Workday HR) into pandas dataframes.

From a terminal
```
python -m venv wkdy_api
source wkdy_api/bin/activate
git clone git@github.com:cwade/workday_academic_api.git
cd workday_academic_api
pip install -r requirements.txt
```

Edit the files config-workday-hr-demo.yml, config-workday-stu-demo.yml, and config-workday-job-history-demo.yml with the correct server_name, tenant_name, and user credentials. The users you choose need to have permissions to make API calls.

Once that's done, from the terminal (with the venv activated) you can run

```
python example.py
```

This should output a csv file for every type of data the script is pulling. Most of these are still very rough. The job_history.csv file is the most developed but I still need to test more to see if what's coming out is correct. 