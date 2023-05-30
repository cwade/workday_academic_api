import yaml
from lxml import etree
from requests.auth import HTTPBasicAuth
import requests
import getpass


def get_xml_report(config_file):
    with open(config_file, 'r') as ymlfile:
        config = yaml.load(ymlfile, yaml.FullLoader)

    username = config['username']
    report_url = config['report_url']

    if 'password' in config:
        password = config['password']
    else:
        password = getpass.getpass('Enter password for {}: '.format(username))

    res = requests.get(report_url, auth=HTTPBasicAuth(username, password))
    xml_data = etree.fromstring(res.content)
    return xml_data
