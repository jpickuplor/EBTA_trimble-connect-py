import sys

sys.path.append(r"C:\...\trimble-connect-py")

from TrimblePy.common.auth import Authentication
from TrimblePy.connect.file_api import TrimbleFileApi


region = 'ap'

auth = Authentication(sql_available=True, token_retrieval_method='env',region=region) 
access_token, refresh_token = auth.get_token()

print(access_token)

# Get the first project from your list of projects

file_api = TrimbleFileApi(authentication=auth)
projects = file_api.get_projects()
project_id = projects[0]['id']

print(project_id)
