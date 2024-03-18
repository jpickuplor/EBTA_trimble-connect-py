# Go through each of the views and determine whether the all the models are relevant. 
from TrimblePy.common.auth import Authentication
from TrimblePy.connect.file_api import TrimbleFileApi
import pandas as pd


region = 'AP'

auth = Authentication(sql_available=True, token_retrieval_method='env',region=region) 
access_token, refresh_token = auth.get_token()

file_api = TrimbleFileApi(authentication=auth)
projects = file_api.get_projects()
project_id = projects[0]['id']

# -----------------------------------------------------------------
# PROJECTS
# -----------------------------------------------------------------


file_api = TrimbleFileApi(authentication=auth,project_id=project_id)

views = file_api.get_views()
view_df = pd.DataFrame(views)


try:
    files = file_api.get_files()
except Exception as e:
    print(f"An error occurred: {e}")

# /files/fs/{fileId}/downloadurl
files_list_of_dicts = [vars(file_obj) for file_obj in files.values()]
file_df = pd.DataFrame(files_list_of_dicts)

def check_model_states(row, file_df):
    m = row.models
    fs = file_df[file_df.versionId.isin(m)]
    ss = fs[fs['parent_folder'] == 'SS']
    if len(ss) > 0:
        return ss.versionId.values
    else:
        return None

states = view_df.apply(lambda row: check_model_states(row, file_df), axis=1)
view_df['states'] = states

flag = view_df[view_df.states.notna()]