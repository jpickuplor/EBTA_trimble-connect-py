'''
File data is added to SQLITE db and models from one project are mapped to another - helpful in migration
'''

from TrimblePy.common.auth import Authentication
from TrimblePy.connect.file_api import TrimbleFileApi
from TrimblePy.connect.model_api import ModelApi
from TrimblePy.connect.model_api import Entity
from TrimblePy.topic.topics_api import TopicApi
import pandas as pd
import sqlite3



def setup_api(auth, project_id=None, region='ap2'):
    try:
        x = auth.ensure_token()
        assert x == True
    except:
        try:
            auth.renew_tokens()
        except:
            auth.get_new_tokens_with_authorization_code()
    access_token, refresh_token = auth.get_token()
    file_api_endpoint = auth.endpoints['tc']  # the base URL for the Trimble Connect API
    # get projects
    if not project_id:
        file_api = TrimbleFileApi(authentication=auth)
        projects = file_api.get_projects()
        project_id = projects[0]['id']
    # setup with project id of interest
    file_api = TrimbleFileApi(authentication=auth,project_id=project_id)
    return file_api, project_id


# -----------------------------------------------------------------
# FILES
# -----------------------------------------------------------------
def create_file_register(auth, project_id=None, region='ap2'):
    file_api = setup_api(auth, project_id=project_id)
    # try to get files
    try:
        files = file_api.get_files()
    except Exception as e:
        print(f"An error occurred: {e}")
    # create df
    files_list_of_dicts = [vars(file_obj) for file_obj in files.values()]
    file_df = pd.DataFrame(files_list_of_dicts)
    # add region data to sql table
    conn = sqlite3.connect('file_data.db')
    table_name = f"{region}_model_data"
    file_df.to_sql(table_name, conn, if_exists='replace', index=False)
    return file_df


ap2_files = create_file_register(project_id='fp2_rG1bRaY',region='ap2')
ap_files = create_file_register(project_id='fp2_rG1bRaY',region='ap')

# -----------------------------------------------------------------
# MAPPING ONE PROJECT TO ANOTHER
# -----------------------------------------------------------------

auth = Authentication(sql_available=True, token_retrieval_method='env')
auth.get_new_tokens_with_authorization_code()
tokens = auth.renew_tokens()
access_token, refresh_token = auth.get_token()
auth.print_expiry_time()


conn = sqlite3.connect('file_data.db')
ap_data = pd.read_sql_query("select * from ap_model_data", conn)
ap2_data = pd.read_sql_query("select * from ap2_model_data", conn)

# ap is the old and ap2 is the new - only looking at files in new, so get list of the filenames in ap2 and get subset of ap
ap2_filenames = ap2_data['name'].tolist()
ap_subset = ap_data[ap_data['name'].isin(ap2_filenames)]

# the fullpath will be the same too
ap_subset = ap_subset[ap_subset['full_path'].isin(ap2_data['full_path'].tolist())]

# hash rows for simplicity
ap_subset['hash'] = ap_subset[['name','fileType','full_path']].apply(lambda x: hash(tuple(x)), axis=1)
ap2_data['hash'] = ap2_data[['name','fileType','full_path']].apply(lambda x: hash(tuple(x)), axis=1)

ap = ap_subset[['hash','id','versionId','name','fileType','full_path']]
ap2 = ap2_data[['hash','id','versionId','name','fileType','full_path']]

# merge on hash and prefix the ap wiht old and ap2 with new
ap_merged = ap.merge(ap2,how='outer',on='hash',suffixes=('_old','_new'))

ap_merged.drop(columns=['hash'],inplace=True)

ap_merged.to_sql('mapped_old_new', conn, if_exists='replace', index=False)

file_api = TrimbleFileApi(authentication=auth,project_id='fp2_rG1bRaY')