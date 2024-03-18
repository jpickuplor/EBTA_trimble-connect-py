from TrimblePy.common.auth import Authentication
from TrimblePy.connect.file_api import TrimbleFileApi
from TrimblePy.connect.model_api import ModelApi
from TrimblePy.connect.model_api import Entity
from TrimblePy.topic.topics_api import TopicApi
import pandas as pd
import sqlite3
import re
import json


# -----------------------------------------------------------------
# FILES
# -----------------------------------------------------------------
def create_file_register(file_api):
    try:
        files = file_api.get_files()
    except Exception as e:
        print(f"An error occurred: {e}")
    # create df
    print("Normalising file data")
    files_list_of_dicts = [vars(file_obj) for file_obj in files.values()]
    file_df = pd.DataFrame(files_list_of_dicts)
    # add region data to sql table
    conn = sqlite3.connect('file_data.db')
    table_name = f"{region}_model_data"
    file_df.to_sql(table_name, conn, if_exists='replace', index=False)
    return file_df




# -----------------------------------------------------------------
# MAPPING ONE PROJECT TO ANOTHER
# -----------------------------------------------------------------
region = 'ap'
project_id='fp2_rG1bRaY'
auth = Authentication(sql_available=True, token_retrieval_method='env',region=region)  # Set up the Authentication instance
auth.renew_tokens()  # Renew the tokens
file_api = TrimbleFileApi(authentication=auth,project_id=project_id)
file_df = create_file_register(file_api=file_api)
# get projects
regex = r"\.[a-zA-Z0-9]{1,4}$"
file_df['file_extension'] = file_df['name'].apply(lambda x: re.findall(regex,x)[0] if len(re.findall(regex,x))>0 else None)

ext_dir = "C:\Laing ORourke\Laing ORourke\OneDrive - Laing ORourke\Documents\emb\data_architecture\ext_types.json"
exts = json.load(open(ext_dir, 'r'))
file_df['file_description'] = file_df['file_extension'].apply(lambda x: exts[str(x).upper()]['description'] if str(x).upper() in exts.keys() else None)
