'''
Index the data relating to ToDos in a SQLITE db
'''

from TrimblePy.common.auth import Authentication
from TrimblePy.connect.file_api import TrimbleFileApi
from TrimblePy.connect.model_api import ModelApi
from TrimblePy.connect.model_api import Entity
from TrimblePy.topic.topics_api import TopicApi
import pandas as pd
import sqlite3

# CREATE A SQLITE DATABASE OF TODO DATA


auth = Authentication(sql_available=True, token_retrieval_method='env')  # Set up the Authentication instance
access_token, refresh_token = auth.get_token()

auth.get_new_tokens_with_authorization_code()

file_api_endpoint = auth.endpoints['tc']  # the base URL for the Trimble Connect API
model_api_endpoint = auth.endpoints['model']  # the base URL for the Trimble Connect Model API
topic_api_endpoint = auth.endpoints['topic']  # the base URL for the Trimble Connect Topic API


file_api = TrimbleFileApi(authentication=auth)
projects = file_api.get_projects()
project_id = projects[0]['id']


# -----------------------------------------------------------------
# FILES
# -----------------------------------------------------------------

def keys_to_columns(keys, df, prefix, dict_column):
    for key in keys:
        df[f'{prefix}.{key}'] = df[dict_column].apply(lambda x: x.get(key))
    df = df.drop(columns=dict_column)
    return df

# re-initialize with the chosen project_id
file_api = TrimbleFileApi(authentication=auth,project_id=project_id)

todos = file_api.get_todos()

df = pd.DataFrame(todos)

# tables for assignee, createdBy, modifiedBy
assignees = df[['id','assignees']]
assignees = assignees.dropna(subset='assignees')
assignees = assignees.explode(column='assignees')
assignees['assignee.id'] = assignees['assignees'].apply(lambda x: x.get('id'))
assignee_keys = ['id', 'type', 'email', 'name', 'firstName', 'lastName', 'tiduuid']

for key in assignee_keys:
    assignees[f'assignee.{key}'] = assignees['assignees'].apply(lambda x: x.get(key))

assignees.drop(columns=['assignees'],inplace=True)


createdBy = df[['id','createdBy']]
user_keys = ['status', 'id', 'tiduuid', 'email', 'firstName', 'lastName']
createdBy = keys_to_columns(user_keys,createdBy,'createdBy','createdBy')

modifiedBy = df[['id','modifiedBy']]
modifiedBy = keys_to_columns(user_keys,modifiedBy,'modifiedBy','modifiedBy')

tags = df[['id','tags']]
tags = tags.explode(column='tags')
tags.dropna(subset='tags',inplace=True)
tag_keys = ['id', 'projectId', 'label', 'description']
tags = keys_to_columns(tag_keys,tags,'tag','tags')


df.drop(columns=['assignees','modifiedBy','createdBy','tags'],inplace=True)

tables = [df,createdBy,modifiedBy,assignees,tags]

atts = []
for id in df['id'].tolist():
    attachments = file_api.get_todo_attachments(id)
    for a in attachments:
        a['todoId'] = id
        atts.append(a)

att = pd.DataFrame(atts)
att.rename(columns={'id':'attachmentId','todoId':'id'},inplace=True)

att_modifiedBy = att[['attachmentId','modifiedBy']]
att_modifiedBy = keys_to_columns(user_keys,att_modifiedBy,'modifiedBy','modifiedBy')

att.drop(columns=['modifiedBy'],inplace=True)

views = att[att['type']=='VIEW2D']

v_data = []

for viewId in views.attachmentId.tolist():
    view_details = file_api.get_2d_view(viewId)
    v_data.append(view_details)

v_ = pd.DataFrame(v_data)

v_modifiedBy = v_[['id','modifiedBy']]
v_modifiedBy = keys_to_columns(user_keys,v_modifiedBy,'modifiedBy','modifiedBy')

v_createdBy = v_[['id','createdBy']]
v_createdBy = keys_to_columns(user_keys,v_createdBy,'createdBy','createdBy')

v_markup = v_[['id','markup']]
v_markup['markup.xfdfData'] = v_markup['markup'].apply(lambda x: x.get('xfdfData'))
v_markup.drop(columns=['markup'],inplace=True)

v_.drop(columns = ['modifiedBy','createdBy','assignees','markup'],inplace=True)


tables = {
    'todos':df,
    'todo_createdBy':createdBy,
    'todo_modifiedBy':modifiedBy,
    'todo_assignees':assignees,
    'todo_tags':tags,
    'todo_attachments':att,
    'attachment_modifiedBy':att_modifiedBy,
    'todo_2d_views':v_,
    '2d_view_createdBy':v_createdBy,
    '2d_view_modifiedBy':v_modifiedBy,
    '2d_view_markup':v_markup
}

conn = sqlite3.connect('todo.db')

for k, v in tables.items():
    print(v)
    v.to_sql(k,conn,if_exists='replace',index=False)

conn.close()
