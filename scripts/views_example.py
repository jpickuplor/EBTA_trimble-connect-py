from TrimblePy.common.auth import Authentication
from TrimblePy.connect.file_api import TrimbleFileApi
from TrimblePy.connect.model_api import ModelApi
from TrimblePy.connect.model_api import Entity
from TrimblePy.topic.topics_api import TopicApi
from TrimblePy.common.helper import return_column_schema, keys_to_columns, columns_to_keys
import pandas as pd
import sqlite3
import re
import json

def to_sql():
    region = 'ap'
    project_id='fp2_rG1bRaY'
    auth = Authentication(sql_available=True, token_retrieval_method='env',region=region)  # Set up the Authentication instance
    auth.renew_tokens()
    file_api = TrimbleFileApi(authentication=auth,project_id=project_id)
    views = file_api.get_views()
    vdf = pd.DataFrame(views)
    # columns -> ['id', 'name', 'description', 'projectId', 'thumbnail', 'createdOn', 'modifiedOn', 'createdBy', 'modifiedBy', 'assignees', 'models', 'files']
    dicts = ['createdBy','modifiedBy']
    coldict = return_column_schema(vdf)
    # 'id': <class 'str'>
    # 'name': <class 'str'>
    # 'description': <class 'str'>
    # 'projectId': <class 'str'>
    # 'thumbnail': <class 'str'>
    # 'createdOn': <class 'str'>
    # 'modifiedOn': <class 'str'>
    # 'createdBy': <class 'dict'> *
    # 'modifiedBy': <class 'dict'> *
    # 'assignees': <class list> | <class dict> *
    # 'models': <class list> | <class str> *
    # 'files': <class list> | <class str> *
    # -------------------------------------
    # vdf.id becomes the common key
    # tables for  <class 'dict'> ,<class list> | <class dict>, <class list> | <class str>
    # _____________________________________
    # CREATED BY <dict>
    createdBy = vdf[['id','createdBy']]
    user_keys = ['status', 'id', 'tiduuid', 'email', 'firstName', 'lastName']
    createdBy = keys_to_columns(user_keys,createdBy,'createdBy','createdBy')
    # MODIFIED BY <dict>
    modifiedBy = vdf[['id','modifiedBy']]
    modifiedBy = keys_to_columns(user_keys,modifiedBy,'modifiedBy','modifiedBy')
    # ASSIGNEES <list> | <class dict>
    assignees = vdf[['id','assignees']]
    assignees = assignees.explode(column='assignees')
    assignees = assignees.dropna(subset='assignees')
    assignees['assignee.id'] = assignees['assignees'].apply(lambda x: x.get('id'))
    assignee_keys = ['id', 'type', 'email', 'name', 'firstName', 'lastName', 'tiduuid']
    # create columns for each key
    for key in assignee_keys:
        assignees[f'assignee.{key}'] = assignees['assignees'].apply(lambda x: x.get(key))
    # drop the original column
    assignees.drop(columns=['assignees'],inplace=True)
    # FILES <list> | <class str>
    files = vdf[['id','files']]
    file_table = files.explode(column='files')
    file_table.reset_index(inplace=True,drop=True)
    # MODELS <list> | <class str>
    models = vdf[['id','models']]
    model_table = models.explode(column='models')
    model_table.reset_index(inplace=True,drop=True)
    # ORGANISE
    vdf.drop(columns=['assignees','modifiedBy','createdBy','files','models'],inplace=True)
    tables = {
        'views': vdf,
        'createdBy': createdBy,
        'modifiedBy': modifiedBy,
        'assignees': assignees,
        'files': file_table,
        'models': model_table
    }
    conn = sqlite3.connect('ap.db')
    for k, v in tables.items():
        print(v)
        v.to_sql(k,conn,if_exists='replace',index=False)

    conn.close()
    return tables

def from_sql(tables):
    conn = sqlite3.connect('ap.db')
    vdf = pd.read_sql("SELECT * FROM views", conn)
    createdBy = pd.read_sql("SELECT * FROM createdBy", conn)
    modifiedBy = pd.read_sql("SELECT * FROM modifiedBy", conn)
    assignees = pd.read_sql("SELECT * FROM assignees", conn)
    file_table = pd.read_sql("SELECT * FROM files", conn)
    model_table = pd.read_sql("SELECT * FROM models", conn)
    # for vdf, we want to recreate the views object
    # we need to recreate the assignees, createdBy, modifiedBy, files and models
    vdf['models'] = vdf['id'].apply(lambda x: model_table[model_table['id']==x]['models'].tolist())
    vdf['files'] = vdf['id'].apply(lambda x: file_table[file_table['id']==x]['files'].tolist())
    # assignees was list of dicts, so we need to recreate that
    assignee_keys = ['id', 'type', 'email', 'name', 'firstName', 'lastName', 'tiduuid']
    assignee_columns = [f'assignee.{key}' for key in assignee_keys]
    assignees['assignees'] = assignees[assignee_columns].apply(lambda x: dict(zip(assignee_keys,x.values)),axis=1)
    assignees = assignees[['id','assignees']]
    vdf['assignees'] = vdf['id'].apply(lambda x: assignees[assignees['id']==x]['assignees'].tolist())
    # createdBy and modifiedBy were dicts, so we need to recreate that
    createdBy = columns_to_keys(createdBy,'createdBy')
    modifiedBy = columns_to_keys(modifiedBy,'modifiedBy')
    vdf['createdBy'] = vdf['id'].map(createdBy.set_index('id')['createdBy'])
    vdf['modifiedBy'] = vdf['id'].map(modifiedBy.set_index('id')['modifiedBy'])
    return vdf


tables = to_sql()
vdf = from_sql(tables)