'''
A range of exmaples looking into retriving all model data and identifying those elements that are involved in Topics
'''

from TrimblePy.common.auth import Authentication
from TrimblePy.connect.file_api import TrimbleFileApi
from TrimblePy.connect.model_api import ModelApi
from TrimblePy.connect.model_api import Entity
from TrimblePy.topic.topics_api import TopicApi
import pandas as pd

# VARIOUS USAGE EXAMPLES. 

# SETUP ENVIRONMENT VARIABLES in .env file
# SQL_SERVER="..."
# SQL_DATABASE="..."
# SQL_SCHEMA="..."
# TRIMBLE_CLIENT_ID="..."
# TRIMBLE_CLIENT_SECRET="..."
# TRIMBLE_REDIRECT_URL="..."
# TRIMBLE_PROJECT_NAME="..."

region = 'US'

auth = Authentication(sql_available=True, token_retrieval_method='env',region=region) 
access_token, refresh_token = auth.get_token()
# auth.get_new_tokens_with_authorization_code()

# Fetch and set the region-specific base URL

file_api_endpoint = auth.endpoints['tc']  # the base URL for the Trimble Connect API
model_api_endpoint = auth.endpoints['model']  # the base URL for the Trimble Connect Model API
topic_api_endpoint = auth.endpoints['topic']  # the base URL for the Trimble Connect Topic API


file_api = TrimbleFileApi(authentication=auth)
projects = file_api.get_projects()
project_id = projects[0]['id']

# -----------------------------------------------------------------
# PROJECTS
# -----------------------------------------------------------------


file_api = TrimbleFileApi(authentication=auth)
projects = file_api.get_projects()
project_id = projects[0]['id']


# -----------------------------------------------------------------
# FILES
# -----------------------------------------------------------------

# re-initialize with the chosen project_id
file_api = TrimbleFileApi(authentication=auth,project_id=project_id)

project_file_prefix = "PROJ-REG-ZONE"

try:
    files = file_api.get_files()
except Exception as e:
    print(f"An error occurred: {e}")


# see paths forward through python object with: files[0].__dir__()

# otherwise create a dict / dataframe
files_list_of_dicts = [vars(file_obj) for file_obj in files.values()]
file_df = pd.DataFrame(files_list_of_dicts)

# We will just work with the project ifcs with project_file_prefix in specified folders
ifc = file_df[
    (file_df['name'].str.endswith('ifc')) & 
    (file_df['name'].str.startswith(project_file_prefix)) &
    (file_df['parent_folder'] == '01-Models')
]
ifc.reset_index(drop=True,inplace=True)


# -----------------------------------------------------------------
# ACTIVITIES
# -----------------------------------------------------------------

# Get dict of the last 200 pages of activities
file_api = TrimbleFileApi(authentication=auth,project_id=project_id)
activities = file_api.get_activities(max_depth=200)

# table data and flatten json
activities = pd.DataFrame(activities)
activities[['createdBy_userStatus','createdBy_userId','createdBy_tiduuid','createdBy_email','createdBy_firstName','createdBy_lastName']] = pd.json_normalize(activities['createdBy'])
activities['objectId'] = activities['details'].apply(lambda x: x['object']['id'])
activities['objectDisplayName'] = activities['details'].apply(lambda x: x['object']['displayName'] if 'displayName' in x['object'].keys() else None)
activities['objectType'] = activities['details'].apply(lambda x: x['object']['type'])
activities.drop(columns=['createdBy','details'],inplace=True)


# -----------------------------------------------------------------
# USERS
# -----------------------------------------------------------------

file_api = TrimbleFileApi(authentication=auth,project_id=project_id)
users = file_api.get_project_users()

# clean data for a dashboard:
users_df = pd.DataFrame(users)
users_df = users_df[users_df["status"] == "ACTIVE"]
users_df = users_df[["id", "tiduuid","email","firstName","lastName","role","lastAccessed"]]
users_df['name'] = users_df['firstName'] + " " + users_df['lastName']
users_df.drop(columns=["firstName","lastName"], inplace=True)
users_df.columns = ['user_id','tiduuid','user_email','user_role','lastAccessed','user_name']

# -----------------------------------------------------------------
# TAGS
# -----------------------------------------------------------------
'''
add_tag(tag)
get_tags()
get_tag(tag_id)
get_tagged_objects(tag_id)
delete_tags(tag_id)
add_objects_to_tag(tag_id, object_list)

'''
file_api = TrimbleFileApi(authentication=auth,project_id=project_id)
tags = file_api.get_tags()
tag_df = pd.DataFrame(tags)

# create a new tag
tag = file_api.add_tag('Test_To_Delete')

# add objects to a tag (or, add tags to an object...)
object_list = [{"id":"File1.id","objectType":"FILE"}]
add_to_tag = file_api.add_objects_to_tag(tag['id'],object_list)

# delete tag
tag = tag['id']
delete_tag = file_api.delete_tags(tag)


# -----------------------------------------------------------------
# MODELS
# -----------------------------------------------------------------

model_api = ModelApi(authentication=auth)
version_ids = ifc.versionId.tolist()[0:2]
version_ids = ['123ABC09', 'XX00SFBA']  # Example Version IDs

try:
    df_models = model_api.build_df_models(version_ids)
except Exception as e:
    print(f"An error occurred: {e}")

# merge with ifc rows
model_data = pd.merge(ifc[ifc.versionId.isin(version_ids)],df_models, on=['id','versionId'])

model = model_api.construct_model(model_data.loc[0])
entities = model.entities
entity = entities[0]

# see the model of an entity!
entity.model
entity.model.versionId

# search for IFCPROJECT
ifc_project_entities = [entity for entity in entities if entity.ifc_type == 'IFCPROJECT'][0]


# get json obj of entity: 
# entity = model_api.entities_object(ifc_project_entities)

# Usage example for a single Entity object
entity_df = model_api.entity_to_df(entities[0], include_product=True)

# -----------------------------------------------------------------
# GET ALL MODEL & ENTITY DATA
# -----------------------------------------------------------------

model_api = ModelApi(authentication=auth)

try:
    df_models = model_api.build_df_models(ifc.versionId.tolist()) # takes a while to run
except Exception as e:
    print(f"An error occurred: {e}")

# merge with ifc rows
model_data = pd.merge(ifc,df_models, on=['versionId']) # sometimes the model api returns an incorrect id for model (returning versionId instead)
model_data.drop(columns=['id_y'],inplace=True)
model_data.rename(columns={'id_x':'id'},inplace=True)


# get all model entities by constructing all models
models = model_api.construct_models(model_data)
all_entities = [model.entities for model in models] # list of lists
all_entities = [item for sublist in all_entities for item in sublist] # flatten list
# create df for entire model
all_entities_df = model_api.process_entities_with_multiprocessing(all_entities,n_workers=10) # handle however many threads you want to use ... API works ok with 6-10

# -----------------------------------------------------------------
# ENTITIES
# -----------------------------------------------------------------

versionId = model_data.loc[0].versionId # 'XX00SFBA'
entityCount = model_data.loc[0].entityCount # 1283

versionId =  'XX00SFBA'
entityCount = 1283

try:
    df_entities = model_api.get_entity_data(versionId, entityCount)
    # Now entity_objects is a list of Entity instances
except Exception as e:
    print(f"An error occurred: {e}")

entityData, psetData, layerData = model_api.get_entity_data(versionId, entityCount)
entities = model_api.construct_entities(entityData, psetData, layerData)
entity = model_api.entities_object(entities[0])

# -----------------------------------------------------------------
# TOPICS
# -----------------------------------------------------------------
project_id = "project_id"

topic_api = TopicApi(authentication=auth,project_id=project_id)


# Get Raw Topic Data in Dict Format
topics = topic_api.get_topics()
topic = topics[0]
viewpoint = topic_api.get_viewpoint(topic.guid, topic.viewpoint_guid)

# create a list of topic objects with viewpoint data
topics = topic_api.construct_topics()
topic_api.construct_all_viewpoints(topics,pool_size=10) # handle however many threads you want to use ... API works ok with 6-10

# Or construct a single viewpoint to add viewpoint data to the topic object
viewpoint = topic_api.construct_viewpoint(topics[0])

# See the ifc-guids association with the topic after constructing the viewpoint
topic = topics[0]
tc_files = topic.tc_files # ['tc:123ABCA-0', 'tc:123O_mno'] - they are returned with frn notation, and are model_id, not version_id
components_involved = topic.ifc_guids

# You can fetch these models individually 
model_ids = [x.replace('tc:','') for x in tc_files]
df_models = model_api.build_df_models(model_ids)
model_data = pd.merge(ifc[ifc['id'].isin(model_ids)],df_models, on=['id','versionId'])

models = [model_api.construct_model(model_data.loc[i]) for i in range(len(model_data))]

ents = [model.entities for model in models]
ents = [item for sublist in ents for item in sublist] # flatten list

# topic components are stored without model name... search for them in the model entities
inv_ents = [x for x in ents if x.entity_id in components_involved and x.model.name in [m.name for m in models]]


# -----------------------------------------------------------------
# CLASHES
# -----------------------------------------------------------------

file_api = TrimbleFileApi(authentication=auth,project_id=project_id)
clashes = file_api.get_clashsets()
clash_details = file_api.get_clash_details(clashes[0]['id'])
list_all_clash_items = file_api.list_all_clash_items(clashes[0]['id'])
new_clash = file_api.post_clashset(name="Test Clashset", tolerance=10, models=['ModelId-0', 'ModelId-1'], ignoreSameDiscipline=False, type_of_clash="CLEARANCE")
