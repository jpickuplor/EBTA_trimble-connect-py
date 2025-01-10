# Trimble Connect APIs
## (PLEASE NOTE - THIS REPOSITORY IS NOT FREQUENTLY MAINTAINED AND PURELY SERVES AS A SET OF USEFUL EXAMPLES)

This example demonstrates how to interact with the Trimble Connect Model API using the `TrimblePy` Python package.

# Project Structure

<details>
<summary>Authentication Module ('auth.py')</summary>

### Class: Authentication

- **Methods**
  - set_base_url
  - get_endpoint
  - \_save_token_env_data
  - \_load_token_env_data
  - \_save_token_to_env
  - \_load_token_from_env
  - ensure_token
  - get_token
  - print_expiry_time
  - get_stored_access_token
  - get_stored_refresh_token
  - \_client_credentials_base64
  - \_get_authorization_code
  - renew_tokens
  - get_new_tokens_with_authorization_code
  - get_sql_engine
  - get_sql_tokens
  - tokens_to_sql
  - update_sql_tokens
  - get_sql_table

</details>

<details>
<summary>File API Module ('file_api.py')</summary>

### Class: TrimbleFileApi

- **Methods**
  - get_projects
  - get_file_snapshot
  - build_index
  - get_full_path
  - get_files
  - safe_request
  - get_activities
  - get_project_users
  - add_tag
  - get_tags
  - get_tag
  - get_tagged_objects
  - delete_tags
  - add_objects_to_tag
  - get_clashsets
  - get_clash_details
  - delete_clash
  - post_clashset
  - list_all_clash_items
  - get_todos
  - get_todo_attachments
  - get_2d_view

### Class: TrimbleFile

- **Methods**
  - **repr**
  </details>

<details>
<summary>Model API Module ('model_api.py')</summary>

### Class ModelApi:

- **Methods**
  - get_model_layers
  - get_model_entities
  - get_pset_defs
  - get_model_info
  - build_df_models
  - construct_model
  - \_construct_model_worker
  - construct_models
  - get_entity_data
  - construct_entities
  - entities_object
  - entity_to_df
  - entity_to_df_optimized
  - process_entities_with_multiprocessing

### Class: Entity

- **Methods**
  - **repr**

### Class: Model

- **Methods**
  - add_entity
  - **repr**
  </details>

<details>
<summary>Org API Module ('org_api.py')</summary>

### Class: OrgApi

- **Methods**
  - get_discovery_trees
  - get_discovery_tree
  - get_nodes
  - get_node

</details>

<details>
<summary>Pset API Module ('pset_api.py')</summary>

### Class: PsetApi

- **Methods**
  - get_lib_defs
  - encoder
  - frn_notation
  - get_object_psets
  - update_pset
  - prop_set_table
  - create_library
  - create_pset
  - update_pset_wrapper
  - mp_helper

</details>

<details>
<summary>Topics API Module ('topics_api.py')</summary>

### Class: TopicApi

- **Methods**
  - get_topics
  - construct_topics
  - get_viewpoint
  - construct_viewpoint
  - \_get_viewpoint_helper
  - get_all_viewpoints
  - construct_all_viewpoints
  - construct_viewpoint_data
  - create_new_issue
  - delete_topic
  - update_viewpoint

### Class: Topic

- **Methods**
  - to_dict
  - **repr**

### Class: Viewpoint

- **Methods**
  - to_dict

</details>


## Installation

Clone the repo / Download Repo 

I've found it easiest to append the path to any files you want to run.

```python

import sys

sys.path.append("path/to/trimble-connect-py")

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


```

Before running the code, make sure to install the required dependencies and set up your environment variables in the `.env` file.

```env
SQL_SERVER=...              (optional)
SQL_DATABASE=...            (optional)
SQL_SCHEMA=...              (optional)
TRIMBLE_CLIENT_ID=...       (required)
TRIMBLE_CLIENT_SECRET=...   (required)
TRIMBLE_REDIRECT_URL=...    (required)
TRIMBLE_PROJECT_NAME=...    (required)
TRIMBLE_ACCESS_TOKEN=...    (optional)
TRIMBLE_REFRESH_TOKEN=...   (optional)


```

## Examples - trimble-connect-py/scripts

- **basic.py** - simple example of setting up the authentication and retrieving the first project from your list of projects
- **Example.py** - comprehensive examples relating to the file api, model api, topic api, and pset api. This demonstrates how to create model and entity objects to work with in your code. Creation of a local data twin of the models and entities is also demonstrated. As well as this, the use of the topics api is demonstrated, showing how to combine the file api, model api and topics api to retrieve viewpoints and associated entities.
- **MappingFilesExample.py** - demonstrates a way to retrieve project files records from two separate projects in two separate regions and map the old modelIds to the new modelIds. This is useful if you have a project in, say, the US region and a project in the AP region and you want to map the modelIds from the US project to the AP project.
- **PsetExample.py** - as an example - geometry data is added to a Pset that compares varying geometry from Solibri, Trimble and Revit, and a flag is added to the Pset to indicate if the geometry is the same or not. This is a general geometry validation exercise. 
- **TodoExample.py** - demonstrates how to retrieve todo data from a project and create a Sqlite database of the todo data. 
- **TopicsExample.py** - update the viewpoint of a Topic to remove lines. 
- *Note - there is an example in the scripts/rough_examples folder that looks into writing custom psets with multiprocessing. While it will work, it is not pretty.*


## Authentication

Create an `Authentication` object to handle the token retrieval and storage. You can choose to store tokens in a Microsoft SQL server by setting `sql_available=True`.

#### Environment Variable Auth Method

```python

from TrimblePy.common.auth import Authentication
auth = Authentication(sql_available=False, token_retrieval_method='env', region='ap')
# tokens store in .env file provide more security than storing in a temporary folder
access_token, refresh_token = auth.get_token()
```

#### SQL Auth Method

```python
# Store tokens in secure SQL server
auth = Authentication(sql_available=True, token_retrieval_method='sql', region='us')
access_token, refresh_token = auth.get_token()
```

#### Web Auth Method (no storage)

```python
# Don't store tokens at all and initiate web request then refresh throughout session
auth = Authentication(sql_available=True, token_retrieval_method='web')
access_token, refresh_token = auth.get_token()
# If your redirect_uri is localhost, copy and paste the url into the console to continue
>>> Opening browser for authentication. Please wait for the redirect URL.
>>> Paste the redirect URL here (it will contain the authorization code): <"http://localhost/?code=us_..."> # paste the entire URL here

```

## Working with Files

Retrieve project files using the `TrimbleFileApi` and then filter the files to work with specific IFC files in specified folders.

```python
from TrimblePy.connect.file_api import TrimbleFileApi
import pandas as pd

file_api = TrimbleFileApi(authentication=auth)

projects = file_api.get_projects()
project_id = projects[0]['id'] # select the most recently modified project

# (optionally reset file_api with project): file_api = TrimbleFileApi(authentication=auth, project_id=project_id)

try:
    files = file_api.get_files(project_id)
except Exception as e:
    print(f"An error occurred: {e}")
```

## Working with Activities

You can retrieve a dictionary of the last x pages of project activities using the `TrimbleFileApi`. The activity data can be converted into a table format and flattened to make it easy to visualize and analyze.

```python
from TrimblePy.connect.file_api import TrimbleFileApi
import pandas as pd

file_api = TrimbleFileApi(authentication=auth, project_id=project_id)
activities = file_api.get_activities(max_depth=200)

# Table data and flatten JSON
activities_df = pd.DataFrame(activities)
activities_df[['createdBy_userStatus', 'createdBy_userId', 'createdBy_tiduuid', 'createdBy_email', 'createdBy_firstName', 'createdBy_lastName']] = pd.json_normalize(activities_df['createdBy'])
activities_df['objectId'] = activities_df['details'].apply(lambda x: x['object']['id'])
activities_df['objectDisplayName'] = activities_df['details'].apply(lambda x: x['object']['displayName'] if 'displayName' in x['object'].keys() else None)
activities_df['objectType'] = activities_df['details'].apply(lambda x: x['object']['type'])
activities_df.drop(columns=['createdBy', 'details'], inplace=True)
```

## Working with Users

You can retrieve and organize user data from your project using the TrimbleFileApi. The user data can then be cleaned and formatted for dashboard visualization or further analysis.

```python
from TrimblePy.connect.file_api import TrimbleFileApi
import pandas as pd

file_api = TrimbleFileApi(authentication=auth, project_id=project_id)
users = file_api.get_project_users()

# Clean data for a dashboard:
users_df = pd.DataFrame(users)
users_df = users_df[users_df["status"] == "ACTIVE"]
users_df = users_df[["id", "tiduuid", "email", "firstName", "lastName", "role", "lastAccessed"]]
users_df['name'] = users_df['firstName'] + " " + users_df['lastName']
users_df.drop(columns=["firstName", "lastName"], inplace=True)
users_df.columns = ['user_id', 'tiduuid', 'user_email', 'user_role', 'last_accessed', 'user_name']
```

## Working with Tags

The following options are available with tags:

- add_tag
- get_tags - all tags
- get_tag - specific tag
- get_tagged_objects - objects tagged with a specific tag
- delete - delete a tag
- add_objects_to_tag - tag objects

```python
file_api = TrimbleFileApi(authentication=auth,project_id=project_id)
tags = file_api.get_tags()
tag_df = pd.DataFrame(tags)

# create a new tag
tag = file_api.add_tag('Precast Concrete')

# add objects to a tag (or, add tags to an object...)
object_list = [{"id":"ObjectID","objectType":"FILE"}]
add_to_tag = file_api.add_objects_to_tag('TagId',object_list)

# delete tag
tag = "TagId"
delete_tag = file_api.delete_tags(tag)
```

## Working with Models

Use the `ModelApi` to retrieve model information and create a DataFrame of model data. Construct models to access their entities.

```python
from TrimblePy.connect.model_api import ModelApi

model_api = ModelApi(authentication=auth)
version_ids = ['EXAMPLE_VERSION_ID_1', 'EXAMPLE_VERSION_ID_2']

try:
    df_models = model_api.build_df_models(version_ids)
except Exception as e:
    print(f"An error occurred: {e}")
```

You can easily fetch all the data for the project.

```python
from TrimblePy.connect.file_api import TrimbleFileApi
from TrimblePy.connect.model_api import ModelApi
import pandas as pd

file_api = TrimbleFileApi(authentication=auth)
project_id = "YOUR_PROJECT_ID"
try:
    files = file_api.get_files(project_id)
except Exception as e:
    print(f"An error occurred: {e}")

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
all_entities_df = model_api.process_entities_with_multiprocessing(all_entities,n_workers=10) # handle however many threads you want to use ... API works ok with 6-10

```

## Retrieving Entity Data

Retrieve entity data for a specific model version ID and construct entities to create a DataFrame of entity properties and data.

```python
versionId = 'EXAMPLE_VERSION_ID'
entityCount = 1283 # Replace with actual entity count
entityData, psetData, layerData = model_api.get_entity_data(versionId, entityCount)
entities = model_api.construct_entities(entityData, psetData, layerData)
```

## Working with Topics

Utilize the `TopicApi` to retrieve and construct topic objects with viewpoint data.

```python
from TrimblePy.topic.topics_api import TopicApi

project_id = "YOUR_PROJECT_ID"
topic_api = TopicApi(authentication=auth, project_id=project_id)

topics = topic_api.construct_topics()
topic_api.construct_all_viewpoints(topics, pool_size=6)
```

## Retrieving Associated Entities for Topics

Fetch models associated with topics and search for entities in the model entities.

```python
tc_files = topics[0].tc_files
model_ids = [x.replace('tc:', '') for x in tc_files]
df_models = model_api.build_df_models(model_ids)

models = [model_api.construct_model(df_models.loc[i]) for i in range(len(df_models))]
ents = [model.entities for model in models]
ents = [item for sublist in ents for item in sublist] # Flatten list
```

## Working With Clashes

Retrieve clash data for a specific clash check and construct clash objects.

```python
from TrimblePy.connect.file_api import TrimbleFileApi

file_api = TrimbleFileApi(authentication=auth)

# Get clashsets
clashes = file_api.get_clashsets()

# Get clash details for first clashset
clash_details = file_api.get_clash_details(clashes[0]['id'])

# List all the clash items for the first clashset
list_all_clash_items = file_api.list_all_clash_items(clashes[0]['id'])

# Create new clashset
new_clash = file_api.post_clashset(name="Test Clashset", tolerance=10, models=['ModelId-0', 'ModelId-1'], ignoreSameDiscipline=False, type_of_clash="CLEARANCE")

```

## Working with ToDos

See scripts/TodoExample.py

## Working with Psets

See scripts/PsetExample.py

## Notes

- Replace placeholder values (such as `'EXAMPLE_VERSION_ID'`, `'YOUR_PROJECT_ID'`) with actual data from your environment.
- Handle exceptions appropriately and add error handling logic as deemed necessary.
