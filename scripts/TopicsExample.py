'''
Remove lines from a viewpoint in a Trimble Connect Topic
'''

from TrimblePy.common.auth import Authentication
from TrimblePy.connect.file_api import TrimbleFileApi
from TrimblePy.connect.model_api import ModelApi
from TrimblePy.connect.model_api import Entity
from TrimblePy.topic.topics_api import TopicApi
import pandas as pd


auth = Authentication(sql_available=True,token_retrieval_method='env',region='ap') # choose sql_available=True if you want to use ms sql server to store and retrieve tokens
# auth.get_new_tokens_with_authorization_code()
# make sure there are tokens available
access_token, refresh_token = auth.get_token()

file_api = TrimbleFileApi(authentication=auth)
projects = file_api.get_projects()
project_id = projects[0]['id']

topics_api = TopicApi(authentication=auth,project_id=project_id)
topics = topics_api.construct_topics()
viewpoints = topics_api.construct_all_viewpoints(topics)

topic = topics[3]
vp_data = topic.viewpoint
# delete the lines
vp_data.lines = []

res = topics_api.update_viewpoint(topic)