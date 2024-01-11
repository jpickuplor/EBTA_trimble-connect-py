import requests
import pandas as pd
from tqdm import tqdm
import time
from multiprocessing import Pool

class TopicApi:
    
    def __init__(self, authentication,project_id):
        self.authentication = authentication
        self.headers = {
            "Authorization": f"Bearer {self.authentication.access_token}"
        }
        self.BASE_URL = self.authentication.endpoints['topic'] + "bcf/2.1/"
        self.project_id = project_id

    def get_topics(self):
        response = requests.get(
            f"{self.BASE_URL}projects/{self.project_id}/topics?skiptoken",
            headers=self.headers,
        )
        data = response.json()
        while response.headers.get("next"):
            response = requests.get(
                response.headers.get("next"),
                headers=self.headers,
            )
            data = data + response.json()
            print(len(data))
        return data
    
    def construct_topics(self):
        raw_topics = self.get_topics()
        topic_objects = []
        for raw_topic in raw_topics:
            topic_obj = Topic(
                version=raw_topic.get('version'),
                guid=raw_topic.get('guid'),
                topic_type=raw_topic.get('topic_type'),
                topic_status=raw_topic.get('topic_status'),
                title=raw_topic.get('title'),
                labels=raw_topic.get('labels'),
                creation_date=raw_topic.get('creation_date'),
                creation_author=raw_topic.get('creation_author'),
                creation_author_uuid=raw_topic.get('creation_author_uuid'),
                created_by_uuid=raw_topic.get('created_by_uuid'),
                modified_date=raw_topic.get('modified_date'),
                modified_author=raw_topic.get('modified_author'),
                modified_author_uuid=raw_topic.get('modified_author_uuid'),
                assigned_to=raw_topic.get('assigned_to'),
                assigned_to_uuid=raw_topic.get('assigned_to_uuid'),
                assignees=raw_topic.get('assignees'),
                description=raw_topic.get('description'),
                viewpoint=raw_topic.get('viewpoint'),
                files=raw_topic.get('files'),
            )
            topic_objects.append(topic_obj)
        return topic_objects
    
    def get_viewpoint(self, topic_id, viewpoint_guid):
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                response = requests.get(
                    f"{self.BASE_URL}projects/{self.project_id}/topics/{topic_id}/viewpoints/{viewpoint_guid}",
                    headers=self.headers,
                )
                try:
                    view = response.json()
                    return view
                except:
                    time.sleep(1)
            except:
                if attempt == max_attempts - 1:  # On the last attempt, return an error message
                    return 'error'
    
    def construct_viewpoint(self, topic):
        if topic.viewpoint is None or 'guid' not in topic.viewpoint:
            return None

        topic_id = topic.guid
        viewpoint_guid = topic.viewpoint['guid']

        viewpoint_data = self.get_viewpoint(topic_id, viewpoint_guid)

        if viewpoint_data == 'error':
            return None  # Handle the error accordingly

        perspective_camera = viewpoint_data.get('perspective_camera', {})
        snapshot = viewpoint_data.get('snapshot', {})
        components = viewpoint_data.get('components', {})

        # Flatten the perspective_camera dictionary and add values to the topic
        topic.field_of_view = perspective_camera.get('field_of_view')
        camera_viewpoint = perspective_camera.get('camera_view_point', {})
        topic.camera_viewpoint_x = camera_viewpoint.get('x')
        topic.camera_viewpoint_y = camera_viewpoint.get('y')
        topic.camera_viewpoint_z = camera_viewpoint.get('z')
        camera_direction = perspective_camera.get('camera_direction', {})
        topic.camera_direction_x = camera_direction.get('x')
        topic.camera_direction_y = camera_direction.get('y')
        topic.camera_direction_z = camera_direction.get('z')
        camera_up_vector = perspective_camera.get('camera_up_vector', {})
        topic.camera_up_vector_x = camera_up_vector.get('x')
        topic.camera_up_vector_y = camera_up_vector.get('y')
        topic.camera_up_vector_z = camera_up_vector.get('z')

        # Flatten the snapshot dictionary and add values to the topic
        topic.snapshot_type = snapshot.get('snapshot_type')
        topic.snapshot_url = snapshot.get('snapshot_url')

        # Combine selection and coloring components
        selection_guids = [comp.get('ifc_guid') for comp in components.get('selection', [])]
        coloring_guids = [color['ifc_guid'] for coloring in components.get('coloring', [])
                        for color in coloring['components']] if 'coloring' in components else []
        all_guids = selection_guids + coloring_guids

        # Append combined IFC GUIDs to the topic's ifc_guids list
        topic.ifc_guids.extend(all_guids)
        topic.ifc_guids = list(set(topic.ifc_guids))  # Remove duplicates if needed

        # Flatten and add the visibility component
        visibility = components.get('visibility', {})
        topic.component_default_visibility = visibility.get('default_visibility')
        topic.component_spaces_visible = visibility.get('view_setup_hints', {}).get('spaces_visible')
        topic.component_openings_visible = visibility.get('view_setup_hints', {}).get('openings_visible')

        # Construct the Viewpoint object with the proper default for clipping_planes
        viewpoint_obj = Viewpoint(
            view_id=viewpoint_data.get('view_id'),
            index=viewpoint_data.get('index'),
            perspective_camera=perspective_camera,
            lines=viewpoint_data.get('lines', []),
            clipping_planes=viewpoint_data.get('clipping_planes', []),  # Set default value for clipping_planes
            snapshot=snapshot,
            components=components,
            guid=viewpoint_guid
        )

        # Set the viewpoint-related Topic attributes
        topic.viewpoint_guid = viewpoint_obj.guid
        topic.viewpoint_view_id = viewpoint_obj.view_id
        topic.index = viewpoint_obj.index
        topic.clipping_planes = viewpoint_obj.clipping_planes

        return viewpoint_obj
    
    def _get_viewpoint_helper(self, args):
        return self.get_viewpoint(*args)

    def get_all_viewpoints(self, topics, pool_size=8):
        # Tuple of topic_id and viewpoint_guid for each topic that requires a viewpoint fetch
        with Pool(pool_size) as pool:
            topic_viewpoint_args = [(topic.guid, topic.viewpoint['guid']) for topic in topics if 'guid' in (topic.viewpoint or {})]
            viewpoints = list(tqdm(pool.imap(self._get_viewpoint_helper, topic_viewpoint_args), total=len(topic_viewpoint_args)))
        
        # Construct topics to viewpoints mapping/r
        viewpoints_dict = {args[0]: viewpoint for args, viewpoint in zip(topic_viewpoint_args, viewpoints) if viewpoint != 'error'}
        
        return viewpoints_dict

    def construct_all_viewpoints(self, topics, pool_size=4):
        topics_viewpoints = self.get_all_viewpoints(topics, pool_size=pool_size)
        for topic in topics:
            if topic.guid in topics_viewpoints:
                viewpoint_data = topics_viewpoints[topic.guid]
                viewpoint_obj = self.construct_viewpoint_data(topic, viewpoint_data)
                topic.viewpoint = viewpoint_obj

    def construct_viewpoint_data(self, topic, viewpoint_data):
        if topic.viewpoint is None or 'guid' not in topic.viewpoint:
            return None

        viewpoint_guid = topic.viewpoint['guid']

        if viewpoint_data == 'error':
            return None  # Handle the error accordingly

        perspective_camera = viewpoint_data.get('perspective_camera', {})
        snapshot = viewpoint_data.get('snapshot', {})
        components = viewpoint_data.get('components', {})

        # Flatten the perspective_camera dictionary and add values to the topic
        topic.field_of_view = perspective_camera.get('field_of_view')
        camera_viewpoint = perspective_camera.get('camera_view_point', {})
        topic.camera_viewpoint_x = camera_viewpoint.get('x')
        topic.camera_viewpoint_y = camera_viewpoint.get('y')
        topic.camera_viewpoint_z = camera_viewpoint.get('z')
        camera_direction = perspective_camera.get('camera_direction', {})
        topic.camera_direction_x = camera_direction.get('x')
        topic.camera_direction_y = camera_direction.get('y')
        topic.camera_direction_z = camera_direction.get('z')
        camera_up_vector = perspective_camera.get('camera_up_vector', {})
        topic.camera_up_vector_x = camera_up_vector.get('x')
        topic.camera_up_vector_y = camera_up_vector.get('y')
        topic.camera_up_vector_z = camera_up_vector.get('z')

        # Flatten the snapshot dictionary and add values to the topic
        topic.snapshot_type = snapshot.get('snapshot_type')
        topic.snapshot_url = snapshot.get('snapshot_url')

        # Combine selection and coloring components
        selection_guids = [comp.get('ifc_guid') for comp in components.get('selection', [])]
        coloring_guids = [color['ifc_guid'] for coloring in components.get('coloring', [])
                        for color in coloring['components']] if 'coloring' in components else []
        all_guids = selection_guids + coloring_guids

        # Append combined IFC GUIDs to the topic's ifc_guids list
        topic.ifc_guids.extend(all_guids)
        topic.ifc_guids = list(set(topic.ifc_guids))  # Remove duplicates if needed

        # Flatten and add the visibility component
        visibility = components.get('visibility', {})
        topic.component_default_visibility = visibility.get('default_visibility')
        topic.component_spaces_visible = visibility.get('view_setup_hints', {}).get('spaces_visible')
        topic.component_openings_visible = visibility.get('view_setup_hints', {}).get('openings_visible')

        # Construct the Viewpoint object with the proper default for clipping_planes
        viewpoint_obj = Viewpoint(
            view_id=viewpoint_data.get('view_id'),
            index=viewpoint_data.get('index'),
            perspective_camera=perspective_camera,
            lines=viewpoint_data.get('lines', []),
            clipping_planes=viewpoint_data.get('clipping_planes', []),  # Set default value for clipping_planes
            snapshot=snapshot,
            components=components,
            guid=viewpoint_guid
        )

        # Set the viewpoint-related Topic attributes
        topic.viewpoint_guid = viewpoint_obj.guid
        topic.viewpoint_view_id = viewpoint_obj.view_id
        topic.index = viewpoint_obj.index
        topic.clipping_planes = viewpoint_obj.clipping_planes

        return viewpoint_obj

    def create_new_issue(self, topic_data, viewpoint_data):
        # We need to create an instance of Topic and Viewpoint from the data provided
        headers = self.headers | {"Content-Type": "application/json"}
        topic_obj = Topic(**topic_data)
        viewpoint_obj = Viewpoint(**viewpoint_data)

        # Convert the objects into dictionary format for the API request
        topic_dict = topic_obj.to_dict()
        view_dict = viewpoint_obj.to_dict()
        
        # Issue creation API endpoint and data
        endpoint = f"{self.BASE_URL}projects/{self.project_id}/topics"
        issue_response = requests.post(endpoint, json=topic_dict, headers=headers)

        if issue_response.ok:
            issue_guid = issue_response.json().get('guid')  # Assume the response provides a guid for the new issue
            # Associate the viewpoint with the newly created issue
            view_endpoint = f"{endpoint}/{issue_guid}/viewpoints"
            view_response = requests.post(view_endpoint, json=view_dict, headers=headers)
            if view_response.ok:
                return "Issue and viewpoint created successfully"
            else:
                return f"Failed to create viewpoint: {view_response.content}"
        else:
            return f"Failed to create issue: {issue_response.content}"

    def delete_topic(self, topic_id):
        try:
            response = requests.delete(
                f"https://open31.connect.trimble.com/bcf/2.1/projects/{self.project_id}/topics/{topic_id}",
                headers=self.headers,
            )
        except:
            return 'error'
        return topic_id, response.text

    def update_viewpoint(self, topic):
        # We need to create an instance of Topic and Viewpoint from the data provided
        headers = self.headers | {"Content-Type": "application/json"}
        view_dict = topic.viewpoint.to_dict()
        issue_guid = topic.guid  # Assume the response provides a guid for the new issue
        # Associate the viewpoint with the newly created issue
        endpoint = f"{self.BASE_URL}projects/{self.project_id}/topics"
        view_endpoint = f"{endpoint}/{issue_guid}/viewpoints"
        view_response = requests.post(view_endpoint, json=view_dict, headers=headers)
        if view_response.ok:
            return "Issue and viewpoint created successfully"
        else:
            return f"Failed to create viewpoint: {view_response.content}"


class Topic:
    def __init__(
        self,
        version,
        guid,
        topic_type,
        topic_status,
        title,
        labels,
        creation_date,
        creation_author,
        creation_author_uuid,
        created_by_uuid,
        modified_date,
        modified_author,
        modified_author_uuid,
        assigned_to,
        assigned_to_uuid,
        assignees,
        description,
        viewpoint,
        files,
    ):
        if files is None:  # Handle the case where files is None
            self.files = []
        else:
            self.files = files
        
        # Now, you can safely construct file-related lists without TypeError
        self.file_names = [file.get('file_name') for file in self.files]
        self.tc_files = [file.get('reference') for file in self.files]
        self.ifc_projects = [file.get('ifc_project') for file in self.files]

        # Initialize viewpoint-related attributes
        if viewpoint is not None and 'guid' in viewpoint:
            self.viewpoint_guid = viewpoint.get('guid')
            self.viewpoint_view_id = viewpoint.get('view_id')
            self.viewpoint_snapshot_url = viewpoint.get('snapshot_url')
            self.viewpoint_snapshot_thumb = viewpoint.get('snapshot_thumb')
        else:
            # Set viewpoint-related attributes to None if no viewpoint data is provided
            self.viewpoint_guid = None
            self.viewpoint_view_id = None
            self.viewpoint_snapshot_url = None
            self.viewpoint_snapshot_thumb = None

        self.version = version
        self.guid = guid
        self.topic_type = topic_type
        self.topic_status = topic_status
        self.title = title
        self.labels = labels
        self.creation_date = creation_date
        self.creation_author = creation_author
        self.creation_author_uuid = creation_author_uuid
        self.created_by_uuid = created_by_uuid
        self.modified_date = modified_date
        self.modified_author = modified_author
        self.modified_author_uuid = modified_author_uuid
        self.assigned_to = assigned_to
        self.assigned_to_uuid = assigned_to_uuid
        self.assignees = assignees
        self.description = description
        self.viewpoint = viewpoint
        self.ifc_guids = []
        self.index = None
        self.clipping_planes = None
        self.field_of_view = None
        self.camera_viewpoint_x = None
        self.camera_viewpoint_y = None
        self.camera_viewpoint_z = None
        self.camera_direction_x = None
        self.camera_direction_y = None
        self.camera_direction_z = None
        self.camera_up_vector_x = None
        self.camera_up_vector_y = None
        self.camera_up_vector_z = None
        self.snapshot_type = None
        self.component_default_visibility = None
        self.component_spaces_visible = None
        self.component_openings_visible = None
    
    def to_dict(self):
        """Convert the Topic instance into a dictionary for the API request."""
        return {
            'version': self.version,
            'guid': self.guid,
            'topic_type': self.topic_type,
            'topic_status': self.topic_status,
            'title': self.title,
            'labels': self.labels,
            'creation_date': self.creation_date,
            'creation_author': self.creation_author,
            'creation_author_uuid': self.creation_author_uuid,
            'created_by_uuid': self.created_by_uuid,
            'modified_date': self.modified_date,
            'modified_author': self.modified_author,
            'modified_author_uuid': self.modified_author_uuid,
            'assigned_to': self.assigned_to,
            'assigned_to_uuid': self.assigned_to_uuid,
            'description': self.description,
            # 'viewpoint' attribute should be handled differently,
            # as it's a complex structure.
            'files': self.files,
        }
    
    def __repr__(self):
        return (
            f"<Topic ID: {self.guid}, "
            f"<Version: {self.version}, "
            f"Title: {self.title}, "
            f"Number of Components: {len(self.ifc_guids)}>"
        )

class Viewpoint:
    def __init__(self, view_id, index, perspective_camera, lines, clipping_planes, snapshot, components, guid):
        self.view_id = view_id
        self.index = index
        self.guid = guid
        self.field_of_view = perspective_camera.get('field_of_view')
        camera_viewpoint = perspective_camera.get('camera_view_point', {})
        self.camera_viewpoint_x = camera_viewpoint.get('x')
        self.camera_viewpoint_y = camera_viewpoint.get('y')
        self.camera_viewpoint_z = camera_viewpoint.get('z')
        camera_direction = perspective_camera.get('camera_direction', {})
        self.camera_direction_x = camera_direction.get('x')
        self.camera_direction_y = camera_direction.get('y')
        self.camera_direction_z = camera_direction.get('z')
        camera_up_vector = perspective_camera.get('camera_up_vector', {})
        self.camera_up_vector_x = camera_up_vector.get('x')
        self.camera_up_vector_y = camera_up_vector.get('y')
        self.camera_up_vector_z = camera_up_vector.get('z')
        self.snapshot_type = snapshot.get('snapshot_type')
        self.snapshot_url = snapshot.get('snapshot_url')
        self.snapshot_data = snapshot.get('snapshot_data')
        self.component_selection = [comp.get('ifc_guid') for comp in components.get('selection', [])]
        self.component_coloring = [comp.get('ifc_guid') for comp in components.get('coloring', [])]
        visibility = components.get('visibility', {})
        self.component_default_visibility = visibility.get('default_visibility')
        self.component_spaces_visible = visibility.get('view_setup_hints', {}).get('spaces_visible')
        self.component_openings_visible = visibility.get('view_setup_hints', {}).get('openings_visible')
        self.clipping_planes = clipping_planes
        self.lines = lines
        
    def to_dict(self):
        """Convert the Viewpoint instance into a dictionary for the API request."""
        return {
            'view_id': self.view_id,
            'index': self.index,
            'guid': self.guid,
            'perspective_camera': {
                'field_of_view': self.field_of_view,
                'camera_view_point': {
                    'x': self.camera_viewpoint_x,
                    'y': self.camera_viewpoint_y,
                    'z': self.camera_viewpoint_z
                },
                'camera_direction': {
                    'x': self.camera_direction_x,
                    'y': self.camera_direction_y,
                    'z': self.camera_direction_z
                },
                'camera_up_vector': {
                    'x': self.camera_up_vector_x,
                    'y': self.camera_up_vector_y,
                    'z': self.camera_up_vector_z
                },
            },
            'snapshot': {
                'snapshot_type': self.snapshot_type,
                'snapshot_data': self.snapshot_data  # Assuming snapshot_data is the encoded snapshot
            },
            'components': {
                'selection': [{'ifc_guid': guid} for guid in self.component_selection],
                'visibility': {
                    'default_visibility': self.component_default_visibility,
                    'view_setup_hints': {
                        'spaces_visible': self.component_spaces_visible,
                        'openings_visible': self.component_openings_visible
                    }
                }
            },
            'clipping_planes': self.clipping_planes,
            'lines': self.lines,
        }