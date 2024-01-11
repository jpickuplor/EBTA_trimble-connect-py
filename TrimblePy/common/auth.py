import os
import base64
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR
from dotenv import load_dotenv, set_key
import webbrowser
from datetime import datetime, timedelta


# Load environment variables
load_dotenv()

class Authentication:
    
    ENV_ACCESS_TOKEN_NAME = 'TRIMBLE_ACCESS_TOKEN'
    ENV_REFRESH_TOKEN_NAME = 'TRIMBLE_REFRESH_TOKEN'
    
    def __init__(self, token_folder='auth', client_id=None, client_secret=None, redirect_url=None, token_retrieval_method=None, sql_available=False, region=None):
        """
        Initializes an instance of the Auth class.

        Args:
            token_folder (str, optional): The folder path where the authentication tokens will be stored. Defaults to 'auth'.
            client_id (str, optional): The client ID for Trimble authentication. If not provided, it will be fetched from the environment variable 'TRIMBLE_CLIENT_ID'.
            client_secret (str, optional): The client secret for Trimble authentication. If not provided, it will be fetched from the environment variable 'TRIMBLE_CLIENT_SECRET'.
            redirect_url (str, optional): The redirect URL for Trimble authentication. If not provided, it will be fetched from the environment variable 'TRIMBLE_REDIRECT_URL'.
            sql_available (bool, optional): Flag indicating whether SQL components should be set up. Defaults to True.
            token_retrieval_method (env,sql,web, optional): Flag indicating whether SQL components should be set up. Defaults to True.
        """
        self.expires_in = None
        self.access_token = None
        self.refresh_token = None
        self.sql_available = sql_available
        self.token_folder = token_folder
        self.auth_endpoint = 'https://id.trimble.com/oauth'
        self.client_id = client_id or os.environ.get('TRIMBLE_CLIENT_ID')
        self.client_secret = client_secret or os.environ.get('TRIMBLE_CLIENT_SECRET')
        self.redirect_url = redirect_url or os.environ.get('TRIMBLE_REDIRECT_URL')
        self.token_retrieval_method = token_retrieval_method
        self.endpoints = {}
        if region:
            self.set_base_url(region)



    def set_base_url(self, selected_region):
        '''
        REGIONS
        'na': North America
        'eu': Europe
        'ap': Asia Pacific
        'ap2': Australia
        '''
        response = requests.get('https://app31.connect.trimble.com/tc/api/2.0/regions', headers={"accept": "application/json"})
        data = response.json()
        for region_info in data:
            if region_info['serviceRegion'] == selected_region:
                self.endpoints['tc'] = region_info['tc-api']
                self.endpoints['objects-sync'] = region_info['objects-sync-api']
                self.endpoints['org'] = region_info['org-api']
                self.endpoints['pset'] = region_info['pset-api']
                self.endpoints['projects'] = region_info['projects-api']
                self.endpoints['wopi'] = region_info['wopi-api']
                self.endpoints['batch'] = region_info['batch-api']
                self.endpoints['user'] = region_info['user-api']
                self.endpoints['model'] = region_info['model-api']
                self.endpoints['topic'] = region_info['topic-api']
                self.endpoints['origin'] = region_info['origin']
                break

    def get_endpoint(self, api_name):
        # Returns the endpoint URL for the given api_name
        return self.endpoints.get(api_name)


# -----------------------------------------------------------------
# .ENV METHODS
# -----------------------------------------------------------------

    def _save_token_env_data(self, access_token, refresh_token, expires_in):
        """
        Save the access token, refresh token, and expires_in timestamp to the .env file.

        Args:
            access_token (str): The access token to be saved.
            refresh_token (str): The refresh token to be saved.
            expires_in (int): The number of seconds until the token expires.
        """
        self._save_token_to_env(self.ENV_ACCESS_TOKEN_NAME, access_token)
        self._save_token_to_env(self.ENV_REFRESH_TOKEN_NAME, refresh_token)
        # Convert expires_in seconds to a datetime object and save as ISO format string
        expires_timestamp = datetime.now() + timedelta(seconds=expires_in)
        self._save_token_to_env('TRIMBLE_TOKEN_EXPIRES', expires_timestamp.isoformat())
    
    def _load_token_env_data(self):
        """
        Load the access token, refresh token, and expires_in timestamp from the .env file.
        
        Returns:
            tuple: A tuple containing the access token, the refresh token, and the expires_in datetime.
        """
        self.access_token = self._load_token_from_env(self.ENV_ACCESS_TOKEN_NAME)
        self.refresh_token = self._load_token_from_env(self.ENV_REFRESH_TOKEN_NAME)
        expires_in_str = self._load_token_from_env('TRIMBLE_TOKEN_EXPIRES')
        self.expires_in = datetime.fromisoformat(expires_in_str) if expires_in_str else None
        return (self.access_token, self.refresh_token, self.expires_in)


    def _save_token_to_env(self, token_type, token):
        """
        Save the specified token to the .env file.

        Args:
            token_type (str): The type of the token (e.g., 'ACCESS_TOKEN', 'REFRESH_TOKEN').
            token (str): The token to be saved.
        """
        env_path = '.env'
        set_key(env_path, token_type, token)


    def _load_token_from_env(self, token_type):
        """
        Load the specified token from the .env file.

        Args:
            token_type (str): The type of the token (e.g., 'ACCESS_TOKEN', 'REFRESH_TOKEN').

        Returns:
            str: The loaded token.
        """
        load_dotenv()
        return os.getenv(token_type)

# -----------------------------------------------------------------
# PATHS
# -----------------------------------------------------------------

    def ensure_token(self):
        if self.expires_in and datetime.now() > self.expires_in:
            if self.refresh_token:
                self.renew_tokens(self.refresh_token)
            else:
                raise Exception("No refresh token available. Please use the web flow to get new tokens.")
                return False
        return True

    def get_token(self):
        """
        Retrieve an access token using the chosen method.

        Returns:
            tuple: A tuple containing the access token and the refresh token. Each can be a string or None.
        """
        if self.access_token and self.refresh_token:
            # expires in is like 1702508094.4117897 - which is unreadable... convert back to minutes and second for print
            expires_in_readable = pd.Timestamp(self.expires_in) - pd.Timestamp.now()
            print("tokens expire in: ", expires_in_readable)
            return self.access_token, self.refresh_token
        
        # First, check for tokens in the selected retrieval method
        if self.token_retrieval_method == 'env':
            access_token, refresh_token, expires_in = self._load_token_env_data()
            self.access_token = access_token
            self.refresh_token = refresh_token
            self.expires_in = expires_in
            if access_token and refresh_token and expires_in > datetime.now():
                return access_token, refresh_token
        
        if self.token_retrieval_method == 'sql':
            df_tokens = self.get_sql_tokens()
            if not df_tokens.empty:
                self.access_token = df_tokens.iloc[0]['accesstoken']
                self.refresh_token = df_tokens.iloc[0]['refreshtoken']
                self.expires_in = df_tokens.iloc[0]['expiresin']
        
        
        # If tokens weren't found in environment variables or another method is used
        # proceed with existing refresh token to renew tokens
        if not self.refresh_token:
            self.refresh_token = self.get_stored_refresh_token()
        
        # If we have refresh token, check if it's close to expiration and renew if necessary
        if self.refresh_token:
            if not self.expires_in or datetime.now() > self.expires_in - timedelta(minutes=1):
                success = self.renew_tokens(self.refresh_token)
                if success:
                    return self.access_token, self.refresh_token
        
        # If still no access token, fall back to the 'store' or 'web' method
        if not self.access_token:
            try:
                self.renew_tokens()
            except:
                self.get_new_tokens_with_authorization_code()
        
        # If we now have tokens return them, otherwise attempt to use 'web' as fallback
        if self.access_token and self.refresh_token:
            if self.token_retrieval_method == 'env':
                self._save_token_to_env(self.ENV_ACCESS_TOKEN_NAME, self.access_token)
                self._save_token_to_env(self.ENV_REFRESH_TOKEN_NAME, self.refresh_token)
            elif self.token_retrieval_method == 'sql':
                self.tokens_to_sql()
            return self.access_token, self.refresh_token
        else:
            self.get_new_tokens_with_authorization_code()
            return self.access_token, self.refresh_token
        


# -----------------------------------------------------------------
# RETURN TOKENS
# -----------------------------------------------------------------
    def print_expiry_time(self):
        if self.expires_in:
            # it is like datetime.datetime(2023, 12, 14, 12, 13, 47, 486629)
            expires_timestamp_print_friendly = pd.Timestamp(self.expires_in)
            print(f"Tokens expire on: {expires_timestamp_print_friendly}")

    def get_stored_access_token(self):
        """
        Retrieves the stored access token based on the token retrieval method.

        Returns:
            str: The stored access token.
        """
        # Check environment variables if the retrieval method is 'env'
        if self.token_retrieval_method == 'env':
            self.access_token = self._load_token_from_env(self.ENV_ACCESS_TOKEN_NAME)
            
        if not self.access_token:
            if self.token_retrieval_method == 'sql':
                df_tokens = self.get_sql_tokens()
                if not df_tokens.empty:
                    return df_tokens.iloc[0]['accesstoken']
                
        if self.access_token:
            return self.access_token
        else:
            # Token not found, we need to issue a new access token
            self.get_new_tokens_with_authorization_code()
            return self._load_token_from_env(self.ENV_ACCESS_TOKEN_NAME)

    
    def get_stored_refresh_token(self):
        """
        Retrieves the stored refresh token based on the token retrieval method.

        Returns:
            str: The stored refresh token.
        """
        # Check environment variables if the retrieval method is 'env'
        if self.token_retrieval_method == 'env':
            self.refresh_token = self._load_token_from_env(self.ENV_REFRESH_TOKEN_NAME)
        if not self.refresh_token:
            if self.token_retrieval_method == 'sql':
                df_tokens = self.get_sql_tokens()
                if not df_tokens.empty:
                    return df_tokens.iloc[0]['refreshtoken']
        return self.refresh_token
    

# -----------------------------------------------------------------
# API CALLS
# -----------------------------------------------------------------


    def _client_credentials_base64(self):
        return base64.b64encode(f"{self.client_id}:{self.client_secret}".encode("ascii")).decode("ascii")

    def _get_authorization_code(self):
        """
        Retrieves the authorization code from the user by opening a browser for authentication.

        Returns:
            str: The authorization code extracted from the redirect URL.
        """
        # Construct the URL for the authorization page
        scope = "openid+project"  # Replace 'project' with the actual scope needed
        url = f"{self.auth_endpoint}/authorize?scope={scope}&response_type=code&client_id={self.client_id}&redirect_uri={self.redirect_url}"
        print('Opening browser for authentication. Please wait for the redirect URL.')
        webbrowser.open(url)
        current_url = input('Paste the redirect URL here (it will contain the authorization code): ')
        return current_url.split('code=')[1].split('&')[0]  # Extracts the authorization code


    def renew_tokens(self, refresh_token=None):
            """
            Renews the access and refresh tokens using the refresh token.

            Returns:
                bool: True if the tokens were successfully renewed, False otherwise.
            """
            if refresh_token is None:
                try:
                    refresh_token = self.refresh_token  
                except:
                    if self.token_retrieval_method == 'env':
                        refresh_token = self._load_token_from_env('refresh')
                    elif self.token_retrieval_method == 'sql':
                        self.get_sql_tokens()
                        refresh_token = self.refresh_token
            if not refresh_token:
                print("No refresh token found. Trying web based renewal.")
                try:
                    self.get_new_tokens_with_authorization_code()
                    return True
                except Exception as e:
                    return False

            headers = {
                'Authorization': f"Basic {self._client_credentials_base64()}",
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json'
            }
            body = {
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token
            }
            response = requests.post(f'{self.auth_endpoint}/token', headers=headers, data=body)
            if response.status_code == 200:
                response_data = response.json()
                self.access_token = response_data['access_token']
                self.refresh_token = response_data['refresh_token']
                expires_in_seconds = response_data.get('expires_in', 3600)
                
                # Convert expires_in_seconds to a datetime object instead of Unix time
                self.expires_in = datetime.now() + timedelta(seconds=expires_in_seconds)
                
                # Check the token retrieval method and save data accordingly
                if self.token_retrieval_method == 'env':
                    self._save_token_env_data(self.access_token, self.refresh_token, expires_in_seconds)
                elif self.token_retrieval_method == 'sql':
                    self.tokens_to_sql()
                else:
                    print('New tokens obtained successfully, available at auth.access_token and auth.refresh_token')
                return True
            else:
                print("Failed to renew tokens. Please try again.")
                return False, response.json()


    def get_new_tokens_with_authorization_code(self):
        """
        Retrieves new access and refresh tokens using the authorization code.

        Returns:
            bool: True if new tokens are obtained and saved successfully, False otherwise.
        """
        try:
            authorization_code = self._get_authorization_code()
            headers = {
                'Authorization': f"Basic {self._client_credentials_base64()}",
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json'
            }
            body = {
                'grant_type': 'authorization_code',
                'code': authorization_code,
                'redirect_uri': self.redirect_url
            }
            response = requests.post(f'{self.auth_endpoint}/token', headers=headers, data=body)

            if response.status_code == 200:
                response_data = response.json()
                self.access_token = response_data['access_token']
                self.refresh_token = response_data['refresh_token']
                expires_in_seconds = response_data.get('expires_in', 3600)
                
                # Convert expires_in_seconds to a datetime object instead of Unix time
                self.expires_in = datetime.now() + timedelta(seconds=expires_in_seconds)
                
                # Check the token retrieval method and save data accordingly
                if self.token_retrieval_method == 'env':
                    self._save_token_env_data(self.access_token, self.refresh_token, expires_in_seconds)
                elif self.token_retrieval_method == 'sql':
                    self.tokens_to_sql()
                elif self.token_retrieval_method == 'web':
                    print('New tokens obtained successfully.')
                return True
            else:
                response.raise_for_status()
        except Exception as e:
            print(f"Failed to get new tokens: {e}")
            return False


# -----------------------------------------------------------------
# SQL METHODS
# -----------------------------------------------------------------


    def get_sql_engine(self):
        if self.sql_available:
            self.sql_server = os.environ.get('SQL_SERVER')
            self.sql_database = os.environ.get('SQL_DATABASE')
            self.sql_schema = os.environ.get('SQL_SCHEMA') 
            self.engine = create_engine(f'mssql+pyodbc://{self.sql_server}/{self.sql_database}?driver=ODBC+Driver+17+for+SQL+Server', fast_executemany=True)
            return self.engine

    def get_sql_tokens(self):
        """
        Retrieves the SQL tokens from the database.

        Returns:
            pandas.DataFrame: A DataFrame containing the SQL tokens.
                If SQL functionality is not available, an empty DataFrame is returned.
        """
        if not self.sql_available:
            print("SQL functionality is not available.")
            return pd.DataFrame()  # Return an empty DataFrame
        engine = self.get_sql_engine()
        query = f"SELECT * FROM {self.sql_schema}.AuthTokens"
        dfq = pd.read_sql(query, engine)  # Use the engine object directly
        if not dfq.empty and self.sql_available:
            self.access_token = dfq.iloc[0]['accesstoken']
            self.refresh_token = dfq.iloc[0]['refreshtoken']
            expires_in = dfq.iloc[0]['expiresin']
            # expires in currently looks like Timestamp('2023-12-14 11:59:05') - change to datetime.datetime(2023, 12, 14, 11, 59, 05, 316722)
            self.expires_in = datetime.fromtimestamp(expires_in.timestamp())
            if self.expires_in and datetime.now() > self.expires_in:
                # Tokens are expired, initiate renewal
                self.renew_tokens()
        return dfq

    def tokens_to_sql(self):
        """
        Save the access token and refresh token to a SQL database table.

        Args:
            access_token (str): The access token to be saved.
            refresh_token (str): The refresh token to be saved.
        """
        engine = self.get_sql_engine()
        access_token = self.access_token
        refresh_token = self.refresh_token
        # back to pd.Timestamp('2023-12-14 11:59:05') from datetime.datetime(2023, 12, 14, 11, 59, 05, 316722)
        expires_in_for_sql = pd.Timestamp(self.expires_in)
        df = pd.DataFrame({'accesstoken': [access_token], 'refreshtoken': [refresh_token], 'expiresin': [expires_in_for_sql]})
        str_columns = df.select_dtypes(include=['object']).columns
        column_types = {column: NVARCHAR(length=4000) for column in str_columns}
        df.to_sql('AuthTokens', con=self.engine, schema=self.sql_schema, if_exists='replace', dtype=column_types, index=False)


    def update_sql_tokens(self):
            """
            Updates the access token and refresh token in the SQL database.
            
            If both the access token and refresh token are available, the tokens are stored in the SQL database.
            Otherwise, a message is printed indicating that there are no stored tokens to update.
            """
            access_token = self.access_token
            refresh_token = self.refresh_token
            if access_token and refresh_token:
                self.tokens_to_sql()
                print("Tokens updated in the SQL database.")
            else:
                print("No stored tokens to update to SQL database.")


    def get_sql_table(self,table_name):
        """
        Retrieves the SQL tokens from the database.

        Returns:
            pandas.DataFrame: A DataFrame containing the SQL tokens.
                If SQL functionality is not available, an empty DataFrame is returned.
        """
        if not self.sql_available:
            print("SQL functionality is not available.")
            return pd.DataFrame()  # Return an empty DataFrame
        engine = self.get_sql_engine()
        query = f"SELECT * FROM {self.sql_schema}.{table_name}"
        dfq = pd.read_sql(query, self.engine)
        return dfq
    