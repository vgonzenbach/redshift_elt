from configparser import ConfigParser
import os

def get_aws_credentials(profile, credential_filepath='~/.aws/credentials'):
    """Given a profile, pull credentials from aws credentials file"""
    aws_cred = ConfigParser()
    try:
        with open(os.path.expanduser(credential_filepath), 'r') as f:
            aws_cred.read_file(f)

        return aws_cred[profile]['aws_access_key_id'], aws_cred[profile]['aws_secret_access_key']
        
    except FileNotFoundError:
        print("Credentials file not found")
        return None, None
    except KeyError:
        print(f"Profile {profile} not found in credentials file")
        return None, None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None
    
