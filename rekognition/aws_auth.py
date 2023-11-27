import json
import base64


# The port identifier for AWS authentication credential objects ports
AWS_AUTH_PORT_ID = "com.knime.aws.authentication"

def encode_basic_auth(access_key_id: str, secret: str) -> bytes:
    """Encode basic auth data to pass through a binary port"""

    auth_data = { "accessKeyId": access_key_id, "secret": secret }
    auth_str = json.dumps(auth_data)
    return base64.b64encode(bytes(auth_str, "utf-8"))


def decode_basic_auth(auth_data: bytes) -> tuple[str, str]:
    """Decode basic auth data passed through a binary port"""

    auth_str = base64.b64decode(str(auth_data, "utf-8"))
    auth_dict = json.loads(auth_str)
    access_key = auth_dict['accessKeyId']
    secret = auth_dict['secret']
    return access_key, secret