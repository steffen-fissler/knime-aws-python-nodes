from typing import List
import logging
import knime_extension as knext
import boto3
from botocore.exceptions import ClientError
from PIL import Image, ImageDraw, ImageColor
import io
import base64
import pandas as pd
import aws_auth
from os.path import exists


LOGGER = logging.getLogger(__name__)
BINARY_IMAGE_PORT_ID = "com.knime.image.binary"

@knext.node(name="Amazon Rekognition Detect Faces", node_type=knext.NodeType.LEARNER, icon_path="icon.png", category="/")
@knext.input_binary(name="AWS Authentication", description="AWS authentication credentials for accessing services", id=aws_auth.AWS_AUTH_PORT_ID)
@knext.input_binary(name="Input Image", description="Input image data to be analysed", id=BINARY_IMAGE_PORT_ID)
@knext.output_binary(name="Output Image", description="Original image overlayed with bounding boxes of detected faces", id=BINARY_IMAGE_PORT_ID)
@knext.output_table(name="Face Attributes", description="Attributes of each detected face in the image")
@knext.output_view(name="Face Detection View", description="Showing the input image with bounding boxes over discovered faces")
class DetectFacesNode(knext.PythonNode):
    """
    Apply the detect faces function of Amazon Rekognition to an image.
    """

    # Columns of the output table schema
    columns = [
        knext.Column(ktype=knext.string(), name="Color"),
        knext.Column(ktype=knext.int64(), name="Age (low)"),
        knext.Column(ktype=knext.int64(), name="Age (high)"),
        knext.Column(ktype=knext.bool_(), name="Smile"),
        knext.Column(ktype=knext.bool_(), name="Eye Glasses"),
        knext.Column(ktype=knext.bool_(), name="Sun Glasses"),
        knext.Column(ktype=knext.string(), name="Gender"),
        knext.Column(ktype=knext.bool_(), name="Eyes Open"),
        knext.Column(ktype=knext.bool_(), name="Mouth Open"),
        knext.Column(ktype=knext.string(), name="Emotions")
    ]

    # Give each detected face a unique color for the bounding box.
    # TODO - The size of this list limits the number of detected faces supported.
    # TODO - generate these or make a bigger default list.
    colors = ["yellow", "blue", "coral", "green", "goldenrod"]

    def configure(self, configure_context: knext.ConfigurationContext, auth_spec: knext.BinaryPortObjectSpec, image_spec: knext.BinaryPortObjectSpec) -> List[knext.Schema]:
        """
        Configure input ports for AWS creds and the input image and output ports for the 
        decorated image and attributes of the detected faces.
        """

        if auth_spec.id != aws_auth.AWS_AUTH_PORT_ID:
            configure_context.set_warning("Unsupported binary port type: " + auth_spec.id)

        if image_spec.id != BINARY_IMAGE_PORT_ID:
            configure_context.set_warning("Unsupported binary port type: " + image_spec.id)

        # Table schema for the face attributes
        table_schema = knext.Schema.from_columns(columns=self.columns)

        return knext.BinaryPortObjectSpec(BINARY_IMAGE_PORT_ID), table_schema


    def execute(self, exec_context: knext.ExecutionContext, auth_input, image_input):
        """
        Use the AWS Rekognition service to detect faces in the input image.
        For each face detected, draw a bounding box on the image and collect
        face attributes. Output the marked up image and the face attributes.
        """

        # Get AWS credentials and create a rekognition client
        access_key, secret = aws_auth.decode_basic_auth(auth_input)
        session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret)
        client = session.client("rekognition")

        # Open the image to draw on it
        image_bytes = io.BytesIO(image_input)
        image = Image.open(image_bytes)
        image_width, image_height = image.size
        draw = ImageDraw.Draw(image)

        LOGGER.info("Image size {0} x {1}".format(image_width, image_height))

        try:
            # Invoke detect faces function of Rekognition
            response = client.detect_faces(
                Image={'Bytes': image_bytes.getvalue()},
                Attributes=['ALL']
            )            

            # For each face detected, draw a rectangle
            # over the bounds of the face on the image
            # and collect the face attributes.
            all_face_attrs = []
            for (face_detail, color) in zip(response['FaceDetails'], self.colors):
                bb = face_detail['BoundingBox']
                left = image_width * bb['Left']
                top = image_height * bb['Top']
                width = image_width * bb['Width']
                height = image_height * bb['Height']

                points = (
                    (left, top),
                    (left + width, top),
                    (left + width, top + height),
                    (left, top + height),
                    (left, top)
                )

                draw.line(points, fill=ImageColor.getrgb(color), width=4)

                face_attrs = self.get_face_attributes(face_detail, color)
                all_face_attrs.append(face_attrs)

            # Create a dataframe for the output face attributes
            column_names = [ column.name for column in self.columns ]
            pd_data = pd.DataFrame(data=all_face_attrs, columns=column_names)

            # Capture the bytes of the modified image
            buffer = io.BytesIO()
            image.save(buffer, format='JPEG')
            image_bytes = buffer.getvalue()

            # Order is important here: image, attributes and the view.
            return image_bytes, knext.Table.from_pandas(pd_data), knext.view_jpeg(image_bytes)
            # Uncomment below to use the HTML view
            #return image_bytes, knext.Table.from_pandas(pd_data), knext.view_html(self.gen_html(image_bytes))

        except ClientError as err:
            LOGGER.error("error invoking detect faces service: {0}; code: ".format(err.response['Error']['Message'], err.response['Error']['Code']))
            return None


    def get_face_attributes(self, fd: dict, color: str): 
        """Collect attributes from the detect faces results"""

        age_low = fd['AgeRange']['Low']
        age_high = fd['AgeRange']['High']
        smile = fd['Smile']['Value']
        eye_glasses = fd['Eyeglasses']['Value']
        sun_glasses = fd['Sunglasses']['Value']
        gender = fd['Gender']['Value']
        eyes_open = fd['EyesOpen']['Value']
        mouth_open = fd['MouthOpen']['Value']

        # Concatenate all emotions into a single string
        emotions_list = [ emotion['Type'] for emotion in fd['Emotions'] if emotion['Confidence'] > 50.0 ]
        emotions = ", ".join(emotions_list)

        return [color, age_low, age_high, smile, eye_glasses, sun_glasses, gender, eyes_open, mouth_open, emotions]


    # Experimenting with generating HTML with the image embedded and with face attributes.
    # The HTML can then be used as input to an HTML view.
    # Goal is to have the attributes next to the image.
    # TODO only the image is displayed.
    def gen_html(self, image_bytes: bytes) -> str:
        """
        Experimenting here with generating HTML embedding the modified image.
        The HTML can mix the image and the face attributes and be fed into
        an HTML view.
        """

        img_str = base64.b64encode(image_bytes).decode("utf-8")
        html = self.base_html % img_str
        return html

    base_html = """
        <html>
            <head>
                <style>
                    .column {
                        float: left;
                    }
                    .left {
                        width: 75%%;
                    }
                    .right {
                        width: 25%%;
                    }
                    .row:after {
                        content: "";
                        display: table;
                        clear: both;
                    }
                    img { 
                        max-width: 100%%; 
                        height: auto; 
                    }
                </style>
            </head>
            <body>
                <div class="row">
                    <div class="column left">
                        <img src="data:image/jpeg;base64, %s">
                    </div>
                    <div class="column right">
                        <h1>Hello</h1>
                    </div>
                </div>
            </body>
        </html>
    """
    

@knext.node(name="AWS Authentication (Python)", node_type=knext.NodeType.SOURCE, icon_path="icon.png", category="/")
@knext.output_binary(name="Authentication Data", description="AWS authentication credentials", id=aws_auth.AWS_AUTH_PORT_ID)
class SimpleAuthNode(knext.PythonNode):
    """
    Provide AWS authentication credentials usable by nodes integrating with AWS services.

    Properties
    ----------
    access_key_id: an AWS Access Key Identifier 
    secret_key: the secret for the access key
    """

    access_key_id = knext.StringParameter(label="Access Key ID", description="Input an AWS access key identifier")
    secret_key = knext.StringParameter(label="Secret Key", description="Input the secret for the access key")

    def configure(self, configure_context: knext.ConfigurationContext) -> List[knext.Schema]:
        """Configure the node with a single output binary port"""

        return knext.BinaryPortObjectSpec(aws_auth.AWS_AUTH_PORT_ID)

    def execute(self, exec_context: knext.ExecutionContext):
        """Convert the auth info from input parameters into a credential object pushed to that output port"""

        return aws_auth.encode_basic_auth(self.access_key_id, self.secret_key)


@knext.node(name="Image Reader (Python)", node_type=knext.NodeType.SOURCE, icon_path="icon.png", category="/")
@knext.output_binary(name="Output Data", description="Marked up image and metadata", id=BINARY_IMAGE_PORT_ID)
class ImageReaderNode(knext.PythonNode):
    """
    Read an image file and pass the data through a binary port.

    This was built to support reading an image file and passing the data
    through a Python binary port. Passing an KNIME type image in a table
    cell does not work yet.

    This may only support JPEG images. Used JPEG as the image file size
    is generally smaller than other image formats.

    Parameters
    ----------
    filepath_param: A fully qualified path to a local image file.
    (this should use a file chooser widget when it's available)
    """

    filepath_param = knext.StringParameter(label="Path to image file", description="Input a complete file path to an image file")

    def configure(self, configure_context: knext.ConfigurationContext) -> List[knext.Schema]:
        """Configure a single binary output port for image contents"""

        if self.filepath_param == None or not exists(self.filepath_param):
            configure_context.set_warning("Input a valid path to an image file")
            
        return knext.BinaryPortObjectSpec(BINARY_IMAGE_PORT_ID)


    def execute(self, exec_context: knext.ExecutionContext):
        """Read the image file and push the image bytes to the output port"""
        
        im = Image.open(self.filepath_param, "r")
        byteArray = io.BytesIO()
        im.save(byteArray, format='JPEG')
        bytes = byteArray.getvalue()
        LOGGER.info("image size: {0} bytes".format(len(bytes)))

        return bytes


@knext.node(name="Image Viewer (Python)", node_type=knext.NodeType.VISUALIZER, icon_path="icon.png", category="/")
@knext.input_binary(name="Input image", description="Input image to display in a view", id=BINARY_IMAGE_PORT_ID)
@knext.output_view(name="Image View", description="View the input image")
class ImageViewerNode(knext.PythonNode):
    """
    Create a (JPEG) view of the input image.
    """

    # Defines a single binary input port and no output ports.
    def configure(self, configure_context: knext.ConfigurationContext, input_spec: knext.BinaryPortObjectSpec) -> List[knext.Schema]:
        """Configure a single input binary port for the image data"""

        if input_spec.id != BINARY_IMAGE_PORT_ID:
            configure_context.set_warning("Unsupported binary port type: " + input_spec.id)
            
        return []

    # Reads the image bytes from the input port, converts to JPEG
    # and creates a JPEG viewer.
    def execute(self, exec_context: knext.ExecutionContext, input_1):
        """Read the image bytes and prepare them for the JPEG view"""
        
        image_bytes = io.BytesIO(input_1)
        image = Image.open(image_bytes)
        buffer = io.BytesIO()
        image.save(buffer, format='JPEG')
        return knext.view_jpeg(buffer.getvalue())
