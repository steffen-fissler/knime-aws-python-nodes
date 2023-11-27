# Python-based KNIME Nodes that integrate with AWS Services

This repository contains a set of Python based node extensions that integrate with AWS services.

## New nodes

The new nodes are put into the root category of the node repository in KNIME. Once the Python
extension supports creating categories for nodes, the nodes will be put into a more customary
repository path.

### Amazon Rekognition

The *Amazon Rekognition Detect Faces* node supports passing image data to the Rekognition service. The node outputs the original image
with bounding boxes drawn around the discovered faces. It also outputs a table with other metadata about each discovered
face.

### Supporting nodes

Additional nodes were created to support the *Detect Faces* node. They are needed currently since the Python
extension does not yet support image cells in tables or other non-table ports. The supporting nodes include:

- **AWS Authentication (Python)** for basic AWS authentication credentials (access key and secret)
- **Image Reader (Python)** to read an image file (JPEG) and output the binary data
- **Image Viewer (Python)** to create a JPEG view of a given image

These supporting nodes are temporary and will not be needed as the Python node extension matures.

### Create EC2 Instance

Two nodes that support creating AWS EC2 Instances using the AWS boto3 [create_instances module](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances)


### Manage EC2 Instances

One node that supports stopping, starting, restarting, or terminating an instance.

### Run command on EC2 Instance

One node that supports sending Shell Scripts to run on an EC2 instance using the [SSM Client send_command module](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html#SSM.Client.send_command)


## Developing

Set up a Conda environment that contains the required KNIME libraries for node development.
More info can be found [here](https://docs.knime.com/latest/pure_python_node_extensions_guide/index.html#_prerequisites).

Configure your IDE to point to the Python (python3) executable in the environment you created.
In Visual Studio Code this is done in the properities section for a workspace. Edit the *Python: Default Interpreter Path* variable.
Set it's value to the path to the *python3* executable in your Conda environment. Here's an example path for the default interpreter:
`/Users/jimfalgout/devtools/miniconda3/envs/my_python_env/bin/python3`. Once this path is set, VSC will
recompile the project.

These additional libraries are added to the dev Conda environment to support the nodes:

- **boto3** supporting calls to AWS services
- **pillow** image access and processing

Look in this [section](https://docs.knime.com/latest/pure_python_node_extensions_guide/index.html#tutorial-writing-first-py-node)
of the developer doc for instructions on setting up a local KNIME instance for debugging the node extension(s).

## Bundling

Follow the instructions [here](https://docs.knime.com/latest/pure_python_node_extensions_guide/index.html#extension-bundling)
for bundling the extensions into a local directory. Bundles are created for individual extensions. Once created,
a KNIME instance can install the bundle making the nodes available for use.


