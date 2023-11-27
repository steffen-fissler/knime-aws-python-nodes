import logging
import knime_extension as knext
import boto3
import pandas as pd
from typing import List
import ec2_manager
import time
LOGGER = logging.getLogger(__name__)


@knext.node(name="Create EC2 Instance(Python)", node_type=knext.NodeType.SOURCE, icon_path="icon.png", category="/")
@knext.output_table(name="Instance Information", description="Instance Metadata")
class CreateInstance(knext.PythonNode):
    ### fix description
    """

    This node lets you create an AWS EC2 Instance with minimal information. It uses the default credentials provide chain on the machine to authenticate with AWS. See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration on configuring your AWS credentials.
    See for a list of instance types available by region: https://aws.amazon.com/ec2/instance-types/
    You can add additional parameters in the Additional Parameters as a JSON String that follow the format outlined at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances. 
    As an example, if you would like to customize the Block Device Settings, you would put the following into the additional Parameters:
    {"TagSpecifications": [{"ResourceType": "instance","Tags": [{"Key": "Name","Value": "EC2fromKNIME"}]}],"IamInstanceProfile": {"Name": "iamName"},"SecurityGroupIds": ["sg-id"]}


    """
    columns = [
        knext.Column(ktype=knext.string(), name="Instance ID")
    ]
    region = knext.StringParameter("Region", "Region to create the EC2 Instance in","us-east-1")

    image = knext.StringParameter("Image ID", "The image used to create the EC2 Instance. The default is for Ubuntu 20.04 LTS", "ami-08d4ac5b634553e16")
    
    instanceType = knext.StringParameter("Instance Type", "The shape of the EC2 Instance.", "t2.medium")

    subnet = knext.StringParameter("Subnet ID", "The subnet to place the EC2 Instance.", "Enter subnet ID")

    keyName = knext.StringParameter("KeyPair Name", "The name of the Key Pair used to create the EC2 Instance.", "Enter Key Name")

    securityGroupID = knext.StringParameter("Security Group ID", "The ID of the Security Group to Use. Leave blank to pass in multiple Security Group Ids or Names in the Additional Parameters Tab", "Enter Security Group or leave blank to use Additional Parameter instead.")

    iamProfile = knext.StringParameter("Instance IAM Profile Name", "Optional, enter one Profile here or use Additional Parameters to send many or ARNs", "Optionally enter the IAM Instance Profile or leave blank to use Additional Parameter instead.")

    additionalParams = knext.StringParameter("Additional Params", "Optional Additional Params", "Optionally enter the IAM Instance Profile or leave blank to use Additional Parameter instead.")

    waitUntilRunning = knext.BoolParameter("Wait until run?", "Leave checked to wait until the Instance is running to return a response. Uncheck for a faster response, but the instance may not be running.",True)

    def configure(self, configure_context: knext.ConfigurationContext) -> List[knext.Schema]: 
         """Configure a single table output port for Instance ID"""
         table_schema = knext.Schema.from_columns(columns=self.columns)
         return table_schema


    def execute(self, exec_context): 
        """Create Instance """
        df = pd.DataFrame()




        try:
            LOGGER.debug("Try 1")
            payload=ec2_manager.ec2Payload(additionalParams=self.additionalParams,
                   ImageId=self.image,
                   InstanceType=self.instanceType,
                   MinCount=1,
                   MaxCount=1,
                   IamInstanceProfile=self.iamProfile,
                   SecurityGroupIds=self.securityGroupID,
                   KeyName=self.keyName,
                   SubnetId=self.subnet)
        except Exception as e:
            raise ValueError("Error building payload to create EC2 Instance" +str(e))

        LOGGER.debug("Creating EC2 Client")
        ec2Client = boto3.client('ec2', region_name=self.region)
        LOGGER.debug("Created EC2 Client. Creating EC2 Resource")
        ec2Resource = boto3.resource('ec2',region_name=self.region)
        LOGGER.debug("Created EC2 Resource. Creating EC2 Instance")

        try:
            LOGGER.debug("Trying 2")
            resp = ec2Resource.create_instances(**payload)
            resp2=[]
            resp2.append(resp[0].id)
            df['Instance ID'] = resp2
            LOGGER.debug("Created EC2 instance. With Instance ID {}".format(str(df['Instance ID'])))
            if self.waitUntilRunning == True:
                LOGGER.info("Waiting until Instance is running")
                resp[0].wait_until_running()
        except Exception as e:
            raise ValueError("Error creating ec2 instance" + str(e))



        return knext.Table.from_pandas(df)





        ### describe instances

@knext.node(name="Describe EC2 Instance(Python)", node_type=knext.NodeType.SOURCE, icon_path="icon.png", category="/")
@knext.input_table("Instance IDs", "The table containing Instance IDs")
@knext.output_table(name="Instance Information", description="Instance Metadata")
class DescribeInstances(knext.PythonNode):
    ### fix description
    """

    This node retieves the state, and full description of an EC2 Instance on AWS as a JSON String


    """
    columns = [
        knext.Column(ktype=knext.string(), name="Instance State"),
        knext.Column(ktype=knext.string(), name="Description")

    ]
    instanceIds= knext.ColumnParameter(label="Instance ID", description="Choose Column Containing the Instance IDs", port_index=0,include_row_key=False,include_none_column=False)
    region = knext.StringParameter("Region", "Region to create the EC2 Instance in","us-east-1")

    def configure(self, configure_context: knext.ConfigurationContext, input_schema_1) -> List[knext.Schema]: 
         """Configure a single table output port for Instance Description"""
         table_schema = input_schema_1.append(knext.Schema.from_columns(columns=self.columns))
         return table_schema


    def execute(self, exec_context, input_1): 
        """Retrieve Description"""
        try:
            ec2 = boto3.client('ec2', region_name=self.region)
            input_1_pd = input_1.to_pandas()
            column = input_1_pd[self.instanceIds].tolist()
            result=ec2.describe_instances(InstanceIds=column)
            instance_state=[]
            descriptions=[]
            for count, value in enumerate(result['Reservations']):
                instance_state.append(result['Reservations'][count]['Instances'][0]['State']['Name'])
                descriptions.append(str(result['Reservations'][count]))

            input_1_pd["Instance State"]=instance_state
            input_1_pd["Description"]= descriptions
            return knext.Table.from_pandas(input_1_pd)
        except Exception as e:
            LOGGER.error("Unable to retrieve description {}".format(str(e)))
        


        

        


        ## run command on ec2 instance


        ## restart / stop / terminate /start instance
@knext.node(name="Manage EC2 Instance(Python)", node_type=knext.NodeType.SOURCE, icon_path="icon.png", category="/")
@knext.input_table("Instance IDs", "The table containing Instance IDs")
@knext.output_table(name="Instance Information", description="Instance Metadata")
class ManageInstances(knext.PythonNode):
    ### fix description
    """

    This node will stop, start, restart, or terminate an instances based on the input provided. The allowed values for the Operation Performed column
    should only be "stop", "start", "restart", or "terminate"


    """
    columns = [
        knext.Column(ktype=knext.string(), name="Previous State"),
        knext.Column(ktype=knext.string(), name="Operation Perfomed"),
        knext.Column(ktype=knext.string(), name="Response")

    ]
    instanceIds= knext.ColumnParameter(label="Instance ID", description="Choose Column Containing the Instance IDs", port_index=0,include_row_key=False,include_none_column=False)
    operation= knext.ColumnParameter(label="Column Containing Start/Stop/Reboot/Terminate", description="Choose Column Containing the Operation to perform on the Instance", port_index=0,include_row_key=False,include_none_column=False)
    region = knext.StringParameter("Region", "Region to create the EC2 Instance in","us-east-1")
    failOnError = knext.BoolParameter("Fail on Error?", "Leave checked to stop operations if one instance fails.",True)
    def configure(self, configure_context: knext.ConfigurationContext, input_schema_1) -> List[knext.Schema]: 
         """Configure a single table output port for Operation Response"""
         table_schema = input_schema_1.append(knext.Schema.from_columns(columns=self.columns))
         return table_schema


    def execute(self, exec_context, input_1): 
        """Run EC2 Operation"""
        try:
            ec2 = boto3.client('ec2', region_name=self.region)
            input_1_pd = input_1.to_pandas()
            column = input_1_pd[self.instanceIds].tolist()
            operation_column=input_1_pd[self.operation].tolist()
            result=ec2.describe_instances(InstanceIds=column)
        except Exception as e:
            if self.failOnError==True:
                raise ValueError("Unable to retrieve Instance ID for an instance {}".format(str(e)))
            else:
                LOGGER.error("Unable to retrieve description {}".format(str(e)))
        instance_state=[]
        performed_op=[]
        description=[]
        ##To-Do: optimize this. Splice into separate arrays based on operation and pass all instances ID per operation at once
        for count, value in enumerate(result['Reservations']):
            instance_state.append(result['Reservations'][count]['Instances'][0]['State']['Name'])
            #(result['Reservations'][count]['Instances'][0]['State']['Name'])
            try:
                if (str(operation_column[count])).lower() == "start":
                    LOGGER.warning(str(column[count]))
                    resp=ec2.start_instances(InstanceIds=[column[count],])
                    performed_op.append("start")
                    description.append(str(resp))
                elif (str(operation_column[count])).lower() == "stop": 
                    resp=ec2.stop_instances(InstanceIds=[column[count],])
                    performed_op.append("stop")
                    description.append(str(resp))
                elif (str(operation_column[count])).lower() == "restart": 
                    resp=ec2.reboot_instances(InstanceIds=[column[count],])
                    performed_op.append("restart")
                    description.append(str(resp))
                elif (str(operation_column[count])).lower() == "terminate": 
                    resp=ec2.terminate_instances(InstanceIds=[column[count],])
                    performed_op.append("terminate")
                    description.append(str(resp))
                else:
                    performed_op.append("None")
                    description.append("Already in this state")
            except Exception as e:
                if self.failOnError==True:
                    raise ValueError("Unable to resolve operation to perform for instance: {} with error {}".format(str(column[count]), e))
                else:
                    performed_op.append("None")
                    description.append("ERROR: Unable to perform operation")
                    LOGGER.warning("Unable to resolve operation to perform for instance: {} with error {}".format(str(column[count]), e))

                    


        input_1_pd["Previous State"]=instance_state
        input_1_pd["Operation Perfomed"]= performed_op
        input_1_pd["Response"]= description
        return knext.Table.from_pandas(input_1_pd)

        ## run command on ec2 instance

@knext.node(name="Run Shell Command on EC2 Instance(Python)", node_type=knext.NodeType.SOURCE, icon_path="icon.png", category="/")
@knext.input_table("Instance IDs", "The table containing the Command Details")
@knext.output_table(name="Command Information", description="Command Metadata")
class RunCommand(knext.PythonNode):
    ### TO DO - add additional params
    """

    This node will run a command on an EC2 Instance using the AWS-RunShellScript Document as described at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html#SSM.Client.send_command and requires the instance to have the AWS SSM Agent Installed as described https://docs.aws.amazon.com/systems-manager/latest/userguide/ssm-agent.html


    """
    columns = [
        knext.Column(ktype=knext.string(), name="Command ID"),
        knext.Column(ktype=knext.string(), name="Command Response")
        


    ]
    instanceIds= knext.ColumnParameter(label="Instance ID", description="Choose Column Containing the Instance IDs", port_index=0,include_row_key=False,include_none_column=False)
    command= knext.ColumnParameter(label="Column Containing the Command", description="Choose Column Containing the command to run on the Instance", port_index=0,include_row_key=False,include_none_column=False)
    region= knext.ColumnParameter(label="Column Containing Output Region", description="Choose Column Containing the region to output to", port_index=0,include_row_key=False,include_none_column=False)
    outputS3BucketName= knext.ColumnParameter(label="Column Containing the S3 Bucket Name", description="Choose Column Containing the S3 Bucket to Output to", port_index=0,include_row_key=False,include_none_column=False)
    failOnError = knext.BoolParameter("Fail on Error?", "Leave checked to stop operations if one command fails.",True)
    waitUntilDone = knext.BoolParameter("Wait until Command is Done?", "Leave checked to swait for the command response",True)
    def configure(self, configure_context: knext.ConfigurationContext, input_schema_1) -> List[knext.Schema]: 
         """Configure a single table output port for Instance ID"""
         if self.waitUntilDone==True:
            self.columns.append(knext.Column(ktype=knext.string(), name="Output URL"))
            self.columns.append(knext.Column(ktype=knext.string(), name="Standard Output Content"))
            self.columns.append(knext.Column(ktype=knext.string(), name="Output"))
    
         table_schema = input_schema_1.append(knext.Schema.from_columns(columns=self.columns))
         return table_schema


    def execute(self, exec_context, input_1): 
        """Run Command"""
        input_1_pd = input_1.to_pandas()
        ids = input_1_pd[self.instanceIds].tolist()
        commands=input_1_pd[self.command].tolist()
        region=input_1_pd[self.region].tolist()
        s3bucket=input_1_pd[self.outputS3BucketName].tolist()
        commandId=[]
        commandResponse=[]
        outputUrl=[]
        outputContent=[]
        output=[]

        for count, value in enumerate(ids):
            try:
                ssm_client= boto3.client('ssm',region_name=region[count])
                resp = ssm_client.send_command(
                    InstanceIds=[ids[count],],
                    DocumentName="AWS-RunShellScript",
                    Parameters={'commands':[str(commands[count]),]},
                    OutputS3Region=str(region[count]),
                    OutputS3BucketName=str(s3bucket[count])
                )
                command_id = resp['Command']['CommandId']
                commandId.append(command_id)
                commandResponse.append(str(resp))
            except Exception as e:
                if self.failOnError==True:
                    raise ValueError("Unable to run command on instance: {} with error {}".format(str(ids[count]), e))
                else:
                    commandId.append("Error")
                    commandResponse.append("Unable to resolve operation to perform for instance: {} with error {}".format(str(column[count]), e))
                    LOGGER.warning("Unable to resolve operation to perform for instance: {} with error {}".format(str(column[count]), e))
            if self.waitUntilDone == True:
                try:
                    time.sleep(2)

                    ssmoutput = ssm_client.get_command_invocation( CommandId=command_id, InstanceId=value)
                    while ssmoutput['Status'] == "InProgress":
                        ssmoutput = ssm_client.get_command_invocation( CommandId=command_id, InstanceId=value)

                    outputUrl.append(ssmoutput['StandardOutputUrl'])
                    outputContent.append(ssmoutput['StandardOutputContent'])
                    output.append(str(ssmoutput))
                except Exception as e:
                    if self.failOnError==True:
                        raise ValueError("Unable to wait for command on instance: {} with error {}".format(str(ids[count]), e))
                    else:
                        outputUrl.append("Error")
                        outputContent.append("Unable to wait for command on instance: {} with error {}".format(str(column[count]), e))
                        output.append("Error")
                        LOGGER.warning("Unable to resolve operation to perform for instance: {} with error {}".format(str(column[count]), e))




                    


        input_1_pd["Command ID"]=command_id
        input_1_pd["Command Response"]= commandResponse
        if self.waitUntilDone==True:
            input_1_pd["Output URL"]=outputUrl
            input_1_pd["Standard Output Content"]=outputContent
            input_1_pd["Output"]=output
        return knext.Table.from_pandas(input_1_pd)


## create instances table input

@knext.node(name="Create EC2 Instance Table Input (Python)", node_type=knext.NodeType.SOURCE, icon_path="icon.png", category="/")
@knext.input_table("Instance Data", "The table containing the Instance Details")
@knext.output_table(name="Instance Information", description="Instance Metadata")
class CreateInstanceTable(knext.PythonNode):
    ### fix description
    """

    This node lets you create many AWS EC2 Instance with minimal information. It uses the default credentials provide chain on the machine to authenticate with AWS. See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration on configuring your AWS credentials.
    See for a list of instance types available by region: https://aws.amazon.com/ec2/instance-types/
    You can add additional parameters in the Additional Parameters as a JSON String that follow the format outlined at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances. 
    As an example, if you would like to customize the Block Device Settings, you would put the following into the additional Parameters:
    {"TagSpecifications": [{"ResourceType": "instance","Tags": [{"Key": "Name","Value": "EC2fromKNIME"}]}],"IamInstanceProfile": {"Name": "iamName"},"SecurityGroupIds": ["sg-id"]}


    """
    columns = [
        knext.Column(ktype=knext.string(), name="Instance IDs"),
        knext.Column(ktype=knext.string(), name="Response")
    ]
    region = knext.ColumnParameter(label="Region Column", description="Choose Column Containing the Regions", port_index=0,include_row_key=False,include_none_column=False)

    image = knext.ColumnParameter(label="Image/AMI Column", description="Choose Column Containing the Image", port_index=0,include_row_key=False,include_none_column=False)
    
    instanceType = knext.ColumnParameter(label="Instance Type Column", description="Choose Column Containing the instance Type", port_index=0,include_row_key=False,include_none_column=False)

    subnet = knext.ColumnParameter(label="Subnet Column", description="Choose Column Containing the Subnet", port_index=0,include_row_key=False,include_none_column=False)

    keyName = knext.ColumnParameter(label="Keypair Column", description="Choose Column Containing the Keypair", port_index=0,include_row_key=False,include_none_column=False)
    
    securityGroupID = knext.ColumnParameter(label="Security Group Column", description="Choose Column Containing the Security Group or leave blank", port_index=0,include_row_key=False,include_none_column=False)

    iamProfile = knext.ColumnParameter(label="IAM Profile Column", description="Choose Column Containing the IAM Profile or leave blank", port_index=0,include_row_key=False,include_none_column=False)

    additionalParams = knext.ColumnParameter(label="Additional Parameters Column", description="Choose Column Containing the Additonal Paramets or leave blank", port_index=0,include_row_key=False,include_none_column=False)

    waitUntilRunning = knext.BoolParameter("Wait until run?", "Leave checked to wait until the Instance is running to return a response. Uncheck for a faster response, but the instance may not be running.",True)

    failOnError = knext.BoolParameter("Fail on Error?", "Leave checked to abort the node if an Instance fails to create running to return a response.",True)

    def configure(self, configure_context: knext.ConfigurationContext, input_schema_1) -> List[knext.Schema]: 
         """Configure a single table output port for Instance ID"""
         table_schema = input_schema_1.append(knext.Schema.from_columns(columns=self.columns))
         return table_schema


    def execute(self, exec_context, input_1): 
        """Create Instance """





        input_1_pd = input_1.to_pandas()
        regions = input_1_pd[self.region].tolist()
        images=input_1_pd[self.image].tolist()
        instanceTypes=input_1_pd[self.instanceType].tolist()
        subnets=input_1_pd[self.subnet].tolist()
        securityGroupIDs=input_1_pd[self.securityGroupID].tolist()
        iamProfiles=input_1_pd[self.iamProfile].tolist()
        additionalParamss=input_1_pd[self.additionalParams].tolist()
        keyNames=input_1_pd[self.keyName].tolist()
        instanceIds=[]
        instanceResponses=[]

        for count, value in enumerate(regions):
            try:
                payload=ec2_manager.ec2Payload(additionalParams=additionalParamss[count],
                    ImageId=images[count],
                    InstanceType=instanceTypes[count],
                    MinCount=1,
                    MaxCount=1,
                    IamInstanceProfile=iamProfiles[count],
                    SecurityGroupIds=securityGroupIDs[count],
                    KeyName=keyNames[count],
                    SubnetId=subnets[count])


                try:
                    LOGGER.debug("Creating EC2 Client")
                    ec2Client = boto3.client('ec2', region_name=regions[count])
                    LOGGER.debug("Created EC2 Client. Creating EC2 Resource")
                    ec2Resource = boto3.resource('ec2',region_name=regions[count])
                    LOGGER.debug("Created EC2 Resource. Creating EC2 Instance")
                    resp = ec2Resource.create_instances(**payload)
                    instanceIds.append(resp[0].id)
                    instanceResponses.append(str(resp))

                    LOGGER.debug("Created EC2 instance. With Instance ID {}".format(str(instanceIds[count])))
                    if self.waitUntilRunning == True:
                        LOGGER.info("Waiting until Instance is running")
                        resp[0].wait_until_running()
                except Exception as e:
                    if self.failOnError==True:
                        raise ValueError(("Error creating ec2 instance " + str(e)))
                    else:
                        LOGGER.warning(("Error creating ec2 instance " + str(e)))
                        instanceIds[count]="ERROR"
                        instanceResponses[count]=("Error creating ec2 instance " + str(e))



            except Exception as e:
                if self.failOnError==True:
                        raise ValueError("Error building payload to create EC2 Instance " +str(e))
                else:
                    LOGGER.warning("Error building payload to create EC2 Instance " +str(e))
                    instanceIds[count]="ERROR"
                    instanceResponses[count]=("Error building payload to create EC2 Instance " +str(e))






        input_1_pd["Instance IDs"]=instanceIds
        input_1_pd["Response"]=instanceResponses
        return knext.Table.from_pandas(input_1_pd)