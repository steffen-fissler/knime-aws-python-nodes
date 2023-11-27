import json
import logging
LOGGER = logging.getLogger(__name__)



createInstanceDict=dict(
            BlockDeviceMappings=[
                {
                    'DeviceName': 'string',
                    'VirtualName': 'string',
                    'Ebs': {
                        'DeleteOnTermination': True|False,
                        'Iops': 123,
                        'SnapshotId': 'string',
                        'VolumeSize': 123,
                        'VolumeType': 'standard',
                        'KmsKeyId': 'string',
                        'Throughput': 123,
                        'OutpostArn': 'string',
                        'Encrypted': True|False
                    },
                    'NoDevice': 'string'
                },
            ],
            ImageId='string',
            InstanceType='a1.medium',
            Ipv6AddressCount=123,
            Ipv6Addresses=[
                {
                    'Ipv6Address': 'string'
                },
            ],
            KernelId='string',
            KeyName='string',
            MaxCount=123,
            MinCount=123,
            Monitoring={
                'Enabled': True|False
            },
            Placement={
                'AvailabilityZone': 'string',
                'Affinity': 'string',
                'GroupName': 'string',
                'PartitionNumber': 123,
                'HostId': 'string',
                'Tenancy': 'default',
                'SpreadDomain': 'string',
                'HostResourceGroupArn': 'string'
            },
            RamdiskId='string',
            SecurityGroupIds=[
                'string',
            ],
            SecurityGroups=[
                'string',
            ],
            SubnetId='string',
            UserData='string',
            AdditionalInfo='string',
            ClientToken='string',
            DisableApiTermination=True|False,
            DryRun=True|False,
            EbsOptimized=True|False,
            IamInstanceProfile={
                'Arn': 'string',
                'Name': 'string'
            },
            InstanceInitiatedShutdownBehavior='stop',
            NetworkInterfaces=[
                {
                    'AssociatePublicIpAddress': True|False,
                    'DeleteOnTermination': True|False,
                    'Description': 'string',
                    'DeviceIndex': 123,
                    'Groups': [
                        'string',
                    ],
                    'Ipv6AddressCount': 123,
                    'Ipv6Addresses': [
                        {
                            'Ipv6Address': 'string'
                        },
                    ],
                    'NetworkInterfaceId': 'string',
                    'PrivateIpAddress': 'string',
                    'PrivateIpAddresses': [
                        {
                            'Primary': True|False,
                            'PrivateIpAddress': 'string'
                        },
                    ],
                    'SecondaryPrivateIpAddressCount': 123,
                    'SubnetId': 'string',
                    'AssociateCarrierIpAddress': True,
                    'InterfaceType': 'string',
                    'NetworkCardIndex': 123,
                    'Ipv4Prefixes': [
                        {
                            'Ipv4Prefix': 'string'
                        },
                    ],
                    'Ipv4PrefixCount': 123,
                    'Ipv6Prefixes': [
                        {
                            'Ipv6Prefix': 'string'
                        },
                    ],
                    'Ipv6PrefixCount': 123
                },
            ],
            PrivateIpAddress='string',
            ElasticGpuSpecification=[
                {
                    'Type': 'string'
                },
            ],
            ElasticInferenceAccelerators=[
                {
                    'Type': 'string',
                    'Count': 123
                },
            ],
            TagSpecifications=[
                {
                    'ResourceType': 'capacity-reservation',
                    'Tags': [
                        {
                            'Key': 'string',
                            'Value': 'string'
                        },
                    ]
                },
            ],
            LaunchTemplate={
                'LaunchTemplateId': 'string',
                'LaunchTemplateName': 'string',
                'Version': 'string'
            },
            InstanceMarketOptions={
                'MarketType': 'spot',
                'SpotOptions': {
                    'MaxPrice': 'string',
                    'SpotInstanceType': 'one-time',
                    'BlockDurationMinutes': 123,
                    'ValidUntil': "",
                    'InstanceInterruptionBehavior': 'hibernate'
                }
            },
            CreditSpecification={
                'CpuCredits': 'string'
            },
            CpuOptions={
                'CoreCount': 123,
                'ThreadsPerCore': 123
            },
            CapacityReservationSpecification={
                'CapacityReservationPreference': 'open',
                'CapacityReservationTarget': {
                    'CapacityReservationId': 'string',
                    'CapacityReservationResourceGroupArn': 'string'
                }
            },
            HibernationOptions={
                'Configured': True|False
            },
            LicenseSpecifications=[
                {
                    'LicenseConfigurationArn': 'string'
                },
            ],
            MetadataOptions={
                'HttpTokens': 'optional',
                'HttpPutResponseHopLimit': 123,
                'HttpEndpoint': 'disabled',
                'HttpProtocolIpv6': 'disabled',
                'InstanceMetadataTags': 'disabled'
            },
            EnclaveOptions={
                'Enabled': True
            },
            PrivateDnsNameOptions={
                'HostnameType': 'ip-name',
                'EnableResourceNameDnsARecord': True,
                'EnableResourceNameDnsAAAARecord': True
            },
            MaintenanceOptions={
                'AutoRecovery': 'disabled'
            },
            DisableApiStop=True
        )


def convertJSONtoDict(jsonString):
    if len(jsonString)>0:
        try:
            jsonData = json.loads(jsonString)
            try:
                returnDict= dict(jsonData)
                return returnDict
            except Exception as e:
                raise Exception("Unable to convert JSON to dict" + str(e))
        except Exception as e:
            raise Exception("String not Valid JSON" + str(e))

    else:
        raise Exception("No data provided on input")


def ec2Payload(additionalParams, **kwargs):
    dictionary={}
    if isinstance(additionalParams,str):
        if len(additionalParams)>0:
            LOGGER.info("Additional Parameter is filled")
            LOGGER.debug(str(additionalParams))
            additionalParams=convertJSONtoDict(jsonString=additionalParams)
    else:
        LOGGER.info("Additional Parameter is empty")
        additionalParams = {}
        
    useSecAddParam = False
    useIAMProfile = False
    #checkSecGroups

    if ("SecurityGroupIds"in additionalParams or "SecurityGroups" in additionalParams) and "SecurityGroupIds" not in kwargs:
        useSecAddParam = True
        LOGGER.info("Using Security group from Additional Parameters")
    elif ("SecurityGroupIds"in additionalParams or "SecurityGroups" in additionalParams) and "SecurityGroupIds" in kwargs:
        if isinstance(kwargs["SecurityGroupIds"],str):
            if len(kwargs["SecurityGroupIds"])>0:
                raise ValueError("Security Group defined in Configuration and Additional Parameters")
        else:
            useSecAddParam = True
    elif("SecurityGroupIds"not in additionalParams or "SecurityGroups" not in additionalParams) and "SecurityGroupIds" in kwargs:
        if isinstance(kwargs["SecurityGroupIds"],str):
            if len(kwargs["SecurityGroupIds"])>0:
                LOGGER.info("Using Security group from configuration")
                useSecAddParam = False
        else:
            raise ValueError("Security Group from configuration empty and none provided in Additional Paramters")
    else:
        raise ValueError("Security Group missing from Additional Parameters and Configuration")
        
    
    #checkIamProfile

    if "IamInstanceProfile" in kwargs:
        if isinstance(kwargs["IamInstanceProfile"],str):
            if len(kwargs["IamInstanceProfile"])>0:
                useIAMProfile = True
                LOGGER.info("Using IAM Profile from Additional Parameters")
        else:
            useIAMProfile = False

    #build payload dictionary
    for args in kwargs:


        if args != 'additionalParams' and args != 'createInstanceDict' and args in createInstanceDict:
            #check security groups
            if args=="SecurityGroupIds":
                if useSecAddParam == False:
                    arr=[]
                    arr.append((kwargs[args]))
                    dictionary["SecurityGroupIds"]=arr
                    LOGGER.debug("Using Security group from configuration to build EC2 Instance Payload Definition")

            #checkIAMProfiles
            elif args=="IamInstanceProfile":
                if useIAMProfile == True:
                    dic1={'Name':kwargs[args]}
                    dict2={args:dic1}                    
                    dictionary.update(dict2)
                    LOGGER.debug("Using IAM Profile from configuration to build EC2 Instance Payload Definition")  
            else:
                dict2={args:kwargs[args]}
                ##Pickup here with adding Loggers
                LOGGER.debug("Adding {} to EC2 Instance Payload".format(str(dict2))) 
                dictionary.update(dict2)
                LOGGER.debug("Succefully updated payload")
        
    LOGGER.debug("Adding Additional Parameters")
    dictionary.update(additionalParams)
    LOGGER.debug("Succesfully added Additional Paramaters to EC2 Instance Payload")
    return dictionary
                