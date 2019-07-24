package com.vmturbo.mediation.conversion.util;

import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Hardcoded Cloud services. Currently only a few of these are created in aws/azure wrapper
 * probe, which are used to set up the owns relationship from cloud service and tiers.
 *
 * TODO: removing this hard-coded enum when we can dynamically discover all the needed cloud
 * services in main probe, currently main probe only returns a few and missing some cloud services
 * which are needed for setting up owns relationship from services to tiers.
 */
public enum CloudService {

    AWS_EC2("aws::CS::AmazonEC2", "AWS EC2", SDKProbeType.AWS),
    AWS_RDS("aws::CS::AmazonRDS", "AWS RDS", SDKProbeType.AWS),
    AWS_EBS("aws::CS::EBS", "AWS EBS", SDKProbeType.AWS), // pjs: EBS doesn't seem to come in as
    // an actual service in the cloud data. But keeping for
    // now since we prob. can't assume all storage tiers are S3
    AWS_S3("aws::CS::AmazonS3", "AWS S3", SDKProbeType.AWS),
    AWS_DEV_SUPPORT("aws::CS::AWSDeveloperSupport", "AWS Developer Support", SDKProbeType.AWS),
    AWS_CLOUDWATCH("aws::CS::AmazonCloudWatch", "AWS CloudWatch", SDKProbeType.AWS),
    AWS_QUICKSIGHT("aws::CS::AmazonQuickSight", "AWS QuickSight", SDKProbeType.AWS),
    AWS_DYNAMODB("aws::CS::AmazonDynamoDB", "AWS DyanmoDB", SDKProbeType.AWS),
    AWS_VPC("aws::CS::AmazonVPC", "AWS VPC", SDKProbeType.AWS),
    AWS_KMS("aws::CS::awskms", "AWS Key Management Service", SDKProbeType.AWS),
    AWS_EKS("aws::CS::AmazonEKS", "AWS EKS", SDKProbeType.AWS),
    AWS_CLOUDTRAIL("aws::CS::AWSCloudTrail", "AWS Cloud Trail", SDKProbeType.AWS),
    AWS_LAMBDA("aws::CS::AWSLambda", "AWS Lambda", SDKProbeType.AWS),
    AWS_SNS("aws::CS::AmazonSNS", "AWS SNS", SDKProbeType.AWS),

    AZURE_STORAGE("azure::CS::Storage", "Storage", SDKProbeType.AZURE),
    AZURE_VIRTUAL_MACHINES("azure::CS::VirtualMachines", "Azure VirtualMachines", SDKProbeType.AZURE),
    AZURE_DATA_SERVICES("azure::CS::DataServices", "Azure DataServices", SDKProbeType.AZURE),
    AZURE_NETWORKING("azure::CS::Networking", "Azure Networking", SDKProbeType.AZURE),
    AZURE_DATA_MANAGEMENT("azure::CS::DataManagement", "Azure DataManagement", SDKProbeType.AZURE),
    AZURE_IDENTITY("azure::CS::Identity", "Azure Identity", SDKProbeType.AZURE),

    GCP_STORAGE("gcp::CS::Storage", "Storage", SDKProbeType.GCP),
    GCP_VIRTUAL_MACHINES("gcp::CS::VirtualMachines", "GCP VirtualMachines", SDKProbeType.GCP);

    private final String id;
    private final String displayName;
    private final SDKProbeType probeType;

    CloudService(String id, String displayName, SDKProbeType probeType) {
        this.id = id;
        this.displayName = displayName;
        this.probeType = probeType;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getId() {
        return id;
    }

    public SDKProbeType getProbeType() {
        return probeType;
    }
}
