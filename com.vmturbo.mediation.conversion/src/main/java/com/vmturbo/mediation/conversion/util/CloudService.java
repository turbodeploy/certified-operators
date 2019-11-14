package com.vmturbo.mediation.conversion.util;

import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Hardcoded Cloud services. These CSs are created in aws/azure wrapper probe,
 * which are used to set up the owns relationship from cloud service and tiers.
 * We should follow the following rule:
 * for each new CS, its Id should be inserted in "Title" case format.
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

    AZURE_DATA_SERVICES("azure::CS::DataServices", "Data Services", SDKProbeType.AZURE),
    AZURE_NETWORKING("azure::CS::Networking", "Networking", SDKProbeType.AZURE),
    AZURE_IDENTITY("azure::CS::Identity", "Identity", SDKProbeType.AZURE),

    AZURE_API_MANAGEMENT("azure::CS::APIManagement", "API Management", SDKProbeType.AZURE),
    AZURE_ADVANCED_DATA_SECURITY("azure::CS::AdvancedDataSecurity", "Advanced Data Security", SDKProbeType.AZURE),
    AZURE_ADVANCED_THREAT_PROTECTION("azure::CS::AdvancedThreatProtection", "Advanced Threat Protection", SDKProbeType.AZURE),
    AZURE_APP_CENTER("azure::CS::AppCenter", "App Center", SDKProbeType.AZURE),
    AZURE_APPLICATION_GATEWAY("azure::CS::ApplicationGateway", "Application Gateway", SDKProbeType.AZURE),
    AZURE_APPLICATION_INSIGHTS("azure::CS::ApplicationInsights", "Application Insights", SDKProbeType.AZURE),
    AZURE_AUTOMATION("azure::CS::Automation", "Automation", SDKProbeType.AZURE),
    AZURE_ACTIVE_DIRECTORY_B2C("azure::CS::AzureActiveDirectoryB2C", "Azure Active Directory B2C", SDKProbeType.AZURE),
    AZURE_ACTIVE_DIRECTORY_DOMAIN_SERVICES("azure::CS::AzureActiveDirectoryDomainServices", "Azure Active Directory Domain Services", SDKProbeType.AZURE),
    AZURE_ANALYSIS_SERVICES("azure::CS::AzureAnalysisServices", "Azure Analysis Services", SDKProbeType.AZURE),
    AZURE_APP_SERVICE("azure::CS::AzureAppService", "Azure App Service", SDKProbeType.AZURE),
    AZURE_BASTION("azure::CS::AzureBastion", "Azure Bastion", SDKProbeType.AZURE),
    AZURE_BLOCKCHAIN("azure::CS::AzureBlockchain", "Azure Blockchain", SDKProbeType.AZURE),
    AZURE_BOT_SERVICE("azure::CS::AzureBotService", "Azure Bot Service", SDKProbeType.AZURE),
    AZURE_COSMOS_DB("azure::CS::AzureCosmosDB", "Azure Cosmos DB", SDKProbeType.AZURE),
    AZURE_DDOS_PROTECTION("azure::CS::AzureDDOSProtection", "Azure DDOS Protection", SDKProbeType.AZURE),
    AZURE_DNS("azure::CS::AzureDNS", "Azure DNS", SDKProbeType.AZURE),
    AZURE_DATA_EXPLORER("azure::CS::AzureDataExplorer", "Azure Data Explorer", SDKProbeType.AZURE),
    AZURE_DATA_FACTORY("azure::CS::AzureDataFactory", "Azure Data Factory", SDKProbeType.AZURE),
    AZURE_DATA_FACTORY_V2("azure::CS::AzureDataFactoryV2", "Azure Data Factory v2", SDKProbeType.AZURE),
    AZURE_DATA_SHARE("azure::CS::AzureDataShare", "Azure Data Share", SDKProbeType.AZURE),
    AZURE_DATABASE_MIGRATION_SERVICE("azure::CS::AzureDatabaseMigrationService", "Azure Database Migration Service", SDKProbeType.AZURE),
    AZURE_DATABASE_FOR_MARIADB("azure::CS::AzureDatabaseForMariaDB", "Azure Database for MariaDB", SDKProbeType.AZURE),
    AZURE_DATABASE_FOR_MYSQL("azure::CS::AzureDatabaseforMySQL", "Database for MySQL", SDKProbeType.AZURE),
    AZURE_DATABASE_FOR_POSTGRESQL("azure::CS::AzureDatabaseForPostgreSQL", "Azure Database for PostgreSQL", SDKProbeType.AZURE),
    AZURE_DATABRICKS("azure::CS::AzureDatabricks", "Azure Databricks", SDKProbeType.AZURE),
    AZURE_DEVOPS("azure::CS::AzureDevOps", "Azure DevOps", SDKProbeType.AZURE),
    AZURE_FIREWALL("azure::CS::AzureFirewall", "Azure Firewall", SDKProbeType.AZURE),
    AZURE_FIREWALL_MANAGER("azure::CS::AzureFirewallManager", "Azure Firewall Manager", SDKProbeType.AZURE),
    AZURE_FRONT_DOOR_SERVICE("azure::CS::AzureFrontDoorService", "Azure Front Door Service", SDKProbeType.AZURE),
    AZURE_LAB_SERVICES("azure::CS::AzureLabServices", "Azure Lab Services", SDKProbeType.AZURE),
    AZURE_MACHINE_LEARNING("azure::CS::AzureMachineLearning", "Azure Machine Learning", SDKProbeType.AZURE),
    AZURE_MAPS("azure::CS::AzureMaps", "Azure Maps", SDKProbeType.AZURE),
    AZURE_MONITOR("azure::CS::AzureMonitor", "Azure Monitor", SDKProbeType.AZURE),
    AZURE_NETAPP_FILES("azure::CS::AzureNetAppFiles", "Azure NetApp Files", SDKProbeType.AZURE),
    AZURE_SEARCH("azure::CS::AzureSearch", "Azure Search", SDKProbeType.AZURE),
    AZURE_SITE_RECOVERY("azure::CS::AzureSiteRecovery", "Azure Site Recovery", SDKProbeType.AZURE),
    AZURE_SPRING_CLOUD("azure::CS::AzureSpringCloud", "Azure Spring Cloud", SDKProbeType.AZURE),
    AZURE_STACK("azure::CS::AzureStack", "Azure Stack", SDKProbeType.AZURE),
    AZURE_SYNTHETICS("azure::CS::AzureSynthetics", "Azure Synthetics", SDKProbeType.AZURE),
    AZURE_BACKUP("azure::CS::Backup", "Backup", SDKProbeType.AZURE),
    AZURE_BANDWIDTH("azure::CS::Bandwidth", "Bandwidth", SDKProbeType.AZURE),
    AZURE_BIZTALK_SERVICES("azure::CS::BizTalkServices", "BizTalk Services", SDKProbeType.AZURE),
    AZURE_CLOUD_SERVICES("azure::CS::CloudServices", "Cloud Services", SDKProbeType.AZURE),
    AZURE_COGNITIVE_SERVICES("azure::CS::CognitiveServices", "Cognitive Services", SDKProbeType.AZURE),
    AZURE_CONTAINER_INSTANCES("azure::CS::ContainerInstances", "Container Instances", SDKProbeType.AZURE),
    AZURE_CONTAINER_REGISTRY("azure::CS::ContainerRegistry", "Container Registry", SDKProbeType.AZURE),
    AZURE_CONTENT_DELIVERY_NETWORK("azure::CS::ContentDeliveryNetwork", "Content Delivery Network", SDKProbeType.AZURE),
    AZURE_COST_MANAGEMENT("azure::CS::CostManagement", "Cost Management", SDKProbeType.AZURE),
    AZURE_DATA_BOX("azure::CS::DataBox", "Data Box", SDKProbeType.AZURE),
    AZURE_DATA_CATALOG("azure::CS::DataCatalog", "Data Catalog", SDKProbeType.AZURE),
    AZURE_DATA_LAKE_ANALYTICS("azure::CS::DataLakeAnalytics", "Data Lake Analytics", SDKProbeType.AZURE),
    AZURE_DATA_LAKE_STORE("azure::CS::DataLakeStore", "Data Lake Store", SDKProbeType.AZURE),
    AZURE_DATA_MANAGEMENT("azure::CS::DataManagement", "Data Management", SDKProbeType.AZURE),
    AZURE_DATACENTER_CAPACITY("azure::CS::DatacenterCapacity", "Datacenter Capacity", SDKProbeType.AZURE),
    AZURE_DIGITAL_TWINS("azure::CS::DigitalTwins", "Digital Twins", SDKProbeType.AZURE),
    AZURE_DYNAMICS_365_FOR_CUSTOMER_INSIGHTS("azure::CS::Dynamics365ForCustomerInsights", "Dynamics 365 for Customer Insights", SDKProbeType.AZURE),
    AZURE_EVENT_GRID("azure::CS::EventGrid", "Event Grid", SDKProbeType.AZURE),
    AZURE_EVENT_HUBS("azure::CS::EventHubs", "Event Hubs", SDKProbeType.AZURE),
    AZURE_EXPRESSROUTE("azure::CS::ExpressRoute", "ExpressRoute", SDKProbeType.AZURE),
    AZURE_FUNCTIONS("azure::CS::Functions", "Functions", SDKProbeType.AZURE),
    AZURE_GITHUB("azure::CS::Github", "GitHub", SDKProbeType.AZURE),
    AZURE_HDINSIGHT("azure::CS::HDInsight", "HDInsight", SDKProbeType.AZURE),
    AZURE_HPCCACHE("azure::CS::HPCCache", "HPCCache", SDKProbeType.AZURE),
    AZURE_INSIGHT_AND_ANALYTICS("azure::CS::InsightAndAnalytics", "Insight and Analytics", SDKProbeType.AZURE),
    AZURE_IOT_CENTRAL("azure::CS::IoTCentral", "IoT Central", SDKProbeType.AZURE),
    AZURE_IOT_HUB("azure::CS::IoTHub", "IoT Hub", SDKProbeType.AZURE),
    AZURE_KEY_VAULT("azure::CS::KeyVault", "Key Vault", SDKProbeType.AZURE),
    AZURE_KUSTO("azure::CS::Kusto", "Kusto", SDKProbeType.AZURE),
    AZURE_LOAD_BALANCER("azure::CS::LoadBalancer", "Load Balancer", SDKProbeType.AZURE),
    AZURE_LOG_ANALYTICS("azure::CS::LogAnalytics", "Log Analytics", SDKProbeType.AZURE),
    AZURE_LOGIC_APPS("azure::CS::LogicApps", "Logic Apps", SDKProbeType.AZURE),
    AZURE_MACHINE_LEARNING_SERVICE("azure::CS::MachineLearningService", "Machine Learning Service", SDKProbeType.AZURE),
    AZURE_MACHINE_LEARNING_STUDIO("azure::CS::MachineLearningStudio", "Machine Learning Studio", SDKProbeType.AZURE),
    AZURE_MEDIA_SERVICES("azure::CS::MediaServices", "Media Services", SDKProbeType.AZURE),
    AZURE_MICROSOFT_AZURE_PEERING_SERVICE("azure::CS::MicrosoftAzurePeeringService", "Microsoft Azure Peering Service", SDKProbeType.AZURE),
    AZURE_MICROSOFT_GENOMICS("azure::CS::MicrosoftGenomics", "Microsoft Genomics", SDKProbeType.AZURE),
    AZURE_MIXED_REALITY("azure::CS::MixedReality", "Mixed Reality", SDKProbeType.AZURE),
    AZURE_MULTI_FACTOR_AUTHENTICATION("azure::CS::Multi-FactorAuthentication", "Multi-Factor Authentication", SDKProbeType.AZURE),
    AZURE_NAT_GATEWAY("azure::CS::NATGateway", "NAT Gateway", SDKProbeType.AZURE),
    AZURE_NETWORK_WATCHER("azure::CS::NetworkWatcher", "Network Watcher", SDKProbeType.AZURE),
    AZURE_NOTIFICATION_HUBS("azure::CS::NotificationHubs", "Notification Hubs", SDKProbeType.AZURE),
    AZURE_PLAYFAB("azure::CS::PlayFab", "PlayFab", SDKProbeType.AZURE),
    AZURE_POWER_BI("azure::CS::PowerBI", "Power BI", SDKProbeType.AZURE),
    AZURE_POWER_BI_EMBEDDED("azure::CS::PowerBIEmbedded", "Power BI Embedded", SDKProbeType.AZURE),
    AZURE_REDIS_CACHE("azure::CS::RedisCache", "Redis Cache", SDKProbeType.AZURE),
    AZURE_RESERVATIONUSAGE("azure::CS::ReservationUsage", "ReservationUsage", SDKProbeType.AZURE),
    AZURE_SQL_ADVANCED_THREAT_PROTECTION("azure::CS::SQLAdvancedThreatProtection", "SQL Advanced Threat Protection", SDKProbeType.AZURE),
    AZURE_SQL_DB_EDGE("azure::CS::SQLDBEdge", "SQL DB Edge", SDKProbeType.AZURE),
    AZURE_SQL_DATA_WAREHOUSE("azure::CS::SQLDataWarehouse", "SQL Data Warehouse", SDKProbeType.AZURE),
    AZURE_SQL_DATABASE("azure::CS::SQLDatabase", "SQL Database", SDKProbeType.AZURE),
    AZURE_SQL_SERVER_STRETCH_DATABASE("azure::CS::SQLServerStretchDatabase", "SQL Server Stretch Database", SDKProbeType.AZURE),
    AZURE_SCHEDULER("azure::CS::Scheduler", "Scheduler", SDKProbeType.AZURE),
    AZURE_SECURITY_CENTER("azure::CS::SecurityCenter", "Security Center", SDKProbeType.AZURE),
    AZURE_SENTINEL("azure::CS::Sentinel", "Sentinel", SDKProbeType.AZURE),
    AZURE_SERVICE_BUS("azure::CS::ServiceBus", "Service Bus", SDKProbeType.AZURE),
    AZURE_SERVICE_FABRIC("azure::CS::ServiceFabric", "Service Fabric", SDKProbeType.AZURE),
    AZURE_SERVICE_FABRIC_MESH("azure::CS::ServiceFabricMesh", "Service Fabric Mesh", SDKProbeType.AZURE),
    AZURE_SIGNALR("azure::CS::SignalR", "SignalR", SDKProbeType.AZURE),
    AZURE_SPATIAL_ANCHORS("azure::CS::SpatialAnchors", "Spatial Anchors", SDKProbeType.AZURE),
    AZURE_SPECIALIZED_COMPUTE("azure::CS::SpecializedCompute", "Specialized Compute", SDKProbeType.AZURE),
    AZURE_STORSIMPLE("azure::CS::StorSimple", "StorSimple", SDKProbeType.AZURE),
    AZURE_STORAGE("azure::CS::Storage", "Storage", SDKProbeType.AZURE),
    AZURE_STREAM_ANALYTICS("azure::CS::StreamAnalytics", "Stream Analytics", SDKProbeType.AZURE),
    AZURE_TIME_SERIES_INSIGHTS("azure::CS::TimeSeriesInsights", "Time Series Insights", SDKProbeType.AZURE),
    AZURE_TRAFFIC_MANAGER("azure::CS::TrafficManager", "Traffic Manager", SDKProbeType.AZURE),
    AZURE_VNET_GATEWAY("azure::CS::VNetGateway", "VNet Gateway", SDKProbeType.AZURE),
    AZURE_VPN_GATEWAY("azure::CS::VPNGateway", "VPN Gateway", SDKProbeType.AZURE),
    AZURE_VIRTUAL_MACHINES("azure::CS::VirtualMachines", "Virtual Machines", SDKProbeType.AZURE),
    AZURE_VIRTUAL_MACHINES_LICENSES("azure::CS::VirtualMachinesLicenses", "Virtual Machines Licenses", SDKProbeType.AZURE),
    AZURE_VIRTUAL_NETWORK("azure::CS::VirtualNetwork", "Virtual Network", SDKProbeType.AZURE),
    AZURE_VIRTUAL_WAN("azure::CS::VirtualWAN", "Virtual WAN", SDKProbeType.AZURE),
    AZURE_VISUAL_STUDIO_ONLINE("azure::CS::VisualStudioOnline", "Visual Studio Online", SDKProbeType.AZURE),
    AZURE_VISUAL_STUDIO_SUBSCRIPTION("azure::CS::VisualStudioSubscription", "Visual Studio Subscription", SDKProbeType.AZURE),
    AZURE_WINDOWS_10_IOT_CORE_SERVICES("azure::CS::Windows10IoTCoreServices", "Windows 10 IoT Core Services", SDKProbeType.AZURE),
    AZURE_XAMARIN_UNIVERSITY("azure::CS::XamarinUniversity", "Xamarin University", SDKProbeType.AZURE),

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
