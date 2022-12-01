package com.vmturbo.common.protobuf.search;

public class SearchableProperties {

    public static final String ENTITY_TYPE = "entityType";

    public static final String OID = "oid";

    public static final String DISPLAY_NAME = "displayName";

    public static final String ENTITY_STATE = "state";

    // Find entities discovered only by the target whose ID is passed in.
    // This is currently used to find entities created by the UserDefinedTopology probe.
    public static final String EXCLUSIVE_DISCOVERING_TARGET = "exclusiveDiscoveringTarget";

    public static final String ENVIRONMENT_TYPE = "environmentType";

    public static final String TAGS_TYPE_PROPERTY_NAME = "tags";

    public static final String DISCOVERED_BY_TARGET = "targetIds";

    public static final String COMMODITY_SOLD_LIST_PROPERTY_NAME = "commoditySoldList";

    public static final String COMMODITY_BOUGHT_LIST_PROPERTY_NAME = "commodityBoughtList";

    public static final String COMMODITY_TYPE_PROPERTY_NAME = "type";

    public static final String COMMODITY_CAPACITY_PROPERTY_NAME = "capacity";

    public static final String VM_INFO_REPO_DTO_PROPERTY_NAME = "virtualMachineInfoRepoDTO";

    public static final String VM_INFO_GUEST_OS_TYPE = "guestOsType";

    public static final String VM_INFO_CORES_PER_SOCKET = "vmsByCoresPerSocket";

    public static final String VM_INFO_SOCKETS = "vmsBySockets";

    public static final String VM_INFO_NUM_CPUS = "numCpus";

    public static final String VENDOR_TOOLS_INSTALLED = "vendorToolsInstalled";

    public static final String VENDOR_TOOLS_VERSION = "vendorToolsVersion";

    public static final String PM_INFO_REPO_DTO_PROPERTY_NAME = "physicalMachineInfoRepoDTO";

    public static final String PM_INFO_NUM_CPUS = "numCpus";

    public static final String PM_INFO_VENDOR = "vendor";

    public static final String PM_INFO_CPU_MODEL = "cpuModel";

    public static final String PM_INFO_MODEL = "model";

    public static final String PM_INFO_TIMEZONE = "timezone";

    public static final String DS_INFO_REPO_DTO_PROPERTY_NAME = "storageInfoRepoDTO";

    public static final String DS_LOCAL = "local";

    /**
     * DTO containing workload controller information.
     */
    public static final String WC_INFO_REPO_DTO_PROPERTY_NAME = "workloadControllerInfoRepoDTO";

    /**
     * Controller type property name used for searching WorkloadControllers by controller type.
     */
    public static final String CONTROLLER_TYPE = "controllerType";

    /**
     * DTO containing service information.
     */
    public static final String SERVICE_INFO_REPO_DTO_PROPERTY_NAME = "serviceInfoRepoDTO";

    /**
     * Property name used for searching Services by kubernetes service type.
     */
    public static final String KUBERNETES_SERVICE_TYPE = "kubernetesServiceType";

    /**
     * Other controller type property name used for searching WorkloadControllers by controller type.
     */
    public static final String OTHER_CONTROLLER_TYPE = "Other";

    /**
     * Property used for searching discovered business accounts which have associated target.
     */
    public static final String ASSOCIATED_TARGET_ID = "associatedTargetId";

    /**
     * Account property name used for search groups owned by business account.
     */
    public static final String ACCOUNT_ID = "accountID";

    /**
     * Attachment state of a storage volume.
     */
    public static final String VOLUME_ATTACHMENT_STATE = "attachmentState";

    /**
     * Attachment state of a storage volume.
     */
    public static final String VOLUME_UNATTACHED_DAYS = "numDaysUnattached";

    /**
     * DTO containing storage volume information.
     */
    public static final String VOLUME_REPO_DTO = "virtualVolumeInfoRepoDTO";

    /**
     * Provider associated with cloud entity.
     */
    public static final String CLOUD_PROVIDER = "cloudProvider";

    /**
     * ID of an entity, local to the vendor associated with a target that discovered the entity.
     */
    public static final String VENDOR_ID = "vendorId";

    /**
     * BusinessAccountInfo class in the repository.
     */
    public static final String BUSINESS_ACCOUNT_INFO_REPO_DTO_PROPERTY_NAME =
        "businessAccountInfoRepoDTO";

    /**
     * Account ID field within the BusinessAccountInfoRepoDTO class.
     */
    public static final String BUSINESS_ACCOUNT_INFO_ACCOUNT_ID = "accountId";

    public static final String VM_CONNECTED_NETWORKS = "connectedNetworks";
    /**
     * Encrypted field from virtual volume information.
     */
    public static final String ENCRYPTED = "encrypted";
    /**
     * Ephemeral field from virtual volume information.
     */
    public static final String EPHEMERAL = "ephemeral";
    /**
     * Deleteable field from virtual volume information.
     */
    public static final String DELETABLE = "deletable";
    /**
     * Whether a VM has active sessions in the desktop pool.
     */
    public static final String VM_DESKTOP_POOL_ACTIVE_SESSIONS = "activeSessions";



    /**
     * Support hot-add memory filter property.
     */
    public static final String HOT_ADD_MEMORY = "hotAddMemory";
    /**
     * Support hot-add cpu filter property.
     */
    public static final String HOT_ADD_CPU = "hotAddCPU";
    /**
     * Support hot-remove cpu filter property.
     */
    public static final String HOT_REMOVE_CPU = "hotRemoveCPU";

    /**
     * Status of a target.
     */
    public static final String TARGET_VALIDATION_STATUS = "validationStatus";

    /**
     * Type of the probe (e.g., VCENTER).
     */
    public static final String PROBE_TYPE = "probeType";

    /**
     * UI Category of the probe (e.g., Hypervisor).
     */
    public static final String PROBE_CATEGORY = "probeCategory";
    /**
     * If the target is hidden.
     */
    public static final String IS_TARGET_HIDDEN = "isTargetHidden";

    /**
     * Marker of a filter that should to though TargetSearchRpc service.
     */
    public static final String TARGET_FILTER_MARKER = "discoveredBy";

    /**
     * Kubernetes cluster filter property.
     */
    public static final String K8S_CLUSTER = "k8sCluster";

    /**
     * DB Server engine property.
     */
    public static final String DB_ENGINE = "dbEngine";

    /**
     * DB Server edition property.
     */
    public static final String DB_EDITION = "dbEdition";

    /**
     * DB Server version property.
     */
    public static final String DB_VERSION = "dbVersion";

    /**
     * Database Server Storage Encryption property.
     */
    public static final String DB_STORAGE_ENCRYPTION = "dbStorageEncryption";

    /**
     * Database Server Storage Autoscaling property.
     */
    public static final String DB_STORAGE_AUTOSCALING = "dbStorageAutoscaling";

    /**
     * Database Server Performance Insights property.
     */
    public static final String DB_PERFORMANCE_INSIGHTS = "dbPerformanceInsights";

    /**
     * Database Server Cluster Role property.
     */
    public static final String DB_CLUSTER_ROLE = "dbClusterRole";

    /**
     * Database Server Storage Type.
     */
    public static final String DB_STORAGE_TYPE = "dbStorageTier";

    /**
     * Database Replication Role.
     */
    public static final String DB_REPLICATION_ROLE = "dbReplicationRole";

    /**
     * Database Pricing Model.
     */
    public static final String DB_PRICING_MODEL = "dbPricingModel";

    /**
     * Database Service Tier.
     */
    public static final String DB_SERVICE_TIER = "dbServiceTier";

    // Application component specs are used for services like Azure App Service webapps and related services like Google App Engine & AWS Elastic Beanstalk.
    /**
     * Application Component Spec Hybrid Connections.
     */
    public static final String APP_COMPONENT_SPEC_HYBRID_CONNECTIONS = "hybridConnectionCount";

    /**
     * Application Component Spec Deployment Slots.
     */
    public static final String APP_COMPONENT_SPEC_DEPLOYMENT_SLOTS = "deploymentSlotCount";

    // Virtual Machine Specs are used for services like Azure App Service Plans and related VMs ie autoscaling groups/scale sets.
    /**
     * Virtual Machine Spec Service Tier.
     */
    public static final String VIRTUAL_MACHINE_SPEC_SERVICE_TIER = "tier";

    /**
     * Virtual Machine Spec App Count.
     */
    public static final String VIRTUAL_MACHINE_SPEC_APP_COUNT = "appCount";

    /**
     * Virtual Machine Spec Days Empty.
     */
    public static final String VIRTUAL_MACHINE_SPEC_DAYS_EMPTY = "daysEmpty";

    /**
     * Property name for DTO containing application service information.
     */
    public static final String APP_SVC_INFO_REPO_DTO = "appServiceInfoRepoDTO";

    /**
     * Info about a VM, Whether it belongs to an azure VDI instance or not.
     */
    public static final String IS_VDI = "isVdi";

    /**
     * The consumer entity type of a compute tier.
     */
    public static final String COMPUTE_TIER_CONSUMER_ENTITY_TYPE = "computeTierConsumerEntityType";

    private SearchableProperties() {}
}
