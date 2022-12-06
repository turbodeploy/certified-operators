package com.vmturbo.common.protobuf.utils;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.plan.PlanProjectREST.PlanProjectType;

public class StringConstants {
    public static final String ALL_GROUP_MEMBERS = "AllGroupMembers";
    public static final String ASSN_ID = "assn_id";
    public static final String AVG_PROPERTY_VALUE = "avg_property_value";
    public static final String AVG_VALUE = "avg_value";
    public static final String AVG = "avg";
    public static final String AVILABLE_CAPACITY = "available_capacity";
    public static final String AVAILABILITY_ZONE = "availability_zone";
    public static final String BALLOONING = "Ballooning";
    public static final String CAPACITY = "capacity";
    public static final String AVG_CAPACITY = "avg_capacity";
    public static final String MIN_CAPACITY = "min_capacity";
    public static final String MAX_CAPACITY = "max_capacity";
    public static final String EFFECTIVE_CAPACITY = "effective_capacity";
    public static final String CLASS_NAME = "class_name";
    public static final String COMMODITY_KEY = "commodity_key";
    public static final String KEY = "key";
    public static final String RELATED_ENTITY = "relatedEntity";
    public static final String VIRTUAL_DISK = "virtualDisk";
    public static final String CPU = "CPU";
    public static final String CREATION_CLASS_NAME = "creationClassName";
    public static final String DAY = "day";
    public static final String DISPLAY_NAME = "display_name";
    public static final String DISPLAY_NAME_ATTR = "displayName";
    public static final String DPOD = "DPod";
    public static final String CONTAINER = "Container";
    public static final String CONTAINERPOD = "ContainerPod";
    public static final String NAMESPACE = "Namespace";
    /**
     * Container platform cluster such as Kubernetes.
     */
    public static final String CONTAINER_PLATFORM_CLUSTER = "ContainerPlatformCluster";
    public static final String WORKLOAD_CONTROLLER = "WorkloadController";
    public static final String CONTAINER_SPEC = "ContainerSpec";
    public static final String VM_SPEC = "VMSpec";
    public static final String EMPTY_STRING = "";
    public static final String ENTITY = "entity";
    public static final String ENCRYPTION_KEY = "encryptionKey";
    public static final String ENTITIES = "entities";
    public static final String ENTITY_DEST_ID = "entity_dest_id";
    public static final String ENTITY_ID = "entity_id";
    public static final String ENTITY_TYPE = "entity_type";
    public static final String FILENAME = "filename";
    public static final String TITLE = "title";
    public static final String GROUP = "Group";
    public static final String GROUP_NAME = "group_name";
    public static final String GROUP_TYPE = "group_type";
    public static final String GROUP_UUID = "group_uuid";
    public static final String HOUR = "hour";
    public static final String HOUR_NUMBER = "hour_number";
    public static final String ID = "id";
    public static final String INSTANCE_NAME = "instance_name";
    public static final String INSTANCE_TYPE = "instance_type";
    public static final String INTERNAL_NAME = "internal_name";
    public static final String IO_THROUGHPUT = "IOThroughput";
    public static final String VCPU_OVERCOMMITMENT = "vCPUOvercommitment";
    public static final String VMEM_OVERCOMMITMENT = "vMemOvercommitment";
    /**
     * Communication channel assigned to targets. If a target has a channel it can only be
     * discovered by probes with the same channel.
     */
    public static final String COMMUNICATION_BINDING_CHANNEL = "communicationBindingChannel";
    public static final String MAX_PROPERTY_VALUE = "max_property_value";
    public static final String MAX_VALUE = "max_value";
    public static final String MEM = "Mem";
    public static final String METRICS_TURBO_PREFIX = "turbo_";
    public static final String PLATFORM = "platform";
    public static final String TENANCY = "tenancy";
    /**
     * Cloud commitment like savings plans.
     */
    public static final String CLOUD_COMMITMENT = "CloudCommitment";
    /**
     * Indicates whether the instance is optimized for Amazon EBS I/O.
     */
    public static final String EBS_OPTIMIZED = "ebsOptimized";
    public static final String WEIGHTED_VALUE = "weighted_value";
    public static final String CLEAR_TIME = "clear_time";
    public static final String LAST_NOTIFY_TIME = "last_notify_time";
    public static final String SEVERITY = "severity";
    public static final String MEMBER_UUID = "member_uuid";
    public static final String MIN_VALUE="min_value";
    public static final String MIN_DATE="min_date";
    public static final String MAX_DATE="max_date";
    public static final String OID = "oid";
    public static final String NAME = "name";
    public static final String CATEGORY = "category";
    public static final String IMPORTANCE = "importance";
    public static final String RISK = "risk";
    public static final String CLEARED = "cleared";
    public static final String COUNT = "count";
    public static final String TAGS_ATTR = "tags";

    public static final String CPU_PROP = "cpu_prop";
    public static final String VM_UUID = "vm_uuid";
    public static final String DS_UUID = "ds_uuid";
    public static final String VM_PR_UUID = "vm_pr_uuid";

    public static final String NET_THROUGHPUT = "NetThroughput";
    public static final String NUM_CPUS = "numCPUs";
    public static final String NUM_VCPUS = "numVCPUs";
    public static final String PER = "_per_";
    public static final String PREF_USER = "user_";

    public static final String PRICE_INDEX = "priceIndex";
    public static final String CURRENT_PRICE_INDEX = "currentPriceIndex";
    public static final String PRODUCES = "Produces";
    public static final String ROI = "rOI";
    public static final String NEXT_STEP_ROI = "nextStepRoi";
    public static final String CURRENT_EXPENSES = "currentExpenses";
    public static final String NEXT_STEP_EXPENSES = "nextStepExpenses";
    public static final String CURRENT_PROFIT_MARGIN = "currentProfitMargin";
    public static final String NUM_SOCKETS = "numSockets";
    public static final String NUM_ACTIONS = "numActions";
    public static final String ACTION_TYPES = "actionTypes";
    public static final String ACTION_TYPE = "actionType";
    public static final String ACTION_MODES = "actionModes";
    public static final String ACTION_STATES = "actionStates";
    public static final String ACTION_COST_TYPE = "costType";
    public static final String RISK_SEVERITY = "riskSeverity";
    public static final String RISK_SUB_CATEGORY = "riskSubCategory";
    public static final String RISK_DESCRIPTION = "risk";
    public static final String REASON_COMMODITY = "reasonCommodity";
    public static final String NUM_NOTIFICATIONS = "numNotifications";
    public static final String RI_COUPON_COVERAGE = "RICouponCoverage";
    public static final String RI_COUPON_UNITS = "RICoupon";

    public static final String RI_COST = "RICost";
    public static final String HOST = "Host";
    public static final String DATABASE_SERVER = "DatabaseServer";
    public static final String PRODUCER_UUID = "producer_uuid";
    public static final String PROPERTY_SUBTYPE = "property_subtype";
    public static final String PROPERTY_SUBVALUE = "property_subvalue";
    public static final String PROPERTY_TYPE = "property_type";
    public static final String PROVIDER_UUID = "provider_uuid";
    public static final String RATE = "rate";
    public static final String AVG_RATE = "avg_rate";
    public static final String SPENT = "spent";
    public static final String AVG_SPENT = "avg_spent";
    public static final String UUID_COUNT = "uuid_count";
    public static final String SAMPLES = "samples";
    public static final String NEW_SAMPLES = "new_samples";
    public static final String AGGREGATED = "aggregated";
    public static final String HASH = "hash";
    public static final String ARTIFACT_KEY = "artifact_key";
    public static final String Q16VCPU = "Q16VCPU";
    public static final String Q1VCPU = "Q1VCPU";
    public static final String Q2VCPU = "Q2VCPU";
    public static final String Q32VCPU = "Q32VCPU";
    public static final String Q4VCPU = "Q4VCPU";
    public static final String Q8VCPU = "Q8VCPU";
    public static final String RECORDED_ON= "recorded_on";
    public static final String USER_NAME = "user_name";
    public static final String DETAILS = "details";
    public static final String RELATION = "relation";
    public static final String RELATION_BOUGHT = "bought";
    public static final String RELATION_SOLD = "sold";
    public static final String SETYPE_NAME_ATTR = "SETypeName";
    public static final String SNAPSHOT_TIME = "snapshot_time";
    public static final String STORAGE_ACCESS = "StorageAccess";
    public static final String STORAGE_AMOUNT = "StorageAmount";
    public static final String STORAGE_LATENCY = "StorageLatency";
    public static final String STORAGE_USED = "storage_used";
    public static final String SYSTEM_LOAD = "system_load";
    public static final String SUFF_AGG = "_agg";
    public static final String SUFF_GROUP = "_group";
    public static final String SUFF_GROUP_ASSNS_TBL = "_group_assns";
    public static final String SUFF_GROUP_MEMBERS_HELPER_TBL = "_group_members_helper";
    public static final String SUFF_GROUP_MEMBERS_TBL = "_group_members";
    public static final String SUFF_GROUPS_TBL = "_groups";
    public static final String SUFF_INSTANCES_TBL = "_instances";
    public static final String SUFF_PER_GROUP = "_per_group";

    public static final String USED = "used";
    public static final String USED_CAPACITY = "used_capacity";
    public static final String UTILIZATION = "utilization";
    public static final String UUID = "uuid";
    /**
     * UUID column name in HistorydbIO-created temp tables.
     */
    public static final String TARGET_OBJECT_UUID = "target_object_uuid";
    public static final String PEAK = "peak";

    public static final String VAL = "val";
    public static final String VALUE = "value";
    public static final String VCPU = "VCPU";
    public static final String VMEM = "VMem";
    public static final String VSTORAGE = "VStorage";
    public static final String VPOD = "VPod";

    public static final String CLUSTER = "Cluster";
    public static final String VIRTUAL_MACHINE_CLUSTER = "VirtualMachineCluster";
    public static final String STORAGE_CLUSTER = "StorageCluster";
    /**
     * The class name used in UI for resource group.
     */
    public static final String RESOURCE_GROUP = "ResourceGroup";

    /**
     * The class name used in UI for node pools.
     */
    public static final String NODE_POOL = "NodePool";

    /**
     * The class name used in UI for folders.
     */
    public static final String BUSINESS_ACCOUNT_FOLDER = "BusinessAccountFolder";

    /**
     * Group types set.
     */
    public static final Set<String> GROUP_TYPES = ImmutableSet.of(GROUP, CLUSTER, STORAGE_CLUSTER, RESOURCE_GROUP, VIRTUAL_MACHINE_CLUSTER, BUSINESS_ACCOUNT_FOLDER);

    // Entity type class names
    public static final String VIRTUAL_MACHINE = "VirtualMachine";
    public static final String PHYSICAL_MACHINE = "PhysicalMachine";
    public static final String HCI_PHYSICAL_MACHINE = "HCIPhysicalMachine";
    public static final String DATA_CENTER = "DataCenter";
    public static final String STORAGE = "Storage";
    public static final String APPSRV = "ApplicationServer";
    public static final String DATABASE = "Database";
    public static final String APPLICATION = "Application";
    public static final String VIRTUAL_APPLICATION = "VirtualApplication";
    public static final String LOAD_BALANCER = "LoadBalancer";
    public static final String CHASSIS = "Chassis";
    public static final String DISK_ARRAY = "DiskArray";
    public static final String LOGICAL_POOL = "LogicalPool";
    public static final String IO_MODULE = "IOModule";
    public static final String STORAGE_CONTROLLER = "StorageController";
    public static final String SWITCH = "Switch";
    public static final String VDC = "VirtualDataCenter";
    public static final String ZONE = "Zone";
    public static final String REGION = "Region";
    public static final String CLOUD_SERVICE = "CloudService";
    public static final String CLOUD_MANAGEMENT = "Cloud Management";
    public static final String TEMPLATE = "template";
    public static final String TIER = "tier";
    public static final String BUSINESS_UNIT = "businessUnit";
    public static final String BUSINESS_APPLICATION = "BusinessApplication";
    public static final String BUSINESS_TRANSACTION = "BusinessTransaction";
    public static final String APPLICATION_COMPONENT = "ApplicationComponent";
    public static final String APPLICATION_COMPONENT_SPEC = "ApplicationComponentSpec";
    public static final String VIRTUAL_MACHINE_SPEC = "VirtualMachineSpec";
    public static final String RESERVED_INSTANCE = "ReservedInstance";
    public static final String ACCOUNTID = "accountID";
    public static final String NUM_RI = "numRIs";
    public static final String RI_COUPON_UTILIZATION = "RICouponUtilization";
    public static final String CLOUD_COMMITMENT_UTILIZATION = "CloudCommitmentUtilization";
    public static final String CLOUD_COMMITMENT_COVERAGE = "CloudCommitmentCoverage";
    public static final String BILLING_FAMILY = "BillingFamily";
    public static final String DESKTOP_POOL = "DesktopPool";
    public static final String BUSINESS_USER = "BusinessUser";
    public static final String VIEW_POD = "ViewPod";
    public static final String AUTOMATION_POLICY = "Automation Policy";
    public static final String AUTOMATION_POLICIES = "Automation Policies";
    public static final String DEFAULT_POLICIES = "Default Policies";
    // Setting Policy class name
    public static final String SETTING_POLICY = "SettingPolicy";
    /** Processor PPool. */
    public static final String PROCESSOR_POOL = "ProcessorPool";
    /** Service Provider. */
    public static final String SERVICE_PROVIDER = "ServiceProvider";
    /** Compute Tier. */
    public static final String COMPUTE_TIER = "ComputeTier";
    /** Databaes Tier. */
    public static final String DATABASE_TIER = "DatabaseTier";
    /** Database Server Tier. */
    public static final String DATABASE_SERVER_TIER = "DatabaseServerTier";
    /** Storage Tier. */
    public static final String STORAGE_TIER = "StorageTier";
    /** Network. */
    public static final String NETWORK = "Network";
    /** Internet. */
    public static final String INTERNET = "Internet";
    /** Virtual Volume. */
    public static final String VIRTUAL_VOLUME = "VirtualVolume";
    /** Hypervisor Server. */
    public static final String HYPERVISOR_SERVER = "HypervisorServer";
    /** Availability Zone. */
    public static final String AVAILABILITY_ZONE_ENTITY = "AvailabilityZone";

    /**
     * Setting names of default settings.
     */
    public static final String DEFAULTS = " Defaults";
    public static final String PHYSICAL_MACHINE_SETTING_NAME = "Host";
    public static final String IO_MODULE_SETTING_NAME = "IO Module";
    /**
     * The class name used in UI for workloads.
     */
    public static final String WORKLOAD = "Workload";

    public static final String NUM_ENTITIES = "numEntities";
    public static final String NUM_HOSTS = "numHosts";
    public static final String NUM_VMS = "numVMs";
    public static final String NUM_DBSS = "numDBSs";
    public static final String NUM_DBS = "numDBs";
    public static final String NUM_DAS = "numDAs";
    public static final String NUM_LOADBALANCERS = "numLoadBalancers";
    public static final String NUM_DCS = "numDCs";
    public static final String NUM_APPS = "numApps";
    public static final String NUM_SERVICES = "numServices";
    public static final String NUM_NETWORKS = "numNetworks";
    public static final String NUM_TARGETS = "numTargets";
    public static final String NUM_CLUSTERS = "numClusters";
    public static final String NUM_STORAGES = "numStorages";
    public static final String NUM_CONTAINERS = "numContainers";
    public static final String NUM_CONTAINERPODS = "numContainerPods";
    public static final String NUM_WORKLOAD_CONTROLLERS = "numWorkloadControllers";
    public static final String NUM_CORES = "numCores";
    public static final String NUM_VDCS = "numVDCs";
    public static final String NUM_VIRTUAL_DISKS = "numVirtualDisks";
    public static final String NUM_VIRTUAL_VOLUMES = "numVolumes";
    public static final String NUM_VMS_PER_HOST = "numVMsPerHost";
    public static final String NUM_VMS_PER_STORAGE = "numVMsPerStorage";
    public static final String NUM_CNT_PER_VM = "numContainersPerVM";
    public static final String NUM_CPOD_PER_VM = "numContainerPodsPerVM";
    public static final String NUM_CNT_PER_HOST = "numContainersPerHost";
    public static final String NUM_CNT_PER_STORAGE = "numContainersPerStorage";
    public static final String NUM_WORKLOADS = "numWorkloads";
    public static final String HOST_NUM_HOSTS = "Host_numHosts";
    public static final String VM_NUM_VMS = "VM_numVMs";
    public static final String STORAGE_NUM_STORAGES = "Storage_numStorages";
    public static final String CONTAINER_NUM_CONTAINERS = "Container_numContainers";
    public static final String HEADROOM_VMS = "headroomVMs";

    public static final String CURRENT_HEADROOM = "currentHeadroom";
    public static final String CAPACITY_HEADROOM = "emptyClusterHeadroom";
    public static final String EXHAUSTION_DAYS = "exhaustionDays";
    public static final String MONTHLY_GROWTH = "monthlyGrowth";
    public static final String DOLLARS_PER_DAY = "$/day";
    public static final String COST_PRICE = "costPrice";
    public static final String BILLED_COST = "billedCost";
    public static final String TOTAL_COST_FOR_DURATION = "totalCostForDuration";
    public static final String SUPER_SAVINGS = "superSavings";
    public static final String RI_DISCOUNT = "riDiscount";
    public static final String CUMULATIVE = "cumulative";
    public static final String SAVINGS = "savings";
    public static final String SAVINGS_TYPE = "savingsType";
    public static final String SAVINGS_AMOUNT = "savingsAmount";
    public static final String CPU_REDUCTION = "cpuReduction";
    public static final String MEM_REDUCTION = "memReduction";
    public static final String INVESTMENT = "investment";
    public static final String DESIREDVMS = "DesiredVMs";
    public static final String CURRENTVMS = "CurrentVMs";
    public static final String V_POD = "VPod";
    public static final String D_POD = "DPod";
    public static final String SERVICE = "Service";
    public static String[] PROPERTY_SUBTYPE_LIST = {"DesiredVMs","CurrentVMs","currentNumHosts","currentNumStorages","currentUtilization"};
    public static final String CREATE_TIME="create_time";

    public static final String TARGET = "Target";
    public static final String TARGET_TYPE = "targetType";
    public static final String TARGET_UUID_CC = "targetUuid";
    public static final String PROPERTY_SUBTYPE_USED = "used";
    public static final String PROPERTY_SUBTYPE_UTILIZATION = "utilization";
    /**
     * Commodity percentile utilization.
     */
    public static final String PROPERTY_SUBTYPE_PERCENTILE_UTILIZATION = "percentileUtilization";
    public static final String TARGET_UUID = "target_uuid";
    public static final String PROVIDER_ID = "provider_id";
    public static final String CATEGORY_ID = "category_id";

    public static final String CPU_HEADROOM = "CPUHeadroom";
    public static final String MEM_HEADROOM = "MemHeadroom";
    public static final String STORAGE_HEADROOM = "StorageHeadroom";
    public static final String TOTAL_HEADROOM = "TotalHeadroom";
    public static final String CPU_EXHAUSTION = "CPUExhaustion";
    public static final String MEM_EXHAUSTION = "MemExhaustion";
    public static final String STORAGE_EXHAUSTION = "StorageExhaustion";
    public static final String VM_GROWTH = "VMGrowth";
    public static final String VM = "VM";
    public static final String SCOPE_TYPE ="scope_type";
    public static final String SCOPE_UUID = "scope_uuid";
    public static final String ENVIRONMENT_TYPE ="environmentType";
    /** Name of state filter. */
    public static final String STATE = "state";
    public static final String TAG_KEY = "tag_key";
    public static final String TAG_VALUE = "tag_value";
    public static final String N_WORKLOADS = "n_workloads";
    public static final String AVG_N_WORKLOADS = "avg_n_workloads";
    public static final String VMS = "VMs";

    public static final String RELATED_TYPE = "related_type";
    public static final String N_ENTITIES = "n_entities";
    public static final String SAVINGS_UNIT = "savings_unit";

    public static final String DOLLARS_PER_HOUR = "$/h";
    public static final String PROPERTY = "property";

    /**
     * Optimize cloud plan type.
     */
    public static final String OPTIMIZE_CLOUD_PLAN = "OPTIMIZE_CLOUD";

    /**
     * Optimize services only.
     */
    public static final String OPTIMIZE_CLOUD_PLAN__OPTIMIZE_SERVICES = "OPTIMIZE_CLOUD__OPTIMIZE_SERVICES_ONLY";

    /**
     * Optimize services and make RI purchases.
     */
    public static final String OPTIMIZE_CLOUD_PLAN__RIBUY_AND_OPTIMIZE_SERVICES =
            "OPTIMIZE_CLOUD__RIBUY_AND_OPTIMIZE_SERVICES";

    /**
     * Purchase RI only.
     */
    public static final String OPTIMIZE_CLOUD_PLAN__RIBUY_ONLY = "OPTIMIZE_CLOUD__RIBUY_ONLY";

    /**
     * DISABLED action mode. This is used to check if VM scale actions are disabled in OCP.
     */
    public static final String DISABLED = "DISABLED";

    /**
     * AUTOMATIC action mode.  This is used to check if VM scale actions are enabled in OCP.
     */
    public static final String AUTOMATIC = "AUTOMATIC";

    /**
     * Cloud resize is disabled.
     */
    public static final String RESIZE = "resize";

    /**
     * Cloud migration plan type.
     */
    public static final String CLOUD_MIGRATION_PLAN = "CLOUD_MIGRATION";

    /**
     * Allocation (new name: Lift & Shift) plan type of cloud migration plan project. Resize = false.
     */
    public static final String MIGRATION_PLAN__ALLOCATION = "__ALLOCATION";
    public static final String CLOUD_MIGRATION_PLAN__ALLOCATION
            = PlanProjectType.CLOUD_MIGRATION.name() + MIGRATION_PLAN__ALLOCATION;

    /**
     * Consumption (new name: Optimized) plan type of cloud migration plan project. Resize = true.
     */
    public static final String MIGRATION_PLAN__CONSUMPTION = "__CONSUMPTION";
    public static final String CLOUD_MIGRATION_PLAN__CONSUMPTION
            = PlanProjectType.CLOUD_MIGRATION.name() + MIGRATION_PLAN__CONSUMPTION;;

    /**
     * Hardware refresh plan type.
     */
    public static final String RECONFIGURE_HARDWARE_PLAN = "RECONFIGURE_HARDWARE";

    /**
     * String indicating the Business Account entity type in the UI.
     */
    public static final String BUSINESS_ACCOUNT = "BusinessAccount";

    public final static String RESULTS_TYPE = "resultsType";
    public final static String BEFORE_PLAN = "beforePlan";
    public final static String ON_DEMAND_COMPUTE_LICENSE_COST = "ON_DEMAND_COMPUTE_LICENSE_COST";

    public static final String CSP = "CSP";

    public static final String UNKNOWN = "Unknown";

    public static final String STAT_PREFIX_CURRENT = "current";
    public static final String DATA = "data";
    public static final String TEMPLATE_TYPE = "template_type";
    public static final String NUMBER_OF_COUPONS = "numberOfCoupons";

    /**
     * Class name for deployment profiles in the API.
     */
    public static final String SERVICE_CATALOG_ITEM = "ServiceCatalogItem";

    /**
     * The default name of the cluster headroom VM template.
     * Should be in sync with defaultTemplates.json in the plan orchestrator.
     */
    public static final String CLUSTER_HEADROOM_DEFAULT_TEMPLATE_NAME = "headroomVM";

    /**
     * Constant used for Virtual Volume Attachment.
     */
    public static final String ATTACHMENT = "attachment";
    public static final String ATTACHED = "VIRTUAL_VOLUME_ATTACHED";
    public static final String UNATTACHED = "VIRTUAL_VOLUME_UNATTACHED";

    /**
     * Constants used for core quota.
     */
    public static final String CORE_QUOTA_PREFIX = "azureQuotaRemaining";
    public static final String CORE_QUOTA_SEPARATOR = "::";
    public static final String BUSINESS_ACCOUNT_PREFIX = "BUSINESS_ACCOUNT::";
    public static final String TOTAL_CORE_QUOTA = "cores";
    public static final String TOTAL_REGIONAL_VCPUS_QUOTA_DISPLAYNAME = "total Regional vCPUs";

    /**
     * Indicates that validation has successfully completed in the topology processor.
     */
    public static final String TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS = "Discovery in progress";

    /**
     * Indicates that validation is still in progress by the topology processor.
     */
    public static final String TOPOLOGY_PROCESSOR_VALIDATION_IN_PROGRESS = "Validation in progress";

    /**
     * Indicates that validation has successfully completed in the topology processor.
     */
    public static final String TOPOLOGY_PROCESSOR_VALIDATION_SUCCESS = "Validated";

    /**
     * Accepting/rejecting user type for actions accepted/rejected from turbo.
     */
    public static final String TURBO_USER_TYPE = "TURBO_USER";

    /**
     * Accepting/rejecting user type for actions accepted/rejected from external orchestrator.
     */
    public static final String EXTERNAL_ORCHESTRATOR_USER_TYPE = "EXTERNAL_ORCHESTRATOR_USER";

    /**
     * VM Entity property for license OS override in cloud migration plans.
     */
    public static final String PLAN_NEW_OS_TYPE_PROPERTY = "plan.newOsLicenseType";

    /**
     * OS name property for display purposes.
     */
    public static final String PLAN_NEW_OS_NAME_PROPERTY = "plan.newOsLicenseName";

    /**
     * Optimize container cluster plan type.
     */
    public static final String OPTIMIZE_CONTAINER_CLUSTER_PLAN = "OPTIMIZE_CONTAINER_CLUSTER";

    /**
     * Migrate container workloads plan type.
     */
    public static final String MIGRATE_CONTAINER_WORKLOADS_PLAN = "MIGRATE_CONTAINER_WORKLOADS";

    /**
     * Buy RI plan type.
     */
    public static final String BUY_RI_PLAN = "BUY_RI_PLAN";

    /**
     * To be used when a plan (like a BUY_RI_PLAN) which has no defined subtype.
     */
    public static final String PLAN__NO_SUB_TYPE = "PLAN_NO_SUB_TYPE";

    /**
     * Set of all Cloud Plan Types (not including sub plans).
     */
    public static final Set<String> CLOUD_PLAN_TYPES = ImmutableSet.of(OPTIMIZE_CLOUD_PLAN, CLOUD_MIGRATION_PLAN, BUY_RI_PLAN);

    /**
     * Wehbhook key subject in the secure storage
     */
    public static final String WEBHOOK_PASSWORD_SUBJECT = "webhook-password";

    /**
     * Wehbhook key subject in the secure storage
     */
    public static final String WEBHOOK_OAUTH_CLIENT_SECRET_SUBJECT = "webhook-oauth-client-secret";

    /**
     * Database Server Cluster Role entity property key.
     */
    public static final String CLUSTER_ROLE = "cluster_role";

    /**
     * Database Server Storage Encryption state entity property key.
     */
    public static final String STORAGE_ENCRYPTION = "storage_encryption";

    /**
     * Database Server Storage Autoscaling state entity property key.
     */
    public static final String STORAGE_AUTOSCALING = "storage_autoscaling";

    /**
     * Database Server Performance Insights state entity property key.
     */
    public static final String AWS_PERFORMANCE_INSIGHTS = "aws_performance_insights";

    /**
     * Database Server storage tier.
     */
    public static final String DBS_STORAGE_TIER = "storage_tier";

    /**
     * Database replication role.
     */
    public static final String DB_REPLICATION_ROLE = "replication_role";

    /**
     * Database pricing model.
     */
    public static final String DB_PRICING_MODEL = "pricing_model";

    /**
     * Database pricing model.
     */
    public static final String DB_SERVICE_TIER = "server_service_tier";

    /**
     * Azure availability set recommend only policy prefix.
     */
    public static final String AVAILABILITY_SET_RECOMMEND_ONLY_PREFIX = "ASRO:";

    /**
     * A prefix for the cluster commodities sold by cluster.
     */
    public static final String FAKE_CLUSTER_COMMODITY_PREFIX = "FAKE_CLUSTER_COMMODITY_PREFIX";

    /**
     * A key for the access commodities sold by fake cluster.
     */
    public static final String FAKE_CLUSTER_ACCESS_COMMODITY_KEY = "FAKE_CLUSTER_ACCESS_COMMODITY_KEY";


}
