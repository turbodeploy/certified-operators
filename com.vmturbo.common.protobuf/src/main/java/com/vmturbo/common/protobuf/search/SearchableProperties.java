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

    public static final String COMMODITY_TYPE_PROPERTY_NAME = "type";

    public static final String COMMODITY_CAPACITY_PROPERTY_NAME = "capacity";

    public static final String VM_INFO_REPO_DTO_PROPERTY_NAME = "virtualMachineInfoRepoDTO";

    public static final String VM_INFO_GUEST_OS_TYPE = "guestOsType";

    public static final String VM_INFO_NUM_CPUS = "numCpus";

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
     * Status of a target.
     */
    public static final String TARGET_VALIDATION_STATUS = "validationStatus";
    /**
     * Marker of a filter that should to though TargetSearchRpc service.
     */
    public static final String TARGET_FILTER_MARKER = "discoveredBy";

    /**
     * Kubernetes cluster filter property.
     */
    public static final String K8S_CLUSTER = "k8sCluster";

    private SearchableProperties() {}
}
