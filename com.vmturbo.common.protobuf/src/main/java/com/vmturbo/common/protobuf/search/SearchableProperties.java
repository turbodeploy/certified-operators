package com.vmturbo.common.protobuf.search;

public class SearchableProperties {

    public static final String ENTITY_TYPE = "entityType";

    public static final String OID = "oid";

    public static final String DISPLAY_NAME = "displayName";

    public static final String ENTITY_STATE = "state";

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
     * BusinessAccountInfo class in the repository.
     */
    public static final String BUSINESS_ACCOUNT_INFO_REPO_DTO_PROPERTY_NAME =
        "businessAccountInfoRepoDTO";

    /**
     * Account ID field within the BusinessAccountInfoRepoDTO class.
     */
    public static final String BUSINESS_ACCOUNT_INFO_ACCOUNT_ID = "accountId";

    public static final String VM_CONNECTED_NETWORKS = "connectedNetworks";

    private SearchableProperties() {}
}
