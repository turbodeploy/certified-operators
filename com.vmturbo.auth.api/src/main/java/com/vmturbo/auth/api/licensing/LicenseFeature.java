package com.vmturbo.auth.api.licensing;

/**
 * Available license features. This could have been a protobuf enum too, but I felt a plain java enum
 * provided cleaner handling of the bidirectional string key <==> enum value mapping that we use
 * when translating between license object types. This wasn't trivial with the protobuf enum because
 * of our use of inconsistent casing in the string keys that appear in the license files. (e.g. "SLA"
 * vs "fabric")
 *
 * If the need for the all-caps features goes away (and it might since they don't get specifically
 * enforced anywhere), then we should consider using the protobuf enum instead for storage/transit
 * efficiency.
 */
public enum LicenseFeature {
    PLANNER("planner"),
    AUTOMATED_ACTIONS("automated_actions"),
    ACTION_SCRIPT("action_script"),
    LOAD_BALANCER("loadbalancer"),
    DEPLOY("deploy"),
    AGGREGATION("aggregation"),
    FABRIC("fabric"),
    CLOUD_TARGETS("cloud_targets"),
    PUBLIC_CLOUD("public_cloud"),
    VDI_CONTROL("vdi_control"),
    VMTURBO_API("vmturbo_api"),
    APP_CONTROL("app_control"),
    STORAGE("storage"),
    NETWORK_CONTROL("network_control"),
    CONTAINER_CONTROL("container_control"),
    SCALING("scaling"),
    SLA("SLA"),
    APPLICATIONS("applications"),
    HISTORICAL_DATA("historical_data"),
    CUSTOM_REPORTS("custom_reports"),
    CUSTOMIZED_VIEWS("customized_views"),
    OPTIMIZER("optimizer"),
    GROUP_EDITOR("group_editor"),
    ACTIVE_DIRECTORY("active_directory"),
    FULL_POLICY("full_policy"),
    MULTIPLE_VC("multiple_vc"),
    SCOPED_USER_VIEW("scoped_user_view"),
    CLUSTER_FLATTENING("cluster_flattening"),
    CLOUD_COST("cloud_cost"),
    API2("API2"),
    CUSTOM_POLICIES("custom_policies"),
    ASSESSMENT("assessment");

    private String key;

    LicenseFeature(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
