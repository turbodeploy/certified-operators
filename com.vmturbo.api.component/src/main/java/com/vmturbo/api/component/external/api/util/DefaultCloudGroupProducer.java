package com.vmturbo.api.component.external.api.util;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.external.api.util.condition.AwsCloudTypeCondition;
import com.vmturbo.api.component.external.api.util.condition.AzureCloudTypeCondition;
import com.vmturbo.api.component.external.api.util.condition.CloudTypeCondition;

/**
 * A default Cloud group producer. In legacy default groups in defined in DefaultGroups.group.topology file.
 * TODO: Moving the default Cloud groups to json file, so it can be parsed and loaded automatically, see OM-39826.
 */
public class DefaultCloudGroupProducer {

    private static final Set<DefaultCloudGroup> defaultCloudGroups = buildDefaultGroups();

    public static final String ALL_CLOUD_VM_UUID = "_y-uCEAmMEeepiIloSuTHiA";

    public static final String ALL_CLOUD_DB_AWS_UUID = "_y-uCEAmMEeepiIloSuTHiB";

    public static final String ALL_CLOUD_DB_AZURE_UUID = "_y-uCEAmMEeepiIloSuTHiC";

    public static final String ALL_CLOULD_WORKLOAD_AWS_AND_AZURE_UUID = "123456789";

    /**
     * Load predefined Clould groups.
     * TODO: load the default groups by parsing the DefaultGroups.group.topology file, when it's
     * shared with XL.
     *
     * @return default predefined Cloud groups
     */
    public static Set<DefaultCloudGroup> getDefaultCloudGroup() {
        return defaultCloudGroups;
    }

    private static Set<DefaultCloudGroup> buildDefaultGroups() {
        final CloudTypeCondition awsCondition = new AwsCloudTypeCondition();
        final CloudTypeCondition azureCondition = new AzureCloudTypeCondition();
        final Set<DefaultCloudGroup> groups = Sets.newHashSet();
        groups.add(new DefaultCloudGroup(
                ALL_CLOUD_VM_UUID,
                "GROUP-CloudVMs",
                "All Cloud VMs",
                "_wK4GWWTbEd-Ea97W1LbBSQ",
                "VirtualMachine", awsCondition.or(awsCondition)));
        groups.add(new DefaultCloudGroup(
                ALL_CLOUD_DB_AWS_UUID,
                "GROUP-CloudDBSs",
                "All Cloud DBSs",
                "_wK4GWWTbEd-Ea97W1LbBSQ",
                "DatabaseServer", awsCondition));
        groups.add(new DefaultCloudGroup(
                ALL_CLOUD_DB_AZURE_UUID,
                "GROUP-CloudDBSs",
                "All Cloud DBSs",
                "_wK4GWWTbEd-Ea97W1LbBSQ",
                "Database", azureCondition));
        groups.add(new DefaultCloudGroup(
                ALL_CLOULD_WORKLOAD_AWS_AND_AZURE_UUID,
                "GROUP-CloudWorkloads",
                "All Cloud Workloads",
                "_wK4GWWTbEd-Ea97W1LbBSQ",
                "Workload", awsCondition.or(azureCondition)));
        return ImmutableSet.copyOf(groups);
    }
}





