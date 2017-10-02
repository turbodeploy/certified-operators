package com.vmturbo.group;

import org.immutables.value.Value;

@Value.Immutable
public abstract class GroupDBDefinition {
    public abstract String databaseName();
    public abstract String policyCollection();
    public abstract String groupCollection();
    public abstract String clusterCollection();
}
