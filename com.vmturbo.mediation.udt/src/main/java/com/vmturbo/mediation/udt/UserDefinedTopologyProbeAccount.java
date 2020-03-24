package com.vmturbo.mediation.udt;

import com.vmturbo.platform.sdk.probe.AccountDefinition;
import com.vmturbo.platform.sdk.probe.AccountValue;

/**
 * Account class for the probe 'UserDefinedTopology'.
 */
@AccountDefinition
public class UserDefinedTopologyProbeAccount {

    @AccountValue(targetId = true, displayName = "Target Name", description = "Target Name")
    private String targetName = "User Defined Topology";

}
