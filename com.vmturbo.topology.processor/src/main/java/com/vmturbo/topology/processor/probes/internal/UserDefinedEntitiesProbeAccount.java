package com.vmturbo.topology.processor.probes.internal;

import com.vmturbo.platform.sdk.probe.AccountDefinition;
import com.vmturbo.platform.sdk.probe.AccountValue;

/**
 * Account class for the probe 'UserDefinedEntities'.
 */
@AccountDefinition
public class UserDefinedEntitiesProbeAccount {

    /**
     * The name of the target field.
     */
    public static final String UDE_FIELD_NAME = "targetName";

    /**
     * The display name of the target field.
     */
    public static final String UDE_DISPLAY_NAME = "Target Name";

    /**
     * Default constructor. Used for creation an instance via Class.newInstance().
     */
    public UserDefinedEntitiesProbeAccount() {
        this.targetName = "UserDefinedEntities::Target";
    }

    @AccountValue(targetId = true,
            displayName = UDE_DISPLAY_NAME,
            description = "URL to fetch custom data")
    private String targetName;

    public String getTargetName() {
        return targetName;
    }
}
