package com.vmturbo.mediation.actionscript;

import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;
import com.vmturbo.platform.sdk.probe.properties.PropertySpec;

/**
 * This class holds configurations for the ActionScript probe.
 */
public class ActionScriptProbeConfiguration {
    private final IPropertyProvider propertyProvider;

    /**
     * Property to control the action identified type exposed to the scripts.
     * If true "actionOid" will have stable action id, otherwise action instance id.
     */
    static final IProbePropertySpec<Boolean> USE_STABLE_ACTION_ID_AS_UUID = new PropertySpec<>(
            "useStableActionIdAsUuid", Boolean::valueOf, true);

    /**
     * Constructor.
     *
     * @param propertyProvider probe properties provider
     */
    public ActionScriptProbeConfiguration(final IPropertyProvider propertyProvider) {
        this.propertyProvider = propertyProvider;
    }

    /**
     * Defines the action identifier type exposed to the scripts.
     * If true then expose actions stable id, otherwise instance id as a value of "actionOid" field.
     *
     * @return true if you need to expose stable action id, otherwise instance id
     */
    public boolean useStableActionIdAsUuid() {
        return propertyProvider.getProperty(USE_STABLE_ACTION_ID_AS_UUID);
    }
}