package com.vmturbo.mediation.webhook;

import com.vmturbo.platform.sdk.probe.AccountDefinition;
import com.vmturbo.platform.sdk.probe.AccountValue;

/**
 * Account values for the Webhook probe.
 */
@AccountDefinition
public class WebhookAccount {

    @AccountValue(targetId = true, displayName = "Target Name", description = "Target Name")
    private final String targetName = "Webhook";

    /**
     * Return target name.
     *
     * @return the name of webhook target
     */
    public String getTargetName() {
        return targetName;
    }

    @Override
    public String toString() {
        // Make sure you do not place any customer secrets in toString()!!!
        return "Webhook targetName = " + getTargetName();
    }
}
