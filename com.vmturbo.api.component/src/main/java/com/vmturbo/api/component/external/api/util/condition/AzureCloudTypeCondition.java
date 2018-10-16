package com.vmturbo.api.component.external.api.util.condition;

import com.vmturbo.api.enums.CloudType;

/**
 * Condition to allow Azure
 */
public class AzureCloudTypeCondition extends AbstractCloudTypeCondition {
    @Override
    public boolean isAllowed(final CloudType cloudType) {
        return CloudType.AZURE.equals(cloudType);
    }
}
