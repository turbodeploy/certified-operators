package com.vmturbo.api.component.external.api.util.condition;

import com.vmturbo.api.enums.CloudType;

/**
 * Condition to allow AWS
 */
public class AwsCloudTypeCondition extends AbstractCloudTypeCondition {
    @Override
    public boolean isAllowed(final CloudType cloudType) {
        return CloudType.AWS.equals(cloudType);
    }
}
