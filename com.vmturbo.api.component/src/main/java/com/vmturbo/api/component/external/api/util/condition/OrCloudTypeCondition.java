package com.vmturbo.api.component.external.api.util.condition;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.api.enums.CloudType;

/**
 * Or condition logic for Cloud condition
 */
public class OrCloudTypeCondition extends AbstractCloudTypeCondition {

    private final CloudTypeCondition condition1;

    private final CloudTypeCondition condition2;

    public OrCloudTypeCondition(@Nonnull final CloudTypeCondition condition1,
                                @Nonnull final CloudTypeCondition condition2) {
        this.condition2 = Objects.requireNonNull(condition2);
        this.condition1 = Objects.requireNonNull(condition1);
    }

    @Override
    public boolean isAllowed(final CloudType cloudType) {
        return condition1.isAllowed(cloudType) || condition2.isAllowed(cloudType);
    }

}
