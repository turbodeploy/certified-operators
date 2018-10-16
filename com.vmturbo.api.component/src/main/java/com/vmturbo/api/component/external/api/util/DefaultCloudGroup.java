package com.vmturbo.api.component.external.api.util;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.api.component.external.api.util.condition.CloudTypeCondition;
import com.vmturbo.api.enums.CloudType;

/**
 * To model default Cloud groups. Legacy has defined couple default groups in
 * DefaultGroups.group.topology. This class is to model these groups.
 */
@Immutable
public class DefaultCloudGroup {

    private final String uuid;

    private final String name;

    private final String displayName;

    private final String memberOf;

    private final String serviceEntityTypeName;

    private final CloudTypeCondition condition;

    public DefaultCloudGroup(@Nonnull final String uuid,
                      @Nonnull final String name,
                      @Nonnull final String displayName,
                      @Nonnull final String memberOf,
                      @Nonnull final String serviceEntityTypeName,
                      @Nonnull final CloudTypeCondition condition) {
        this.uuid = Objects.requireNonNull(uuid);
        this.name = Objects.requireNonNull(name);
        this.displayName = Objects.requireNonNull(displayName);
        this.memberOf = Objects.requireNonNull(memberOf);
        this.serviceEntityTypeName = Objects.requireNonNull(serviceEntityTypeName);
        this.condition = Objects.requireNonNull(condition);
    }

    public String getUuid() {
        return uuid;
    }

    public String getName() {
        return name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getMemberOf() {
        return memberOf;
    }

    public String getServiceEntityTypeName() {
        return serviceEntityTypeName;
    }

    public CloudTypeCondition getCondition() {
        return condition;
    }

    public boolean isAllow(CloudType cloudType) {
        return condition.isAllowed(cloudType);
    }
}