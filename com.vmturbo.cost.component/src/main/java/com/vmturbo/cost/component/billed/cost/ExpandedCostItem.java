package com.vmturbo.cost.component.billed.cost;

import java.time.Instant;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.cloud.common.scope.CloudScope;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostBucket;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostItem;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CostBilling.CostTagGroup;

/**
 * Represents a {@link BilledCostItem} collapsed with data from its cost bucket and cost data..
 */
@HiddenImmutableImplementation
@Immutable
interface ExpandedCostItem {

    long billingFamilyId();

    long serviceProviderId();

    BilledCostItem costItem();

    default Instant sampleTs() {
        return Instant.ofEpochMilli(parentBucket().getSampleTsUtc());
    }

    @Nonnull
    BilledCostBucket parentBucket();

    default boolean hasCostTagGroup() {
        return costTagGroup() != null;
    }

    @Nullable
    CostTagGroup costTagGroup();

    @Derived
    default CloudScope scope() {
        return CloudScope.builder()
                .resourceId(costItem().hasEntityId() ? costItem().getEntityId() : null)
                .resourceType(costItem().hasEntityType() ? EntityType.forNumber(costItem().getEntityType()) : null)
                .accountId(costItem().hasAccountId() ? costItem().getAccountId() : null)
                .regionId(costItem().hasRegionId() ? costItem().getRegionId() : null)
                .cloudServiceId(costItem().hasCloudServiceId() ? costItem().getCloudServiceId() : null)
                .serviceProviderId(serviceProviderId())
                .build();

    }

    static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for constructing immutable {@link ExpandedCostItem} instances.
     */
    class Builder extends ImmutableExpandedCostItem.Builder {}
}
