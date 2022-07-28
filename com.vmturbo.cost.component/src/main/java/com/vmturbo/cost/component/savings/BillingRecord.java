package com.vmturbo.cost.component.savings;

import java.time.LocalDateTime;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.immutables.value.Value;
import org.immutables.value.Value.Derived;

import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;

/**
 * Represents a subset of billed cost table data that is needed for savings calculations. Only
 * billed cost rows that have been detected as changed (since last check) are fetched.
 */
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Value.Immutable(lazyhash = true)
public interface BillingRecord {
    /**
     * Usage date.
     *
     * @return Datetime for usage date.
     */
    LocalDateTime getSampleTime();

    /**
     * Target entity id, is non-0.
     *
     * @return Entity id.
     */
    long getEntityId();

    /**
     * Type of entity.
     *
     * @return Entity type.
     */
    int getEntityType();

    /**
     * Gets the price model type.
     *
     * @return Price model.
     */
    PriceModel getPriceModel();

    /**
     * Gets the cost category type.
     *
     * @return Cost category.
     */
    CostCategory getCostCategory();

    /**
     * Provider id, is non-0.
     *
     * @return Provider id.
     */
    long getProviderId();

    /**
     * Type of provider (e.g compute tier or storage tier).
     *
     * @return Provider type.
     */
    int getProviderType();

    /**
     * Type of commodity (e.g. storage amount or IOPS).
     *
     * @return commodity type
     */
    int getCommodityType();

    /**
     * For VM usage, this is how many hours in the day that usage was billed for. Not applicable
     * for volumes.
     *
     * @return Usage hours.
     */
    double getUsageAmount();

    /**
     * Cost that was billed for the given usage hours.
     *
     * @return Cost charged.
     */
    double getCost();

    /**
     * Time at which this billing cost record was last changed.
     *
     * @return Last changed timestamp, can be null.
     */
    @Nullable
    Long getLastUpdated();

    /**
     * Both entityId and providerId are present, and if either cost or usage are non-0, then we
     * have a 'valid' billing record. If both cost and usage are 0, we skip those records.
     * Check the provider type to make sure it matches valid provider types.
     *
     * @param supportedProviderTypes Interested provider types, e.g. compute, storage, DB/DBS.
     * @return True if valid.
     */
    @Derived
    default boolean isValid(@Nonnull final Set<Integer> supportedProviderTypes) {
        return getEntityId() != 0 && getProviderId() != 0
                && (Double.compare(getCost(), 0d) != 0
                || Double.compare(getUsageAmount(), 0d) != 0)
                && supportedProviderTypes.contains(getProviderType());
    }

    /**
     * Creates a new builder.
     */
    class Builder extends ImmutableBillingRecord.Builder {}
}
