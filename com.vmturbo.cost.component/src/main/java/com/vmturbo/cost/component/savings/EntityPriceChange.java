package com.vmturbo.cost.component.savings;

import java.util.Optional;

import org.immutables.gson.Gson;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

/**
 * Stores info about pricing changes.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Gson.TypeAdapters
@Immutable(lazyhash = true)
interface EntityPriceChange {
    /**
     * Pre-action cost. E.g on-demand compute cost for VM.
     *
     * @return Cost double.
     */
    double getSourceCost();

    /**
     * Target (post-action) cost. If unattached volume is deleted, this cost with be 0.
     *
     * @return Cost double.
     */
    double getDestinationCost();

    /**
     * Gets the difference in cost between destination and source.
     *
     * @return Positive for scale UP (Investment) actions, when source cost is less than destination.
     */
    @Derived
    default double getDelta() {
        return getDestinationCost() - getSourceCost();
    }

    /**
     * OID of source tier (pre-action) or entity.
     *
     * @return Optional source oid.
     */
    Optional<Long> getSourceOid();

    /**
     * OID of target tier (post-action).
     *
     * @return Optional destination oid.
     */
    Optional<Long> getDestinationOid();

    /**
     * Creates a new builder.
     */
    class Builder extends ImmutableEntityPriceChange.Builder {}
}
