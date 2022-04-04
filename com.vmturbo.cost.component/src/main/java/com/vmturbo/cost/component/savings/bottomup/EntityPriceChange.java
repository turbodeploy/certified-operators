package com.vmturbo.cost.component.savings.bottomup;

import org.immutables.gson.Gson;
import org.immutables.value.Value.Default;
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
public interface EntityPriceChange {
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
     * @return source oid.
     */
    @Default
    default Long getSourceOid() {
        return 0L;
    }

    /**
     * OID of target tier (post-action).
     *
     * @return destination oid.
     */
    @Default
    default Long getDestinationOid() {
        return 0L;
    }

    /**
     * Whether the price change (recommendation) is active and accumulating missed.
     *
     * @return whether the price change (recommendation) is active
     */
    @Default
    default boolean active() {
        return true;
    }

    /**
     * Creates a new builder.
     */
    class Builder extends ImmutableEntityPriceChange.Builder {}

    /**
     * Dummy price change used to populate the current recommendation field in the entity state for
     * entities that existed before action revert was implemented.  We now require that all entity
     * state instances contain a valid recommendation.
     */
    EntityPriceChange EMPTY = new EntityPriceChange.Builder()
            .active(false)
            .sourceOid(0L).destinationOid(0L)
            .sourceCost(0D).destinationCost(0D)
            .build();
}
