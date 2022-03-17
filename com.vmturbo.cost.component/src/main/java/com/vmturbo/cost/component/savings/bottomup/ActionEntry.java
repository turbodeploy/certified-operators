package com.vmturbo.cost.component.savings.bottomup;

import java.util.Map;

import javax.annotation.Nonnull;

import org.immutables.gson.Gson;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

/**
 * Stores information about executed actions.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Gson.TypeAdapters
@Immutable(lazyhash = true)
interface ActionEntry {
    /**
     * Event type.
     *
     * @return the event type.
     */
    ActionEvent.ActionEventType getEventType();

    /**
     * OID of source tier (pre-action) or entity.
     *
     * @return source oid.
     */
    Long getSourceOid();

    /**
     * OID of target tier (post-action).
     *
     * @return destination oid.
     */
    Long getDestinationOid();

    /**
     * Commodities affected by the action.
     *
     * @return Map from commodity type to usage for that commodity
     */
    Map<Integer, Double> getCommodityUsage();

    /**
     * Check whether the source and destination OIDs are the reverse of this one.
     *
     * @param eventType event type
     * @param destOid destination OID of the provider change
     * @return true if the eventType/destOid combination reverses this action entry
     */
    default boolean reverses(@Nonnull ActionEvent.ActionEventType eventType, long destOid) {
        return getEventType() == eventType && getSourceOid() == destOid;
    }

    /**
     * Check whether the source and destination OIDs are a duplicate of this one.
     *
     * @param eventType event type
     * @param destOid destination OID of the provider change
     * @return true if the eventType/destOid combination matches this action entry
     */
    default boolean duplicates(@Nonnull ActionEvent.ActionEventType eventType, long destOid) {
        return getEventType() == eventType && getDestinationOid() == destOid;
    }

    /**
     * Creates a new builder.
     */
    class Builder extends ImmutableActionEntry.Builder {}
}
