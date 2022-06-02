package com.vmturbo.cost.component.savings.calculator;

import org.immutables.value.Value;

/**
 * A point on the savings graph that signifies the action chain is terminated.
 */
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Value.Immutable(lazyhash = true)
public interface ActionChainTermination extends ActionDataPoint {

    /**
     * Creates a new builder.
     */
    class Builder extends ImmutableActionChainTermination.Builder {}
}
