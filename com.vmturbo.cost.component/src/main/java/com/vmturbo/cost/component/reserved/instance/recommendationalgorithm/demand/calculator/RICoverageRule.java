package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.calculator;

import java.util.Optional;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * A coverage rule for applying RI coverage.
 */
@HiddenImmutableImplementation
@Immutable
public interface RICoverageRule {

    /**
     * Whether to use the shared (billing family) scope.
     * @return True, if the billing family scope should be use. False, if the account scope
     * should be used.
     */
    boolean sharedScope();

    /**
     * Whether use include the zone scope.
     * @return True, if the zone scope should be used. False otherwise.
     */
    boolean zoneScoped();

    /**
     * Indicates whether size flexibility should be factor into coverage assignments.
     * @return True, if to apply size flexible coverage rules. False, indicates size inflexible
     * matching should be applied. An empty option indicates both rules should be applied.
     */
    @Nonnull
    Optional<Boolean> sizeFlexible();

    /**
     * Indicates whether platform flexibility should be factor into coverage assignments.
     * @return True, if to apply platform flexible coverage rules. False, indicates platform inflexible
     * matching should be applied. An empty option indicates both rules should be applied.
     */
    @Nonnull
    Optional<Boolean> platformFlexible();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link RICoverageRule} instances.
     */
    class Builder extends ImmutableRICoverageRule.Builder {}
}
