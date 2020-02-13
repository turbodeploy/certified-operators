package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.immutables.value.Value;

/**
 * Represents a hashable key for the grouping of demand clusters by an account grouping. An account
 * grouping can be either a billing family or standalone account. Because the OIDs of an account and
 * billing family may overlap, the type of the grouping is also required to correctly group demand
 * clusters
 */
@Value.Immutable
public interface AccountGroupingIdentifier {

    /**
     * Represents either the standalone business account's OID or the billing family group ID.
     *
     * @return The ID of this account grouping.
     */
    long id();

    /**
     * The type of this account grouping. The type can either be a billing family grouping or a
     * standalone account.
     *
     * @return The account grouping type.
     */
    @Nonnull
    AccountGroupingType groupingType();

    enum AccountGroupingType {
        BILLING_FAMILY,
        STANDALONE_ACCOUNT
    }

    /**
     * A human-readable tag for this account grouping. This attribute is not used for hashing or
     * equality checks of the account grouping ID.
     *
     * @return The human-readable tag for this account grouping ID.
     */
    @Value.Default
    @Value.Auxiliary
    default String tag() {
        return StringUtils.EMPTY;
    }
}
