package com.vmturbo.platform.analysis.economy;

/**
 * The different states a given trader can be in.
 *
 * <p>
 *  The states originate from our application domain of data center optimization and not from the
 *  market model.
 * </p>
 */
public enum TraderState {
    ACTIVE,
    SUSPENDED,
    IN_MAINTENANCE,
}
