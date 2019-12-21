package com.vmturbo.market.cloudscaling.sma.entities;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Stable Marriage algorithm enumerated type for Tenancy.
 */
public enum SMATenancy {
    /**
     * tenancy is unknown.
     */
    UNKNOWN,
    /**
     * default tenancy.
     */
    DEFAULT,
    /**
     * receive the benefits of having separated hosts from the rest of the AWS customers.
     */
    DEDICATED,
    /**
     * With a dedicated host, you purchase an entire physical host from AWS.
     */
    HOST;

    /**
     * Returns tenancy from the string name.
     *
     * @param enumName string name
     * @return enum value.  Return {@code UNKNOWN} if no one match {@code enumName}
     */
    @Nonnull
    public static SMATenancy getByName(@Nullable String enumName) {
        for (SMATenancy tenancy : SMATenancy.values()) {
            if (tenancy.name().equalsIgnoreCase(enumName)) {
                return tenancy;
            }
        }
        return UNKNOWN;
    }

}
