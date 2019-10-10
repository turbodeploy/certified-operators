package com.vmturbo.reserved.instance.coverage.allocator.utils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;

/**
 * A helper class for working with {@link ReservedInstanceBought} and {@link ReservedInstanceSpec}
 */
public class ReservedInstanceHelper {

    private ReservedInstanceHelper() {}

    /**
     * Determines whether an RI is expired, based on its start time, term, and the current time.
     *
     * @param reservedInstance An instance of {@link ReservedInstanceBought} to check
     * @param riSpec The {@link ReservedInstanceSpec} associated with {@code reservedInstance}
     * @return True, if the RI is expired. False otherwise
     */
    public static boolean isExpired(@Nonnull ReservedInstanceBought reservedInstance,
                                    @Nonnull ReservedInstanceSpec riSpec) {

        Objects.requireNonNull(reservedInstance);
        Objects.requireNonNull(riSpec);

        final ReservedInstanceBoughtInfo riInfo = reservedInstance.getReservedInstanceBoughtInfo();
        final ReservedInstanceSpecInfo riSpecInfo = riSpec.getReservedInstanceSpecInfo();

        final Instant reservedInstanceExpirationInstant = Instant
                .ofEpochMilli(riInfo.getStartTime())
                // AWS definition of a year: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-reserved-instances.html
                // Azure documentation is lacking. Therefore, we default to AWS definition
                .plus(riSpecInfo.getType().getTermYears() * 365L, ChronoUnit.DAYS);

        return Instant.now().isAfter(reservedInstanceExpirationInstant);
    }

    /**
     * Determines whether {@code riSpec} is instance size flexible.
     *
     * @param riSpec An instance of {@link ReservedInstanceSpec}
     * @return True, if {@code riSpec} is instance size flexible. False otherwise
     */
    public static boolean isSpecInstanceSizeFlexible(@Nonnull ReservedInstanceSpec riSpec) {
        return riSpec.getReservedInstanceSpecInfo().getSizeFlexible();
    }

    /**
     * Determines whether {@code riSpec} is platform size flexible.
     *
     * @param riSpec An instance of {@link ReservedInstanceSpec}
     * @return True, if {@code riSpec} is platform size flexible. False otherwise
     */
    public static boolean isSpecPlatformFlexible(@Nonnull ReservedInstanceSpec riSpec) {
        return riSpec.getReservedInstanceSpecInfo().getPlatformFlexible();
    }
}
