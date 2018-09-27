package com.vmturbo.cost.component.reserved.instance;

import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class used to compute coupons of a AWS ec2 instances based on the instance type name.
 * Please refer this page for the details about how AWS model NFU for reserved instance:
 * https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/apply_ri.html
 */
public class AwsReservedInstanceCoupon {
    private static final Logger logger = LogManager.getLogger();

    private static final Pattern nXlargePattern = Pattern.compile("(\\d+)xlarge");

    // a list of AWS instance type with the coupon number associated with.
    private enum InstanceSizeWithCoupon {
        UNKNOWN("unknown", 0),
        NANO("nano", 1),
        MICRO("micro", 2),
        SMALL("small", 4),
        MEDIUM("medium", 8),
        LARGE("large", 16),
        XLARGE("xlarge", 32);

        private final String instanceTypeGivenName;
        private final int numberOfCoupons;

        InstanceSizeWithCoupon(String instanceSize, Integer couponUnit) {
            this.instanceTypeGivenName = instanceSize;
            this.numberOfCoupons = couponUnit;
        }

        public String getInstanceTypeGivenName() {
            return instanceTypeGivenName;
        }

        public int getNumberOfCoupons() {
            return numberOfCoupons;
        }
    }

    /**
     * Convert instance type of the reserved instance to coupons.
     *
     * @param instanceType Instance type of the reserved instance, e.g. t2.micro, m4.large.
     * @return Normalized factor of the instance size, or -1 if unknown.
     */
    public static int convertInstanceTypeToCoupons(@Nullable String instanceType) {
        final Optional<String> sizePart = instanceTypeSize(instanceType);
        if (sizePart.isPresent()) {
            String sizeName = sizePart.get();
            Optional<InstanceSizeWithCoupon> size = Arrays.stream(InstanceSizeWithCoupon.values())
                    .filter(riCouponUnit -> sizeName.equalsIgnoreCase(
                            riCouponUnit.getInstanceTypeGivenName()))
                    .findFirst();

            if (size.isPresent()) {
                return size.get().getNumberOfCoupons();
            }

            // Look for a multiple of xlarge, eg 2xlarge. We assume that AWS will continue
            // to consistently name these so that Nxlarge is N times as many NFU as an xlarge.

            Matcher matcher = nXlargePattern.matcher(sizeName);
            if (matcher.matches()) {
                return Integer.valueOf(matcher.group(1)) * InstanceSizeWithCoupon.XLARGE.getNumberOfCoupons();
            }
        }

        logger.warn("Unknown coupon conversion for instance type '{}'", instanceType);

        return InstanceSizeWithCoupon.UNKNOWN.getNumberOfCoupons();
    }

    /**
     * Given an AWS instance type name, return the size portion.
     *
     * @param instanceType an instance type name, eg "t2.small"
     * @return the size portion of the name, eg "small", or null
     * if the type name cannot be parsed.
     */
    @Nonnull
    private static Optional<String> instanceTypeSize(@Nullable String instanceType) {
        if (instanceType == null) {
            return Optional.empty();
        }

        int dotIndex = instanceType.indexOf('.');
        if (dotIndex < 0) {
            return Optional.empty();
        }

        return Optional.of(instanceType.substring(dotIndex + 1));
    }
}
