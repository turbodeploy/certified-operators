package com.vmturbo.cost.component.reserved.instance;

import javax.annotation.Nonnull;

/**
 * The class wrappers the service entity information which needs for {@link ReservedInstanceCoverageStore},
 * in order to store entity level reserved instance coverage data. When cost component
 * received the latest real time topology entity, the topology entity will be converted
 * to this {@link ServiceEntityReservedInstanceCoverage}, and then will be used by
 * {@link ReservedInstanceCoverageStore}.
 */
public class ServiceEntityReservedInstanceCoverage {

    private final long id;

    private final long regionId;

    private final long availabilityZoneId;

    private final long businessAccountId;

    private final double totalCoupons;

    private ServiceEntityReservedInstanceCoverage(final long id,
                                                  final long regionId,
                                                  final long availabilityZoneId,
                                                  final long businessAccountId,
                                                  final double totalCoupons) {
        this.id = id;
        this.regionId = regionId;
        this.availabilityZoneId = availabilityZoneId;
        this.businessAccountId = businessAccountId;
        this.totalCoupons = totalCoupons;
    }

    public long getId() {
        return this.id;
    }

    public long getRegionId() {
        return this.regionId;
    }

    public long getAvailabilityZoneId() {
        return this.availabilityZoneId;
    }

    public long getBusinessAccountId() {
        return this.businessAccountId;
    }

    public double getTotalCoupons() {
        return this.totalCoupons;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private long id;

        private long regionId;

        private long availabilityZoneId;

        private long businessAccountId;

        private double totalCoupons;

        private Builder() {}

        @Nonnull
        public ServiceEntityReservedInstanceCoverage build() {
            return new ServiceEntityReservedInstanceCoverage(id, regionId, availabilityZoneId,
                    businessAccountId, totalCoupons);
        }

        @Nonnull
        public Builder setId(final long id) {
            this.id = id;
            return this;
        }

        @Nonnull
        public Builder setRegionId(final long regionId) {
            this.regionId = regionId;
            return this;
        }

        @Nonnull
        public Builder setAvailabilityZoneId(final long availabilityZoneId) {
            this.availabilityZoneId = availabilityZoneId;
            return this;
        }

        @Nonnull
        public Builder setBusinessAccountId(final long businessAccountId) {
            this.businessAccountId = businessAccountId;
            return this;
        }

        @Nonnull
        public Builder setTotalCoupons(final long totalCoupons) {
            this.totalCoupons = totalCoupons;
            return this;
        }
    }
}
