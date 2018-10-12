package com.vmturbo.market.topology.conversions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

// dummy class used in test to represent the the cost library
// TODO: remove this class once RB-26121 is committed
public class CostLibrary {
    public ComputePriceBundle getComputePriceBundle(final long tierId, final long regionId) {
        // TODO: dummy data returned by cost library
        return new ComputePriceBundle(OSType.LINUX, new ArrayList<>());
    }
    /**
     * A bundle of of possible prices for a (compute tier, region) combination. The possible
     * prices are affected by the {@link OSType} and owning business account of the VM consuming
     * from the tier.
     */
    public static class ComputePriceBundle {

        private final OSType baseOsType;

        private final List<ComputePrice> prices;

        public ComputePriceBundle(@Nonnull final OSType osType,
                                   @Nonnull final List<ComputePrice> prices) {
            this.baseOsType = Objects.requireNonNull(osType);
            this.prices = Objects.requireNonNull(prices);
        }

        @Nonnull
        public OSType getBaseOsType() {
            return baseOsType;
        }

        @Nonnull
        public List<ComputePrice> getPrices() {
            return prices;
        }

        @Nonnull
        public static Builder newBuilder() {
            return new Builder();
        }

        public static class Builder {
            private final ImmutableList.Builder<ComputePrice> priceBuilder = ImmutableList.builder();

            private OSType baseOsType = OSType.LINUX;

            private Builder() {}

            @Nonnull
            public Builder setBaseOsType(@Nonnull final OSType osType) {
                this.baseOsType = osType;
                return this;
            }

            @Nonnull
            public Builder addPrice(final long accountId, final OSType osType, final double hourlyPrice) {
                // TODO (roman, September 25) - Replace with CostTuple
                priceBuilder.add(new ComputePrice(accountId, osType, hourlyPrice));
                return this;
            }

            @Nonnull
            public ComputePriceBundle build() {
                return new ComputePriceBundle(baseOsType, priceBuilder.build());
            }
        }

        // Temporary object acting as a placeholder.
        public static class ComputePrice {
            private final long accountId;
            private final OSType osType;
            private final double hourlyPrice;

            public ComputePrice(final long accountId,
                                final OSType osType,
                                final double hourlyPrice) {
                this.accountId = accountId;
                this.osType = osType;
                this.hourlyPrice = hourlyPrice;
            }

            public long getAccountId() {
                return accountId;
            }

            public OSType getOsType() {
                return osType;
            }

            public double getHourlyPrice() {
                return hourlyPrice;
            }
        }
    }
}
