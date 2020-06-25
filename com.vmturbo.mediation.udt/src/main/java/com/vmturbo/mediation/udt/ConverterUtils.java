package com.vmturbo.mediation.udt;

import static com.vmturbo.platform.common.builders.SDKConstants.ACCESS_COMMODITY_CAPACITY;
import static com.vmturbo.platform.common.builders.SDKConstants.ACCESS_COMMODITY_USED;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.vmturbo.platform.common.builders.CommodityBuilders;
import com.vmturbo.platform.common.builders.CommodityBuilders.ApplicationBought;
import com.vmturbo.platform.common.builders.CommodityBuilders.ApplicationSold;

/**
 * Utility class for converting purposes.
 */
class ConverterUtils {

    /**
     * Utility classes should not have a public or default constructor.
     */
    private ConverterUtils() {

    }

    @Nonnull
    @ParametersAreNonnullByDefault
    static String getApplicationCommodityKey(String providerId, String consumerId) {
        return String.format("ApplicationCommodity::%s::%s", providerId, consumerId);
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    static ApplicationSold createApplicationSold(String providerId, String consumerId) {
        return CommodityBuilders.application()
                .sold()
                .key(getApplicationCommodityKey(providerId, consumerId))
                .capacity(ACCESS_COMMODITY_CAPACITY);
    }

    @Nonnull
    @ParametersAreNonnullByDefault
    static ApplicationBought createApplicationBought(String providerId, String consumerId) {
        return CommodityBuilders.application()
                .from(providerId)
                .key(getApplicationCommodityKey(providerId, consumerId))
                .used(ACCESS_COMMODITY_USED);
    }
}
