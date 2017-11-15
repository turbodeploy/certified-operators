package com.vmturbo.market.component.api;

import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs;

import javax.annotation.Nonnull;

/**
 * Defines listener required to process the price index notifications.
 */
public interface PriceIndexListener {

    /**
     * Callback receiving the price index message.
     *
     * @param priceIndex The price index object.
     */
    void onPriceIndexReceived(final @Nonnull PriceIndexDTOs.PriceIndexMessage priceIndex);

}
