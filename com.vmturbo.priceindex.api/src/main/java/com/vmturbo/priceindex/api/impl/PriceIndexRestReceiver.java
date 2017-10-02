package com.vmturbo.priceindex.api.impl;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.ComponentRestClient;

import javax.annotation.Nonnull;

/**
 * The client that calls the PriceIndex REST methods
 * and interprets the responses.
 */
class PriceIndexRestReceiver extends ComponentRestClient {

    PriceIndexRestReceiver(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        super(connectionConfig);
    }
}
