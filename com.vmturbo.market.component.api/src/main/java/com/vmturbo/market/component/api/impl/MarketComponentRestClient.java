package com.vmturbo.market.component.api.impl;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.ComponentRestClient;

/**
 * The client that calls the Market Component's REST methods
 * and interprets the responses.
 */
class MarketComponentRestClient extends ComponentRestClient {

    MarketComponentRestClient(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        super(connectionConfig);
    }
}
