package com.vmturbo.market.component.api.impl;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.market.component.api.MarketComponent;

/**
 * Exception with the internal functioning of the {@link MarketComponent}.
 */
public class MarketComponentException extends ApiClientException {

    public MarketComponentException(@Nonnull final String message) {
        super(Objects.requireNonNull(message));
    }

    public MarketComponentException(@Nonnull final String message,
                                    @Nonnull final Throwable cause) {
        super(Objects.requireNonNull(message), Objects.requireNonNull(cause));
    }
}
