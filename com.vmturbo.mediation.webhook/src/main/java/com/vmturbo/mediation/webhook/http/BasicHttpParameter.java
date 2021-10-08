package com.vmturbo.mediation.webhook.http;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.connector.common.http.HttpParameter;

/**
 * Class that represents a http header value.
 */
public class BasicHttpParameter implements HttpParameter {
    private final String parameter;

    /**
     * BasicHttpParameter constructor.
     *
     * @param parameter the string representing the header name.
     */
    public BasicHttpParameter(@Nonnull String parameter) {
        this.parameter = parameter;
    }

    @Nonnull
    @Override
    public String getParameterId() {
        return parameter;
    }
}
