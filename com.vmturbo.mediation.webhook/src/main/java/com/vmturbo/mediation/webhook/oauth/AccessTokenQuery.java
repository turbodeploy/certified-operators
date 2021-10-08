package com.vmturbo.mediation.webhook.oauth;

import java.util.Collection;
import java.util.Collections;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.http.Header;

import com.vmturbo.mediation.connector.common.HttpMethodType;
import com.vmturbo.mediation.connector.common.ParametersBasedHttpBody;
import com.vmturbo.mediation.connector.common.http.AbstractHttpBodyAwareQuery;

/**
 * The query used to construct an oAuth request.
 */
public class AccessTokenQuery
        extends AbstractHttpBodyAwareQuery<AccessTokenResponse, ParametersBasedHttpBody> {

    /**
     * Creates the query to send to the oauth endpoint.
     *
     * @param httpMethodType type of method of the request, decided by the target
     *         credentials.
     * @param body the body of the request to send to the webhook endpoint.
     * @param headers the headers of the http request.
     */
    public AccessTokenQuery(@Nonnull HttpMethodType httpMethodType,
            @Nullable ParametersBasedHttpBody body, @Nonnull Collection<Header> headers) {
        super(httpMethodType, "", Collections.emptyMap(), body, headers, AccessTokenResponse.class);
    }
}

