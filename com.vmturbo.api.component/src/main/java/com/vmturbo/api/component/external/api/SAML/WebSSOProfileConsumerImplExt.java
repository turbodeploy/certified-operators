package com.vmturbo.api.component.external.api.SAML;

import static com.vmturbo.api.component.external.api.util.ApiUtils.getClientIp;

import java.io.Serializable;

import javax.servlet.http.HttpServletRequest;

import org.opensaml.common.SAMLException;
import org.opensaml.ws.transport.InTransport;
import org.opensaml.ws.transport.http.HttpServletRequestAdapter;
import org.springframework.security.saml.context.SAMLMessageContext;
import org.springframework.security.saml.websso.WebSSOProfileConsumerImpl;

/**
 * Extends WebSSOProfileConsumerImpl to provide remote user request IP address information for auditing.
 */
public class WebSSOProfileConsumerImplExt extends WebSSOProfileConsumerImpl {

    public static final String EMPTY_STRING = "";

    /**
     * Process additional remote user request IP address from the SAML exchange.
     * The method is called once all the other processing was finished and incoming message is deemed as valid.
     *
     * @param context context containing incoming message
     * @return remote user IP address
     * @throws SAMLException in case processing fails
     */
    @Override
    protected Serializable processAdditionalData(SAMLMessageContext context) throws SAMLException {
        InTransport inTransport = context.getInboundMessageTransport();
        HttpServletRequest httpRequest = null;
        if (inTransport instanceof HttpServletRequestAdapter) {
            httpRequest = ((HttpServletRequestAdapter) inTransport).getWrappedRequest();
        }

        return httpRequest == null ? null : getClientIp(httpRequest).orElse(EMPTY_STRING);

    }
}