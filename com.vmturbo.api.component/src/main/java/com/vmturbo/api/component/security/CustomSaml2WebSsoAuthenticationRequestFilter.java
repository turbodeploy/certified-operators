package com.vmturbo.api.component.security;

import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistrationRepository;
import org.springframework.security.saml2.provider.service.servlet.filter.Saml2WebSsoAuthenticationRequestFilter;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

/**
 * Custom SAML authentication filter to support base URL (/vmturbo).
 */
public class CustomSaml2WebSsoAuthenticationRequestFilter extends
        Saml2WebSsoAuthenticationRequestFilter {

    private static final String BASE_URL = "/vmturbo";
    private static final String PATTERN = BASE_URL + "/saml2/authenticate/{registrationId}";

    /**
     * Constructor.
     *
     * @param relyingPartyRegistrationRepository replying party registration repository.
     */
    public CustomSaml2WebSsoAuthenticationRequestFilter(
            RelyingPartyRegistrationRepository relyingPartyRegistrationRepository) {
        super(relyingPartyRegistrationRepository);
        super.setRedirectMatcher(new AntPathRequestMatcher(PATTERN));
    }
}