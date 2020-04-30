package com.vmturbo.api.component.external.api;

import static java.util.Arrays.asList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.saml2.credentials.Saml2X509Credential;
import org.springframework.security.saml2.provider.service.registration.InMemoryRelyingPartyRegistrationRepository;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistration;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistrationRepository;
import org.springframework.security.saml2.provider.service.registration.Saml2MessageBinding;
import org.springframework.security.saml2.provider.service.servlet.filter.Saml2WebSsoAuthenticationFilter;
import org.springframework.security.saml2.provider.service.servlet.filter.Saml2WebSsoAuthenticationRequestFilter;
import org.springframework.security.web.authentication.SavedRequestAwareAuthenticationSuccessHandler;

import com.vmturbo.api.component.communication.SamlAuthenticationProvider;
import com.vmturbo.api.component.security.CustomSaml2WebSsoAuthenticationRequestFilter;
import com.vmturbo.api.component.security.CustomSamlAuthenticationFilter;
import com.vmturbo.api.component.security.HeaderAuthenticationFilter;
import com.vmturbo.api.component.security.SamlAuthenticationCondition;

/**
 * <p>Spring security configuration for SAML.</p>
 */
@Configuration
@Conditional(SamlAuthenticationCondition.class)
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class SamlApiSecurityConfig extends ApiSecurityConfig {

    private static final Logger logger = LogManager.getLogger();

    private static final String SAML_REDIRECT_URL = "/app/index.html#/view/main/home/Market/hybrid";
    private static final String BASE_URL = "/vmturbo";
    private static final String LOGIN_PROCESSING_URL = BASE_URL + "/saml2/sso/{registrationId}";

    // SAML web SSO end point, e.g.:
    // https://dev-771202.oktapreview.com/app/ibmdev771202_turboxl1_1/exkfdsn6oy5xywqCO0h7/sso/saml
    @Value("${samlWebSsoEndpoint}")
    private String samlWebSsoEndpoint;

    // SAML IDP entity id, e.g.: http://www.okta.com/exkfdsn6oy5xywqCO0h7
    @Value("${samlEntityId}")
    private String samlEntityId;

    // SAML SP registration ID, e.g.: simplesamlphp
    @Value("${samlRegistrationId}")
    private String samlRegistrationId;

    // SAML SP restriction tag, e.g. turbo
    @Value("${samlSpEntityId}")
    private String samlSpEndityId;

    /**
     * SAML IDP certificate.
     * E.g.:
     * -----BEGIN CERTIFICATE-----\n" +
     * ...
     * "-----END CERTIFICATE-----
     */
    @Value("${samlIdpCertificate}")
    private String samlIdpCertificate;

    @Autowired(required = false)
    private SamlAuthenticationProvider samlAuthenticationProvider;

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        super.configure(http);

        http.saml2Login(saml2 -> saml2.relyingPartyRegistrationRepository(
                relyingPartyRegistrationRepository())
                .loginProcessingUrl(LOGIN_PROCESSING_URL) // IDP redirect URL
                .authenticationManager(new ProviderManager(asList(samlAuthenticationProvider)))
                .successHandler(successRedirectHandler()));

        // customize SAML authentication request to support adding base URL (/vmturbo).
        http.addFilterBefore(customSaml2WebSsoAuthenticationRequestFilter(),
                Saml2WebSsoAuthenticationRequestFilter.class);
        // customer SAML authentication to store IP address.
        http.addFilterBefore(customSamlAuthenticationFilter(),
                Saml2WebSsoAuthenticationFilter.class);
    }

    /**
     * Create customized SAML authentication filter to support base URL (/vmturbo).
     *
     * @return the new {@link HeaderAuthenticationFilter}
     */
    @Bean
    public CustomSamlAuthenticationFilter customSamlAuthenticationFilter() {
        return new CustomSamlAuthenticationFilter();
    }

    /**
     * Create customized SAML authentication filter to store IP address.
     *
     * @return the new {@link HeaderAuthenticationFilter}
     */
    @Bean
    public CustomSaml2WebSsoAuthenticationRequestFilter customSaml2WebSsoAuthenticationRequestFilter() {
        return new CustomSaml2WebSsoAuthenticationRequestFilter(
                relyingPartyRegistrationRepository());
    }

    /**
     * Register replying party. Currently only support one replying party, but can be extended to
     * support multiple replying parties.
     *
     * @return replying party repository.
     */
    @Bean
    public RelyingPartyRegistrationRepository relyingPartyRegistrationRepository() {
        //SAML configuration
        //Mapping this application to one or more Identity Providers
        return new InMemoryRelyingPartyRegistrationRepository(relyingPartyRegistration());
    }

    private RelyingPartyRegistration relyingPartyRegistration() {
        Saml2X509Credential signingCredential =
                Saml2X509CredentialsUtil.getRelyingPartyCredentials().get(0);
        //IDP certificate for verification of incoming messages
        Saml2X509Credential idpVerificationCertificate =
                Saml2X509CredentialsUtil.buildSaml2X509Credential(samlIdpCertificate);
        String acsUrlTemplate =
                "https://{baseHost}" + LOGIN_PROCESSING_URL;
        return RelyingPartyRegistration.withRegistrationId(samlRegistrationId)
                .providerDetails(c -> {
                    c.webSsoUrl(samlWebSsoEndpoint);
                    c.binding(Saml2MessageBinding.POST);
                    c.signAuthNRequest(true);
                    c.entityId(samlEntityId);
                })
                .credentials(c -> c.add(signingCredential))
                .credentials(c -> c.add(idpVerificationCertificate))
                .localEntityIdTemplate(samlSpEndityId)
                .assertionConsumerServiceUrlTemplate(acsUrlTemplate)
                .build();
    }

    /**
     * Handler deciding where to redirect user after successful login.
     *
     * @return the SavedRequestAwareAuthenticationSuccessHandler
     */
    @Bean
    public SavedRequestAwareAuthenticationSuccessHandler successRedirectHandler() {
        SavedRequestAwareAuthenticationSuccessHandler successRedirectHandler =
                new SavedRequestAwareAuthenticationSuccessHandler();
        successRedirectHandler.setDefaultTargetUrl(SAML_REDIRECT_URL);
        return successRedirectHandler;
    }
}