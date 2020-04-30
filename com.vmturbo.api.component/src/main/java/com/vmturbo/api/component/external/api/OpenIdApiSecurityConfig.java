package com.vmturbo.api.component.external.api;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.oauth2.client.CommonOAuth2Provider;
import org.springframework.security.oauth2.client.InMemoryOAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.registration.InMemoryClientRegistrationRepository;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizationRequestRedirectFilter;
import org.springframework.security.oauth2.core.AuthenticationMethod;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.ClientAuthenticationMethod;
import org.springframework.security.oauth2.core.oidc.IdTokenClaimNames;
import org.springframework.web.filter.ForwardedHeaderFilter;

import com.vmturbo.api.component.security.OpenIdAuthenticationCondition;

/**
 * <p>Spring security configuration for OpenID.</p>
 */
@Configuration
@Conditional(OpenIdAuthenticationCondition.class)
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class OpenIdApiSecurityConfig extends ApiSecurityConfig {

    private static final Logger logger = LogManager.getLogger();

    private static final String OAUTH2_REDIRECT_URL = "/app/index.html#/view/main/home/Market/hybrid";
    private static final String BASE_URL = "/vmturbo";
    private static final String LOGIN_URL = BASE_URL + "/oauth2/login/code/*";
    private static final String AUTHORIZATION_URL = BASE_URL + "/oauth2/authorization";
    private static final String DEFAULT_REDIRECT_URL = "{baseUrl}" + BASE_URL + "/oauth2/{action}/code/{registrationId}";

    @Value("${openIdEnabled:false}")
    private boolean openIdEnabled;
    // Specify a list of OpenID clients such as “google”, “okta”, “ibm”:
    @Value("${openIdClients}")
    private String[] openIdClients;
    // OpenID clientAuthentication such as “basic”, “post”, “none”:
    @Value("${openIdClientAuthentication:basic}")
    private String openIdClientAuthentication;
    // OpenID userAuthentication such as “header”, “form”, “query”:
    @Value("${openIdUserAuthentication:header}")
    private String openIdUserAuthentication;
    @Value("${openIdAccessTokenUri:https://www.googleapis.com/oauth2/v4/token}")
    private String openIdAccessTokenUri;
    @Value("${openIdUserAuthorizationUri:https://accounts.google.com/o/oauth2/v2/auth}")
    private String openIdUserAuthorizationUri;
    @Value("${openIdClientId}")
    private String openIdClientId;
    @Value("${openIdClientSecret}")
    private String openIdClientSecret;
    @Value("${openIdClientScope:openid, profile, email}")
    private String[] openIdClientScope;
    @Value("${openIdUserInfoUri:https://www.googleapis.com/oauth2/v3/userinfo}")
    private String openIdUserInfoUri;
    @Value("${openIdJwkSetUri:https://www.googleapis.com/oauth2/v3/certs}")
    private String openIdJwkSetUri;

    @Bean
    ForwardedHeaderFilter forwardedHeaderFilter() {
        return new ForwardedHeaderFilter();
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        super.configure(http);
        http.oauth2Login()
            .loginProcessingUrl(LOGIN_URL)
            .redirectionEndpoint().baseUri(LOGIN_URL).and()
            .authorizationEndpoint().baseUri(AUTHORIZATION_URL).and()
            .clientRegistrationRepository(clientRegistrationRepository())
            .authorizedClientService(authorizedClientService())
            .successHandler(successRedirectHandler());
        // customize OpenID redirect request to support reverse proxy.
        http.addFilterBefore(forwardedHeaderFilter(), OAuth2AuthorizationRequestRedirectFilter.class);
    }

    /**
     * Handler deciding where to redirect user after successful login.
     *
     * @return the SavedRequestAwareAuthenticationSuccessHandler
     */
    public CustomRequestAwareAuthenticationSuccessHandler successRedirectHandler() {
      customRequestAwareAuthenticationSuccessHandler.setDefaultTargetUrl(OAUTH2_REDIRECT_URL);
        return customRequestAwareAuthenticationSuccessHandler;
    }

    @Autowired
    private CustomRequestAwareAuthenticationSuccessHandler customRequestAwareAuthenticationSuccessHandler;

    /**
     * Create a new ClientService.
     *
     * @return the OAuth2AuthorizedClientService
     */
    @Bean
    public OAuth2AuthorizedClientService authorizedClientService() {
        return new InMemoryOAuth2AuthorizedClientService(
                clientRegistrationRepository());
    }

    /**
     * Create a new ClientRegistrationRepository.
     *
     * @return the ClientRegistrationRepository
     */
    @Bean
    public ClientRegistrationRepository clientRegistrationRepository() {
        List<ClientRegistration> registrations = Lists.newArrayList(openIdClients).stream()
                .map(c -> getRegistration(c))
                .filter(registration -> registration != null)
                .collect(Collectors.toList());

        return new InMemoryClientRegistrationRepository(registrations);
    }

    /**
     * Create a client registration.
     *
     * @param client the client registration name
     * @return the ClientRegistration
     */
    private ClientRegistration getRegistration(String client) {
        if (client.equals("google")) {
            return CommonOAuth2Provider.GOOGLE.getBuilder(client)
                .redirectUriTemplate(DEFAULT_REDIRECT_URL)
                .clientId(openIdClientId).clientSecret(openIdClientSecret).build();
        } else if (client.equals("github")) {
            return CommonOAuth2Provider.GITHUB.getBuilder(client)
                .redirectUriTemplate(DEFAULT_REDIRECT_URL)
                .clientId(openIdClientId).clientSecret(openIdClientSecret).build();
        } else if (client.equals("facebook")) {
            return CommonOAuth2Provider.FACEBOOK.getBuilder(client)
                .redirectUriTemplate(DEFAULT_REDIRECT_URL)
                .clientId(openIdClientId).clientSecret(openIdClientSecret).build();
        } else if (client.equals("okta")) {
            return CommonOAuth2Provider.OKTA.getBuilder(client)
                .tokenUri(openIdAccessTokenUri)
                .authorizationUri(openIdUserAuthorizationUri)
                .userInfoUri(openIdUserInfoUri)
                .jwkSetUri(openIdJwkSetUri)
                .redirectUriTemplate(DEFAULT_REDIRECT_URL)
                .clientId(openIdClientId).clientSecret(openIdClientSecret).build();
        } else {
            return getBuilder(client)
                .clientAuthenticationMethod(new ClientAuthenticationMethod(openIdClientAuthentication))
                .userInfoAuthenticationMethod(new AuthenticationMethod(openIdUserAuthentication))
                .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
                .scope(openIdClientScope)
                .userNameAttributeName(IdTokenClaimNames.SUB)
                .clientName(client)
                .tokenUri(openIdAccessTokenUri)
                .authorizationUri(openIdUserAuthorizationUri)
                .userInfoUri(openIdUserInfoUri)
                .jwkSetUri(openIdJwkSetUri)
                .redirectUriTemplate(DEFAULT_REDIRECT_URL)
                .clientId(openIdClientId).clientSecret(openIdClientSecret).build();
        }
    }

    /**
     * Create a custom client registration builder.
     *
     * @param registrationId the client registration id
     * @return the ClientRegistration
     */
    public final ClientRegistration.Builder getBuilder(String registrationId) {
        ClientRegistration.Builder builder = ClientRegistration.withRegistrationId(registrationId);
        return builder;
    }
}