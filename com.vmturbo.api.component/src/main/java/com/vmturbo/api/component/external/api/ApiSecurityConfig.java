package com.vmturbo.api.component.external.api;

import static com.vmturbo.api.component.external.api.service.AuthenticationService.LOGIN_MANAGER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.xml.parsers.ParserConfigurationException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.app.VelocityEngine;
import org.opensaml.saml2.metadata.provider.DOMMetadataProvider;
import org.opensaml.saml2.metadata.provider.MetadataProvider;
import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.opensaml.xml.parse.StaticBasicParserPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.MethodInvokingFactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.security.core.session.SessionRegistryImpl;
import org.springframework.security.saml.SAMLAuthenticationProvider;
import org.springframework.security.saml.SAMLBootstrap;
import org.springframework.security.saml.SAMLEntryPoint;
import org.springframework.security.saml.SAMLLogoutFilter;
import org.springframework.security.saml.SAMLLogoutProcessingFilter;
import org.springframework.security.saml.SAMLProcessingFilter;
import org.springframework.security.saml.SAMLWebSSOHoKProcessingFilter;
import org.springframework.security.saml.context.SAMLContextProvider;
import org.springframework.security.saml.context.SAMLContextProviderLB;
import org.springframework.security.saml.key.JKSKeyManager;
import org.springframework.security.saml.key.KeyManager;
import org.springframework.security.saml.log.SAMLDefaultLogger;
import org.springframework.security.saml.metadata.CachingMetadataManager;
import org.springframework.security.saml.metadata.ExtendedMetadata;
import org.springframework.security.saml.metadata.ExtendedMetadataDelegate;
import org.springframework.security.saml.metadata.MetadataDisplayFilter;
import org.springframework.security.saml.metadata.MetadataGenerator;
import org.springframework.security.saml.metadata.MetadataGeneratorFilter;
import org.springframework.security.saml.parser.ParserPoolHolder;
import org.springframework.security.saml.processor.HTTPPAOS11Binding;
import org.springframework.security.saml.processor.HTTPPostBinding;
import org.springframework.security.saml.processor.HTTPRedirectDeflateBinding;
import org.springframework.security.saml.processor.HTTPSOAP11Binding;
import org.springframework.security.saml.processor.SAMLBinding;
import org.springframework.security.saml.processor.SAMLProcessorImpl;
import org.springframework.security.saml.trust.httpclient.TLSProtocolConfigurer;
import org.springframework.security.saml.trust.httpclient.TLSProtocolSocketFactory;
import org.springframework.security.saml.userdetails.SAMLUserDetailsService;
import org.springframework.security.saml.util.VelocityFactory;
import org.springframework.security.saml.websso.SingleLogoutProfile;
import org.springframework.security.saml.websso.SingleLogoutProfileImpl;
import org.springframework.security.saml.websso.WebSSOProfile;
import org.springframework.security.saml.websso.WebSSOProfileConsumer;
import org.springframework.security.saml.websso.WebSSOProfileConsumerHoKImpl;
import org.springframework.security.saml.websso.WebSSOProfileECPImpl;
import org.springframework.security.saml.websso.WebSSOProfileImpl;
import org.springframework.security.saml.websso.WebSSOProfileOptions;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.DefaultSecurityFilterChain;
import org.springframework.security.web.FilterChainProxy;
import org.springframework.security.web.access.channel.ChannelProcessingFilter;
import org.springframework.security.web.authentication.SavedRequestAwareAuthenticationSuccessHandler;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler;
import org.springframework.security.web.authentication.logout.LogoutHandler;
import org.springframework.security.web.authentication.logout.SecurityContextLogoutHandler;
import org.springframework.security.web.authentication.logout.SimpleUrlLogoutSuccessHandler;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.session.HttpSessionEventPublisher;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.web.filter.GenericFilterBean;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import com.vmturbo.api.component.external.api.SAML.SAMLCondition;
import com.vmturbo.api.component.external.api.SAML.SAMLUtils;
import com.vmturbo.api.component.external.api.SAML.WebSSOProfileConsumerImplExt;
import com.vmturbo.api.component.security.UserPolicyCondition;
import com.vmturbo.api.component.security.FailedAuthRequestAuditor;
import com.vmturbo.auth.api.Base64CodecUtils;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLog;

/**
 * Configure security for the REST API Dispatcher here.
 * The BASE_URI are defined in {@link ExternalApiConfig#BASE_URL_MAPPINGS}.
 *
 * <p>The legacy API - BASE_URI/api/** - requires no permissions (OM-22367).</p>
 * Permissions for the "new API":
 * <ul>
 * <li>BASE_URI/api/** - legacy API - no authentication.
 * <li>BASE_URI/login/** - only "anonymous" requests are allowed.
 * I.e. the "/login" request won't be honored if the user is already logged in
 * <li>BASE_URI/** - other requests require ".authenticated()".
 * </ul>
 * Following requests are used in UI login page before authentication.
 * To fix the access deny error messages, they are allowed to be accessed by unauthenticated requests.
 * We should revisit to make sure they have valid business uses
 * and are NOT exposing sensitive data (OM-22369).
 * <ul>
 * <li>BASE_URI/checkInit - checks whether the admin has been initialized
 * <li>BASE_URI/cluster/isXLEnabled - check whether XL is enable
 * <li>BASE_URI/cluster/proactive/initialized - check whether proactive is initialized
 * <li>BASE_URI/cluster/proactive/enabled - check whether proactive is enabled
 * <li>BASE_URI/users/me - get the current User
 * <li>BASE_URI/admin/versions - get current product version info and available updates
 * <li>BASE_URI/license - get license
 * <li>BASE_URI/initAdmin - set initial administrator password (will fail if call more than one time)
 * </ul>
 */
@Configuration
@Conditional(UserPolicyCondition.class)
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class ApiSecurityConfig extends WebSecurityConfigurerAdapter {
    private static final Logger logger = LogManager.getLogger();

    private static final String TURBO = "turbo";
    private static final String HTTPS_SLASH = "https://";
    private static final String VMTURBO = "/vmturbo";
    private static final String DEV_ENTITY_BASE_URL = "https://localhost/vmturbo";
    private static final String LOCALHOST = "localhost";
    private static final int DEFAULT_PORT = 443;
    private static final String HTTPS = "https";
    private static final String SAML_REDIRECT_URL = "/app/index.html#/view/main/home/Market/hybrid";

    // mapping to remove the trailing character, so /vmturbo/rest/* -> /vmturbo/rest/
    private static ImmutableList<String> baseURIs =
            ImmutableList.copyOf(ExternalApiConfig.BASE_URL_MAPPINGS
                .stream()
                .map(StringUtils::chop)
                .collect(Collectors.toList()));

    @Autowired
    ApplicationContext applicationContext;

    @Autowired(required = false)
    private SAMLUserDetailsService samlUserDetailsService;

    @Value("${samlEnabled:false}")
    private boolean samlEnabled;

    @Value("${samlIdpMetadata:samlIdpMetadata}")
    private String samlIdpMetadata;

    @Value("${samlEntityId:turbo}")
    private String samlEntityId;

    @Value("${samlExternalIP:localhost}")
    private String samlExternalIP;

    @Value("${samlKeystore:samlKeystore}")
    private String samlKeystore;

    @Value("${samlKeystorePassword:nalle123}")
    private String samlKeystorePassword;

    @Value("${samlPrivateKeyAlias:apollo}")
    private String samlPrivateKeyAlias;

    /**
     * Initialization of OpenSAML library.
     *
     * @return new SAMLBootstrap bean
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public static SAMLBootstrap sAMLBootstrap() {
        return new SAMLBootstrap();
    }

    /**
     * Add a custom authentication exception entry point to replace the default handler.
     * The reason is, for example, if there are some request without login authenticated, the default
     * handler "Http403ForbiddenEntryPoint" will send 403 Forbidden response, but the right
     * response should be 401 Unauthorized.
     *
     * @return the new default authentication exception handler
     */
    @Bean
    public AuthenticationEntryPoint restAuthenticationEntryPoint() {
        return new RestAuthenticationEntryPoint();
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable();
        http.headers().frameOptions().disable();

        // SAML flag
        if (samlEnabled) {
            http.httpBasic()
                    .authenticationEntryPoint(samlEntryPoint());
            http.addFilterBefore(metadataGeneratorFilter(), ChannelProcessingFilter.class)
                    .addFilterAfter(samlFilter(), BasicAuthenticationFilter.class);
            logger.info("System enabled SAML authentication and authorization.");
            AuditLog.newEntry(AuditAction.SET_DEFAULT_AUTH,
                    "Enabled SAML authentication and authorization.", true)
                    .targetName(LOGIN_MANAGER)
                    .audit();
        }
        // Add a custom authentication exception entry point to replace the default handler.
        // The reason is, for example, if there are some request without login authenticated, the default
        // handler "Http403ForbiddenEntryPoint" will send 403 Forbidden response, but the right
        // response should be 401 Unauthorized.
        http.exceptionHandling().authenticationEntryPoint(restAuthenticationEntryPoint());
        // Preventing session fixation is implemented in AuthenticationService::login().
        // Not here with Java configuration, because:
        // 1. login/logout is not configured in the standard way (E.g. by setting the formLogin, loginPage),
        // so changeSessionId does't work.
        // 2. Minimize the impact on the session fixation fix. Since not all REST APIs are secured,
        // by enabling sessionManagement policy here, the secure cookie could be overwritten by insecure cookie
        // and cause HTTP 403 error.
        // http.sessionManagement().sessionCreationPolicy(SessionCreationPolicy.IF_REQUIRED);
        // http.sessionManagement().sessionFixation().changeSessionId();
        for (String base_uri : baseURIs) {
            http.authorizeRequests()
                    .antMatchers(base_uri + "api/**").permitAll()
                    .antMatchers(base_uri + "login").permitAll()
                    .antMatchers(base_uri + "logout").permitAll()
                    .antMatchers(base_uri + "checkInit").permitAll()
                    .antMatchers(base_uri + "cluster/isXLEnabled").permitAll()
                    .antMatchers(base_uri + "cluster/proactive/initialized").permitAll()
                    .antMatchers(base_uri + "cluster/proactive/enabled").permitAll()
                    .antMatchers(base_uri + "users/me").permitAll()
                    .antMatchers(base_uri + "users/saml").permitAll()
                    .antMatchers(base_uri + "admin/versions").permitAll()
                    .antMatchers(base_uri + "admin/productcapabilities").permitAll()
                    .antMatchers(base_uri + "license").permitAll()
                    .antMatchers(base_uri + "initAdmin").permitAll()
                    .antMatchers(base_uri + "saml/**").permitAll()
                    .antMatchers(base_uri + "**").authenticated();
        }

        // enable session management to keep active session in SessionRegistry,
        // also allow multiple sessions for same user (-1).
        http.sessionManagement().maximumSessions(-1).sessionRegistry(sessionRegistry());
    }

    /**
     * Sets a custom authentication provider.
     *
     * @param auth SecurityBuilder used to create an AuthenticationManager.
     */
    @Override
    protected void configure(AuthenticationManagerBuilder auth) {
        if (samlEnabled) {
            auth.authenticationProvider(samlAuthenticationProvider());
        }
    }

    /**
     * Initialization of the Velocity templating engine used in the communication underlying
     * the SAML message exchange protocol.
     *
     * @return the Velocity templating engine instance
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public VelocityEngine velocityEngine() {
        return VelocityFactory.getEngine();
    }

    /**
     * JAXP XML parser pool needed for OpenSAML parsing.
     *
     * @return a new parser pool
     */
    @Bean(initMethod = "initialize")
    @Conditional(SAMLCondition.class)
    public StaticBasicParserPool parserPool() {
        return new StaticBasicParserPool();
    }

    /**
     * Holder for the JAXP XML parser pool above.
     *
     * @return a new ParserPoolHolder
     */
    @Bean(name = "parserPoolHolder")
    @Conditional(SAMLCondition.class)
    public ParserPoolHolder parserPoolHolder() {
        return new ParserPoolHolder();
    }

    /**
     * SAML Authentication Provider responsible for validating of received SAML messages.
     *
     * @return a new SAMLAuthenticationProvider bean
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLAuthenticationProvider samlAuthenticationProvider() {
        SAMLAuthenticationProvider samlAuthenticationProvider = new SAMLAuthenticationProvider();
        samlAuthenticationProvider.setUserDetails(samlUserDetailsService);
        samlAuthenticationProvider.setForcePrincipalAsString(false);
        return samlAuthenticationProvider;
    }

    /**
     * Provider of default SAML Context. The scheme is set to HTTPS. The external (landing) IP
     * is taken from the 'samlExternalIP' configuration property, and if not specified defaults
     * to 'localhost. The default port is set = 443.
     *
     * @return the new SAMLContextProvider bean
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLContextProvider contextProvider() {
        SAMLContextProviderLB samlContextProviderLB = new SAMLContextProviderLB();
        samlContextProviderLB.setScheme(HTTPS);
        if (samlExternalIP != null) {
            samlContextProviderLB.setServerName(samlExternalIP);
        } else {
            samlContextProviderLB.setServerName(LOCALHOST);
        }
        samlContextProviderLB.setServerPort(DEFAULT_PORT);
        samlContextProviderLB.setIncludeServerPortInRequestURL(false);
        samlContextProviderLB.setContextPath("/");
        return samlContextProviderLB;
    }

    /**
     * Logger for SAML messages and events. The default sends SAML progress messages to log4j.
     *
     * @return a SAMLDefaultLogger that uses log4j
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLDefaultLogger samlLogger() {
        return new SAMLDefaultLogger();
    }

    /**
     * Create a SAML 2.0 WebSSO Assertion parser / consumer.
     *
     * @return a new WebSSOProfileConsumer
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public WebSSOProfileConsumer webSSOprofileConsumer() {
        return new WebSSOProfileConsumerImplExt();
    }

    /**
     * Create a SAML 2.0 Holder-of-Key WebSSO Assertion Consumer to
     * process of the SAML Holder-of-Key Browser SSO profile.
     *
     * @return a new instance of WebSSOProfileConsumer
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public WebSSOProfileConsumer hokWebSSOprofileConsumer() {
        return new WebSSOProfileConsumerHoKImpl();
    }

    /**
     * Create a SAML 2.0 Web SSO profile bean.
     *
     * @return a new WebSSOProfile
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public WebSSOProfile webSSOprofile() {
        return new WebSSOProfileImpl();
    }

    /**
     * SAML 2.0 Holder-of-Key Web SSO profile.
     *
     * @return a new instance of WebSSOProfileConsumer
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public WebSSOProfileConsumer hokWebSSOProfile() {
        return new WebSSOProfileConsumerHoKImpl();
    }

    /**
     * Create a new SAML 2.0 ECP profile to handle SSH handshake.
     *
     * @return a new WebSSOProfile
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public WebSSOProfile ecpprofile() {
        return new WebSSOProfileECPImpl();
    }

    /**
     * Create a handler for SingleLogout protocol.
     *
     * @return a new SingleLogoutProfile handler
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public SingleLogoutProfile logoutprofile() {
        return new SingleLogoutProfileImpl();
    }

    /**
     * Create a Central storage of cryptographic keys.
     *
     * @return a new KeyManager to store the SAML keys
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public KeyManager keyManager() {
        byte[] byteArray = Base64CodecUtils.decode(samlKeystore);
        Resource storeFile = new ByteArrayResource(byteArray);
        Map<String, String> passwords = new HashMap<>();
        passwords.put(samlPrivateKeyAlias, samlKeystorePassword);
        return new JKSKeyManager(storeFile, samlKeystorePassword, passwords, samlPrivateKeyAlias);
    }

    /**
     * Configure the TLS Protocol for secure communication.
     *
     * @return a new TLSProtocolConfigurer to handle secure communication
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public TLSProtocolConfigurer tlsProtocolConfigurer() {
        return new TLSProtocolConfigurer();
    }

    /**
     * Configure the ProtocolSocketFactory for TLS connections to the IDP.
     *
     * @return a new ProtocolSocketFactory bean
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public ProtocolSocketFactory socketFactory() {
        return new TLSProtocolSocketFactory(keyManager(), null, "default");
    }

    /**
     * Create a bean for the Socket Factory Protocol - HTTPS, using the default TLS port (443).
     *
     * @return the newly created Protocol
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public Protocol socketFactoryProtocol() {
        return new Protocol(HTTPS, socketFactory(), DEFAULT_PORT);
    }

    /**
     * Create a bean to register the protocol above. This requires a call to the static
     * method {@link Protocol#registerProtocol}, hence the MethodInvokingFactoryBean.
     *
     * @return the bean to register the desired protocol
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public MethodInvokingFactoryBean socketFactoryInitialization() {
        MethodInvokingFactoryBean methodInvokingFactoryBean = new MethodInvokingFactoryBean();
        methodInvokingFactoryBean.setTargetClass(Protocol.class);
        methodInvokingFactoryBean.setTargetMethod("registerProtocol");
        Object[] args = {HTTPS, socketFactoryProtocol()};
        methodInvokingFactoryBean.setArguments(args);
        return methodInvokingFactoryBean;
    }

    /**
     * Create an WebSSOProfileOptions object that excludes the scoping from the SSO
     * request sent to the IDP.
     *
     * @return the new WebSSOProfileOptions bean
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public WebSSOProfileOptions defaultWebSSOProfileOptions() {
        WebSSOProfileOptions webSSOProfileOptions = new WebSSOProfileOptions();
        webSSOProfileOptions.setIncludeScoping(false);
        return webSSOProfileOptions;
    }

    /**
     * Entry point to initialize authentication, with the default values taken from
     * the configuration properties.
     *
     * @return the SAMLEntryPoint to represent this authentication process
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLEntryPoint samlEntryPoint() {
        SAMLEntryPoint samlEntryPoint = new SAMLEntryPoint();
        samlEntryPoint.setDefaultProfileOptions(defaultWebSSOProfileOptions());
        return samlEntryPoint;
    }

    /**
     * Setup advanced info about the metadata.
     *
     * @return an ExtendedMetadata bean
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public ExtendedMetadata extendedMetadata() {
        ExtendedMetadata extendedMetadata = new ExtendedMetadata();
        extendedMetadata.setIdpDiscoveryEnabled(false);
        extendedMetadata.setSignMetadata(false);
        extendedMetadata.setEcpEnabled(true);
        return extendedMetadata;
    }

    /**
     * Create an ExtendedMetadataProvider description specific to Okta.
     *
     * @return an ExtendedMetadataProvider specific to okta.
     * @throws ParserConfigurationException if there's configuring the parser for the XML
     * @throws IOException if there's an error reading the XML
     * @throws SAXException if there's an error parsing the XML
     */
    @Bean
    @Conditional(SAMLCondition.class)
    @Qualifier("okta")
    public ExtendedMetadataDelegate oktaExtendedMetadataProvider()
            throws ParserConfigurationException, SAXException, IOException {
        Element element = SAMLUtils.loadXMLFromString(SAMLUtils.SAMPLEIDPMETADA)
            .getDocumentElement();
        return new ExtendedMetadataDelegate(new DOMMetadataProvider(element),
            new ExtendedMetadata());
    }

    /**
     * IDP Metadata configuration specific to Okta. Paths to metadata of IDPs in
     * circle of trust are here. Do no forget to call initialize method on providers
     *
     * @return IDP Metadata
     * @throws MetadataProviderException if there is an error creating the CachingMetadataProvider
     * @throws IOException if there's an error fetching the metadata
     * @throws SAXException if there's an error parsing the metadata
     * @throws ParserConfigurationException if there's an error configuring the metadata parser
     */
    @Bean
    @Conditional(SAMLCondition.class)
    @Qualifier("metadata")
    public CachingMetadataManager metadata() throws MetadataProviderException, IOException,
        SAXException, ParserConfigurationException {
        List<MetadataProvider> providers = new ArrayList<>();
        if (samlIdpMetadata != null ) {
            DOMMetadataProvider provider = getDomMetadataProvider();
            ExtendedMetadataDelegate delegate = new ExtendedMetadataDelegate(provider, new ExtendedMetadata());
            delegate.setMetadataTrustCheck(false);
            providers.add(delegate);
        } else { //default provider for development
            providers.add(oktaExtendedMetadataProvider());
        }
        return new CachingMetadataManager(providers);
    }

    private DOMMetadataProvider getDomMetadataProvider() throws IOException, SAXException, ParserConfigurationException {
        byte[] byteArray = Base64CodecUtils.decode(samlIdpMetadata);
        String idpMetadata = new String(byteArray);
        Element element = SAMLUtils.loadXMLFromString(idpMetadata).getDocumentElement();
        return new DOMMetadataProvider(element);
    }

    /**
     * Generates service provide metadata.
     *
     * @return new MetadataGenerator based on the configuration properties
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public MetadataGenerator metadataGenerator() {
        MetadataGenerator metadataGenerator = new MetadataGenerator();
        if (samlEntityId != null) {
            metadataGenerator.setEntityId(samlEntityId);
        } else { // It's empty in Consul
            metadataGenerator.setEntityId(TURBO);
        }
        if (samlExternalIP != null) {
            metadataGenerator.setEntityBaseURL(HTTPS_SLASH + samlExternalIP + VMTURBO);
        } else { // It's empty in Consul
            metadataGenerator.setEntityBaseURL(DEV_ENTITY_BASE_URL);
        }
        metadataGenerator.setExtendedMetadata(extendedMetadata());
        metadataGenerator.setIncludeDiscoveryExtension(false);
        metadataGenerator.setKeyManager(keyManager());
        return metadataGenerator;
    }

    /**
     * The filter is waiting for connections on URL suffixed with filterSuffix
     * and presents SP metadata there.
     *
     * @return the new MetadataDisplayFilter
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public MetadataDisplayFilter metadataDisplayFilter() {
        return new MetadataDisplayFilter();
    }

    /**
     * Handler deciding where to redirect user after successful login.
     *
     * @return the SavedRequestAwareAuthenticationSuccessHandler
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public SavedRequestAwareAuthenticationSuccessHandler successRedirectHandler() {
        SavedRequestAwareAuthenticationSuccessHandler successRedirectHandler =
                new SavedRequestAwareAuthenticationSuccessHandler();
        successRedirectHandler.setDefaultTargetUrl(SAML_REDIRECT_URL);
        return successRedirectHandler;
    }

    /**
     * Handler deciding where to redirect user after failed login.
     *
     * @return the SimpleUrlAuthenticationFailureHandler
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public SimpleUrlAuthenticationFailureHandler authenticationFailureHandler() {
        SimpleUrlAuthenticationFailureHandler failureHandler =
                new SimpleUrlAuthenticationFailureHandler();
        failureHandler.setUseForward(true);
        failureHandler.setDefaultFailureUrl("/error");
        return failureHandler;
    }

    /**
     * The SAMLWebSSOHoKProcessingFilter (holder-of-key) that ties together the
     * AuthenticationManager, the AuthenticationSuccessHandler,
     * and the AuthenticationFailureHandler.
     *
     * @return the new SAMLWebSSOHoKProcessingFilter bean
     * @throws Exception if there's an error creating the AuthenticationManager
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLWebSSOHoKProcessingFilter samlWebSSOHoKProcessingFilter() throws Exception {
        SAMLWebSSOHoKProcessingFilter samlWebSSOHoKProcessingFilter = new SAMLWebSSOHoKProcessingFilter();
        samlWebSSOHoKProcessingFilter.setAuthenticationSuccessHandler(successRedirectHandler());
        samlWebSSOHoKProcessingFilter.setAuthenticationManager(authenticationManager());
        samlWebSSOHoKProcessingFilter.setAuthenticationFailureHandler(authenticationFailureHandler());
        return samlWebSSOHoKProcessingFilter;
    }

    /**
     * Processing filter for WebSSO profile messages that ties together the
     * AuthenticationManager, the AuthenticationSuccessHandler,
     * and the AuthenticationFailureHandler.     *
     * @return a new SAMLProcessingFilter
     * @throws Exception if there's an error creating the AuthenticationManager
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLProcessingFilter samlWebSSOProcessingFilter() throws Exception {
        SAMLProcessingFilter samlWebSSOProcessingFilter = new SAMLProcessingFilter();
        samlWebSSOProcessingFilter.setAuthenticationManager(authenticationManager());
        samlWebSSOProcessingFilter.setAuthenticationSuccessHandler(successRedirectHandler());
        samlWebSSOProcessingFilter.setAuthenticationFailureHandler(authenticationFailureHandler());
        return samlWebSSOProcessingFilter;
    }

    /**
     * Create a filer based on the metadataGenerator bean created above.
     *
     * @return the new MetadataGeneratorFilter
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public MetadataGeneratorFilter metadataGeneratorFilter() {
        return new MetadataGeneratorFilter(metadataGenerator());
    }

    /**
     * Handler for successful logout, redirecting to the URL "/".
     *
     * @return a SimpleUrlLogoutSuccessHandler
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public SimpleUrlLogoutSuccessHandler successLogoutHandler() {
        SimpleUrlLogoutSuccessHandler successLogoutHandler = new SimpleUrlLogoutSuccessHandler();
        successLogoutHandler.setDefaultTargetUrl(SAML_REDIRECT_URL);
        return successLogoutHandler;
    }

    /**
     * Logout handler terminating local session.
     *
     * @return a logout handler that clears the HTTP session
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public SecurityContextLogoutHandler securityContextLogoutHandler() {
        SecurityContextLogoutHandler logoutHandler =
                new SecurityContextLogoutHandler();
        logoutHandler.setInvalidateHttpSession(true);
        logoutHandler.setClearAuthentication(true);
        return logoutHandler;
    }

    /**
     * Add a call to log out from the context in addition to the success logout handler.

     * @return the ProcessingFilter for the SAML logout
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLLogoutProcessingFilter samlLogoutProcessingFilter() {
        return new SAMLLogoutProcessingFilter(
            successLogoutHandler(),
            securityContextLogoutHandler());
    }

    /**
     * Overrides default logout processing filter with the one processing SAML messages.
     *
     * @return the processing filter for SAML logout handling
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLLogoutFilter samlLogoutFilter() {
        return new SAMLLogoutFilter(successLogoutHandler(),
                new LogoutHandler[]{securityContextLogoutHandler()},
                new LogoutHandler[]{securityContextLogoutHandler()});
    }

    /**
     * Configure an HTTPSOAP11Binding binding.
     *
     * @return the configured HTTPSOAP11Binding
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public HTTPSOAP11Binding soapBinding() {
        return new HTTPSOAP11Binding(parserPool());
    }

    /**
     * Create an HTTP Post binding using the parserPool and velocityEngine defined above.
     *
     * @return the SAML Velocity handler for HTTP post binding handling
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public HTTPPostBinding httpPostBinding() {
        return new HTTPPostBinding(parserPool(), velocityEngine());
    }

    /**
     * Create a SAML parser binding for HTTP redirect responses.
     *
     * @return the SAML Velocity handler for redirected responses
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public HTTPRedirectDeflateBinding httpRedirectDeflateBinding() {
        return new HTTPRedirectDeflateBinding(parserPool());
    }

    /**
     * Create a SAML parser for HTTP Soap 11 responses.
     *
     * @return the SAML Velocity parser for HTTP Soap 11 responses
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public HTTPSOAP11Binding httpSOAP11Binding() {
        return new HTTPSOAP11Binding(parserPool());
    }

    /**
     * Create a SAML parser for HTTP Soap AOS responses.
     *
     * @return a SAML parser for HTTP Soap AOS responses
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public HTTPPAOS11Binding httpPAOS11Binding() {
        return new HTTPPAOS11Binding(parserPool());
    }

    /**
     * Top level SAML Processor.
     *
     * @return a SAML top-level processor
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLProcessorImpl processor() {
        Collection<SAMLBinding> bindings = new ArrayList<>();
        bindings.add(httpRedirectDeflateBinding());
        bindings.add(httpPostBinding());
        bindings.add(httpSOAP11Binding());
        bindings.add(httpPAOS11Binding());
        return new SAMLProcessorImpl(bindings);
    }

    /**
     * Define the security filter chain in order to support SSO Auth by using SAML 2.0.
     *
     * @return Filter chain proxy
     * @throws Exception if there is an error creating one of the filters
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public FilterChainProxy samlFilter() throws Exception {
        return new FilterChainProxy(Lists.newArrayList(
            filterChainEntry("/vmturbo/saml/login/**", samlEntryPoint()),
            filterChainEntry("/vmturbo/saml/logout/**", samlLogoutFilter()),
            filterChainEntry("/vmturbo/saml/metadata/**", metadataDisplayFilter()),
            filterChainEntry("/vmturbo/saml/SSO/**", samlWebSSOProcessingFilter()),
            filterChainEntry("/vmturbo/saml/SSOHoK/**", samlWebSSOHoKProcessingFilter()),
            filterChainEntry("/vmturbo/saml/SingleLogout/**", samlLogoutProcessingFilter())
        ));
    }

    private DefaultSecurityFilterChain filterChainEntry(@Nonnull final String pattern,
                                                        @Nonnull final GenericFilterBean filter) {
        return new DefaultSecurityFilterChain(new AntPathRequestMatcher(pattern),
            filter);
    }

    /**
     * Returns the authentication manager currently used by Spring.
     * It represents a bean definition with the aim allow wiring from
     * other classes performing the Inversion of Control (IoC).
     *
     * @throws Exception if there's an error creating an AuthenticationManager
     */
    @Bean
    @Conditional(SAMLCondition.class)
    @Override
    public AuthenticationManager authenticationManagerBean() throws Exception {
        return super.authenticationManagerBean();
    }

    /**
     * Publish http session event to remove destroyed sessions from
     * {@link org.springframework.security.core.session.SessionRegistry}.
     *
     * @return  publisher for HTTP Session events
     */
    @Bean
    public HttpSessionEventPublisher httpSessionEventPublisher() {
        return new HttpSessionEventPublisher();
    }

    /**
     * Keep user sessions, so they can be used to expire deleted user's active sessions.
     *
     * @return an instance of the Spring Session Registry default implementation
     */
    @Bean
    public SessionRegistry sessionRegistry() {
        return new SessionRegistryImpl();
    }

    /**
     * Add a handler for auding failed Auth requests.
     *
     * @return a handler for Failed Auth Requests that logs messages to the audit log
     */
    @Bean
    public FailedAuthRequestAuditor failedAuthRequestAuditor() {
        return new FailedAuthRequestAuditor(applicationContext);
    }
}

