package com.vmturbo.api.component.external.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.velocity.app.VelocityEngine;
import org.opensaml.saml2.metadata.provider.DOMMetadataProvider;
import org.opensaml.saml2.metadata.provider.MetadataProvider;
import org.opensaml.saml2.metadata.provider.MetadataProviderException;
import org.opensaml.xml.parse.StaticBasicParserPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.MethodInvokingFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.saml.SAMLAuthenticationProvider;
import org.springframework.security.saml.SAMLBootstrap;
import org.springframework.security.saml.SAMLEntryPoint;
import org.springframework.security.saml.SAMLLogoutFilter;
import org.springframework.security.saml.SAMLLogoutProcessingFilter;
import org.springframework.security.saml.SAMLProcessingFilter;
import org.springframework.security.saml.SAMLWebSSOHoKProcessingFilter;
import org.springframework.security.saml.context.SAMLContextProviderImpl;
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
import org.springframework.security.saml.websso.WebSSOProfileConsumerImpl;
import org.springframework.security.saml.websso.WebSSOProfileECPImpl;
import org.springframework.security.saml.websso.WebSSOProfileImpl;
import org.springframework.security.saml.websso.WebSSOProfileOptions;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.DefaultSecurityFilterChain;
import org.springframework.security.web.FilterChainProxy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.channel.ChannelProcessingFilter;
import org.springframework.security.web.authentication.SavedRequestAwareAuthenticationSuccessHandler;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler;
import org.springframework.security.web.authentication.logout.LogoutHandler;
import org.springframework.security.web.authentication.logout.SecurityContextLogoutHandler;
import org.springframework.security.web.authentication.logout.SimpleUrlLogoutSuccessHandler;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import com.vmturbo.api.component.external.api.SAML.SAMLCondition;
import com.vmturbo.api.component.external.api.SAML.SAMLUtils;
import com.vmturbo.api.component.external.api.SAML.WebSSOProfileConsumerImplExt;

/**
 * <p>Configure security for the REST API Dispatcher here.
 * The BASE_URI are defined in {@link com.vmturbo.api.component.external.api.ExternalApiConfig#BASE_URL_MAPPINGS}.</p>
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
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
public class ApiSecurityConfig extends WebSecurityConfigurerAdapter {
    // map "/vmturbo/rest/*" to "/vmturbo/rest/",
    // and "/vmturbo/api/v2/*" to "/vmturbo/api/v2/"
    private static Function<String, String> mappingsToBaseURI = input -> input.substring(0, input.length() - 1);
    private static ImmutableList<String> baseURIs =
            ImmutableList.copyOf(Iterables.transform(ExternalApiConfig.BASE_URL_MAPPINGS,
                    mappingsToBaseURI));

    @Autowired(required = false)
    private SAMLUserDetailsService samlUserDetailsService;

    @Value("${samlEnabled:false}")
    private boolean samlEnabled;

    // Initialization of OpenSAML library
    @Bean
    @Conditional(SAMLCondition.class)
    public static SAMLBootstrap sAMLBootstrap() {
        return new SAMLBootstrap();
    }

    @Bean
    public AuthenticationEntryPoint restAuthenticationEntryPoint() {
        return new RestAuthenticationEntryPoint();
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable();

        // SAML flag
        if (samlEnabled) {
            http.httpBasic()
                    .authenticationEntryPoint(samlEntryPoint());
            http.addFilterBefore(metadataGeneratorFilter(), ChannelProcessingFilter.class)
                    .addFilterAfter(samlFilter(), BasicAuthenticationFilter.class);
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
                    .antMatchers(base_uri + "admin/versions").permitAll()
                    .antMatchers(base_uri + "license").permitAll()
                    .antMatchers(base_uri + "licenses/summary").permitAll()
                    .antMatchers(base_uri + "initAdmin").permitAll()
                    .antMatchers(base_uri + "saml/**").permitAll()
                    .antMatchers(base_uri + "**").authenticated();
        }
    }

    // Initialization of the velocity engine for parsing SAML messages
    @Bean
    @Conditional(SAMLCondition.class)
    public VelocityEngine velocityEngine() {
        return VelocityFactory.getEngine();
    }

    // XML parser pool needed for OpenSAML parsing
    @Bean(initMethod = "initialize")
    @Conditional(SAMLCondition.class)
    public StaticBasicParserPool parserPool() {
        return new StaticBasicParserPool();
    }

    @Bean(name = "parserPoolHolder")
    @Conditional(SAMLCondition.class)
    public ParserPoolHolder parserPoolHolder() {
        return new ParserPoolHolder();
    }

    // SAML Authentication Provider responsible for validating of received SAML messages
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLAuthenticationProvider samlAuthenticationProvider() {
        SAMLAuthenticationProvider samlAuthenticationProvider = new SAMLAuthenticationProvider();
        samlAuthenticationProvider.setUserDetails(samlUserDetailsService);
        samlAuthenticationProvider.setForcePrincipalAsString(false);
        return samlAuthenticationProvider;
    }

    // Provider of default SAML Context
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLContextProviderImpl contextProvider() {
        SAMLContextProviderLB samlContextProviderLB = new SAMLContextProviderLB();
        samlContextProviderLB.setScheme("https");
        samlContextProviderLB.setServerName("localhost");
        samlContextProviderLB.setServerPort(443);
        samlContextProviderLB.setIncludeServerPortInRequestURL(false);
        samlContextProviderLB.setContextPath("/");
        return samlContextProviderLB;
    }

    // Logger for SAML messages and events
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLDefaultLogger samlLogger() {
        return new SAMLDefaultLogger();
    }

    // SAML 2.0 WebSSO Assertion Consumer
    @Bean
    @Conditional(SAMLCondition.class)
    public WebSSOProfileConsumer webSSOprofileConsumer() {
        return new WebSSOProfileConsumerImplExt();
    }

    // SAML 2.0 Holder-of-Key WebSSO Assertion Consumer
    @Bean
    @Conditional(SAMLCondition.class)
    public WebSSOProfileConsumerHoKImpl hokWebSSOprofileConsumer() {
        return new WebSSOProfileConsumerHoKImpl();
    }

    // SAML 2.0 Web SSO profile
    @Bean
    @Conditional(SAMLCondition.class)
    public WebSSOProfile webSSOprofile() {
        return new WebSSOProfileImpl();
    }

    // SAML 2.0 Holder-of-Key Web SSO profile
    @Bean
    @Conditional(SAMLCondition.class)
    public WebSSOProfileConsumerHoKImpl hokWebSSOProfile() {
        return new WebSSOProfileConsumerHoKImpl();
    }

    // SAML 2.0 ECP profile
    @Bean
    @Conditional(SAMLCondition.class)
    public WebSSOProfileECPImpl ecpprofile() {
        return new WebSSOProfileECPImpl();
    }

    @Bean
    @Conditional(SAMLCondition.class)
    public SingleLogoutProfile logoutprofile() {
        return new SingleLogoutProfileImpl();
    }

    // Central storage of cryptographic keys
    @Bean
    @Conditional(SAMLCondition.class)
    public KeyManager keyManager() {
        DefaultResourceLoader loader = new DefaultResourceLoader();
        Resource storeFile = loader
                .getResource("classpath:/saml/samlKeystore.jks");
        String storePass = "nalle123";
        Map<String, String> passwords = new HashMap<String, String>();
        passwords.put("apollo", "nalle123");
        String defaultKey = "apollo";
        return new JKSKeyManager(storeFile, storePass, passwords, defaultKey);
    }

    // Setup TLS Socket Factory
    @Bean
    @Conditional(SAMLCondition.class)
    public TLSProtocolConfigurer tlsProtocolConfigurer() {
        return new TLSProtocolConfigurer();
    }

    @Bean
    @Conditional(SAMLCondition.class)
    public ProtocolSocketFactory socketFactory() {
        return new TLSProtocolSocketFactory(keyManager(), null, "default");
    }

    @Bean
    @Conditional(SAMLCondition.class)
    public Protocol socketFactoryProtocol() {
        return new Protocol("https", socketFactory(), 443);
    }

    @Bean
    @Conditional(SAMLCondition.class)
    public MethodInvokingFactoryBean socketFactoryInitialization() {
        MethodInvokingFactoryBean methodInvokingFactoryBean = new MethodInvokingFactoryBean();
        methodInvokingFactoryBean.setTargetClass(Protocol.class);
        methodInvokingFactoryBean.setTargetMethod("registerProtocol");
        Object[] args = {"https", socketFactoryProtocol()};
        methodInvokingFactoryBean.setArguments(args);
        return methodInvokingFactoryBean;
    }

    @Bean
    @Conditional(SAMLCondition.class)
    public WebSSOProfileOptions defaultWebSSOProfileOptions() {
        WebSSOProfileOptions webSSOProfileOptions = new WebSSOProfileOptions();
        webSSOProfileOptions.setIncludeScoping(false);
        return webSSOProfileOptions;
    }

    // Entry point to initialize authentication, default values taken from
    // properties file
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLEntryPoint samlEntryPoint() {
        SAMLEntryPoint samlEntryPoint = new SAMLEntryPoint();
        samlEntryPoint.setDefaultProfileOptions(defaultWebSSOProfileOptions());
        return samlEntryPoint;
    }

    // Setup advanced info about metadata
    @Bean
    @Conditional(SAMLCondition.class)
    public ExtendedMetadata extendedMetadata() {
        ExtendedMetadata extendedMetadata = new ExtendedMetadata();
        extendedMetadata.setIdpDiscoveryEnabled(false);
        extendedMetadata.setSignMetadata(false);
        extendedMetadata.setEcpEnabled(true);
        return extendedMetadata;
    }

    @Bean
    @Conditional(SAMLCondition.class)
    @Qualifier("okta")
    public ExtendedMetadataDelegate oktaExtendedMetadataProvider()
            throws ParserConfigurationException, SAXException, IOException {
        Element element = SAMLUtils.loadXMLFromString(SAMLUtils.SAMPLEIDPMETADA).getDocumentElement();
        DOMMetadataProvider provider = new DOMMetadataProvider(element);
        ExtendedMetadataDelegate extendedMetadataDelegate = new ExtendedMetadataDelegate(provider, new ExtendedMetadata());
        return extendedMetadataDelegate;
    }

    // IDP Metadata configuration - paths to metadata of IDPs in circle of trust is here.
    // Do no forget to call initialize method on providers
    @Bean
    @Conditional(SAMLCondition.class)
    @Qualifier("metadata")
    public CachingMetadataManager metadata() throws MetadataProviderException, IOException, SAXException, ParserConfigurationException {
        List<MetadataProvider> providers = new ArrayList<MetadataProvider>();
        providers.add(oktaExtendedMetadataProvider());
        return new CachingMetadataManager(providers);
    }

    // Filter automatically generates default SP metadata
    @Bean
    @Conditional(SAMLCondition.class)
    public MetadataGenerator metadataGenerator() {
        MetadataGenerator metadataGenerator = new MetadataGenerator();
        metadataGenerator.setEntityId("turbo");
        metadataGenerator.setEntityBaseURL("https://localhost/vmturbo");
        metadataGenerator.setExtendedMetadata(extendedMetadata());
        metadataGenerator.setIncludeDiscoveryExtension(false);
        metadataGenerator.setKeyManager(keyManager());
        return metadataGenerator;
    }

    // The filter is waiting for connections on URL suffixed with filterSuffix
    // and presents SP metadata there
    @Bean
    @Conditional(SAMLCondition.class)
    public MetadataDisplayFilter metadataDisplayFilter() {
        return new MetadataDisplayFilter();
    }

    // Handler deciding where to redirect user after successful login
    @Bean
    @Conditional(SAMLCondition.class)
    public SavedRequestAwareAuthenticationSuccessHandler successRedirectHandler() {
        SavedRequestAwareAuthenticationSuccessHandler successRedirectHandler =
                new SavedRequestAwareAuthenticationSuccessHandler();
        successRedirectHandler.setDefaultTargetUrl("/");
        return successRedirectHandler;
    }

    // Handler deciding where to redirect user after failed login
    @Bean
    @Conditional(SAMLCondition.class)
    public SimpleUrlAuthenticationFailureHandler authenticationFailureHandler() {
        SimpleUrlAuthenticationFailureHandler failureHandler =
                new SimpleUrlAuthenticationFailureHandler();
        failureHandler.setUseForward(true);
        failureHandler.setDefaultFailureUrl("/error");
        return failureHandler;
    }

    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLWebSSOHoKProcessingFilter samlWebSSOHoKProcessingFilter() throws Exception {
        SAMLWebSSOHoKProcessingFilter samlWebSSOHoKProcessingFilter = new SAMLWebSSOHoKProcessingFilter();
        samlWebSSOHoKProcessingFilter.setAuthenticationSuccessHandler(successRedirectHandler());
        samlWebSSOHoKProcessingFilter.setAuthenticationManager(authenticationManager());
        samlWebSSOHoKProcessingFilter.setAuthenticationFailureHandler(authenticationFailureHandler());
        return samlWebSSOHoKProcessingFilter;
    }

    // Processing filter for WebSSO profile messages
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLProcessingFilter samlWebSSOProcessingFilter() throws Exception {
        SAMLProcessingFilter samlWebSSOProcessingFilter = new SAMLProcessingFilter();
        samlWebSSOProcessingFilter.setAuthenticationManager(authenticationManager());
        samlWebSSOProcessingFilter.setAuthenticationSuccessHandler(successRedirectHandler());
        samlWebSSOProcessingFilter.setAuthenticationFailureHandler(authenticationFailureHandler());
        return samlWebSSOProcessingFilter;
    }

    @Bean
    @Conditional(SAMLCondition.class)
    public MetadataGeneratorFilter metadataGeneratorFilter() {
        return new MetadataGeneratorFilter(metadataGenerator());
    }

    // Handler for successful logout
    @Bean
    @Conditional(SAMLCondition.class)
    public SimpleUrlLogoutSuccessHandler successLogoutHandler() {
        SimpleUrlLogoutSuccessHandler successLogoutHandler = new SimpleUrlLogoutSuccessHandler();
        successLogoutHandler.setDefaultTargetUrl("/");
        return successLogoutHandler;
    }

    // Logout handler terminating local session
    @Bean
    @Conditional(SAMLCondition.class)
    public SecurityContextLogoutHandler logoutHandler() {
        SecurityContextLogoutHandler logoutHandler =
                new SecurityContextLogoutHandler();
        logoutHandler.setInvalidateHttpSession(true);
        logoutHandler.setClearAuthentication(true);
        return logoutHandler;
    }

    // Filter processing incoming logout messages
    // First argument determines URL user will be redirected to after successful
    // global logout
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLLogoutProcessingFilter samlLogoutProcessingFilter() {
        return new SAMLLogoutProcessingFilter(successLogoutHandler(),
                logoutHandler());
    }

    // Overrides default logout processing filter with the one processing SAML
    // messages
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLLogoutFilter samlLogoutFilter() {
        return new SAMLLogoutFilter(successLogoutHandler(),
                new LogoutHandler[]{logoutHandler()},
                new LogoutHandler[]{logoutHandler()});
    }

    @Bean
    @Conditional(SAMLCondition.class)
    public HTTPSOAP11Binding soapBinding() {
        return new HTTPSOAP11Binding(parserPool());
    }

    @Bean
    @Conditional(SAMLCondition.class)
    public HTTPPostBinding httpPostBinding() {
        return new HTTPPostBinding(parserPool(), velocityEngine());
    }

    @Bean
    @Conditional(SAMLCondition.class)
    public HTTPRedirectDeflateBinding httpRedirectDeflateBinding() {
        return new HTTPRedirectDeflateBinding(parserPool());
    }

    @Bean
    @Conditional(SAMLCondition.class)
    public HTTPSOAP11Binding httpSOAP11Binding() {
        return new HTTPSOAP11Binding(parserPool());
    }

    @Bean
    @Conditional(SAMLCondition.class)
    public HTTPPAOS11Binding httpPAOS11Binding() {
        return new HTTPPAOS11Binding(parserPool());
    }

    // Processor
    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLProcessorImpl processor() {
        Collection<SAMLBinding> bindings = new ArrayList<SAMLBinding>();
        bindings.add(httpRedirectDeflateBinding());
        bindings.add(httpPostBinding());
        bindings.add(httpSOAP11Binding());
        bindings.add(httpPAOS11Binding());
        return new SAMLProcessorImpl(bindings);
    }

    /**
     * Define the security filter chain in order to support SSO Auth by using SAML 2.0
     *
     * @return Filter chain proxy
     * @throws Exception
     */
    @Bean
    @Conditional(SAMLCondition.class)
    public FilterChainProxy samlFilter() throws Exception {
        List<SecurityFilterChain> chains = new ArrayList<SecurityFilterChain>();
        chains.add(new DefaultSecurityFilterChain(new AntPathRequestMatcher("/vmturbo/saml/login/**"),
                samlEntryPoint()));
        chains.add(new DefaultSecurityFilterChain(new AntPathRequestMatcher("/vmturbo/saml/logout/**"),
                samlLogoutFilter()));
        chains.add(new DefaultSecurityFilterChain(new AntPathRequestMatcher("/vmturbo/saml/metadata/**"),
                metadataDisplayFilter()));
        chains.add(new DefaultSecurityFilterChain(new AntPathRequestMatcher("/vmturbo/saml/SSO/**"),
                samlWebSSOProcessingFilter()));
        chains.add(new DefaultSecurityFilterChain(new AntPathRequestMatcher("/vmturbo/saml/SSOHoK/**"),
                samlWebSSOHoKProcessingFilter()));
        chains.add(new DefaultSecurityFilterChain(new AntPathRequestMatcher("/vmturbo/saml/SingleLogout/**"),
                samlLogoutProcessingFilter()));
        return new FilterChainProxy(chains);
    }

    /**
     * Returns the authentication manager currently used by Spring.
     * It represents a bean definition with the aim allow wiring from
     * other classes performing the Inversion of Control (IoC).
     *
     * @throws Exception
     */
    @Bean
    @Conditional(SAMLCondition.class)
    @Override
    public AuthenticationManager authenticationManagerBean() throws Exception {
        return super.authenticationManagerBean();
    }

    /**
     * Sets a custom authentication provider.
     *
     * @param auth SecurityBuilder used to create an AuthenticationManager.
     * @throws Exception
     */
    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        if (samlEnabled) {
            auth.authenticationProvider(samlAuthenticationProvider());
        }
    }
}

