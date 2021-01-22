package com.vmturbo.auth.component;

import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.ADMINISTRATOR;
import static com.vmturbo.auth.api.authorization.jwt.SecurityConstant.OBSERVER;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDecisionManager;
import org.springframework.security.access.AccessDecisionVoter;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.expression.method.ExpressionBasedPreInvocationAdvice;
import org.springframework.security.access.expression.method.MethodSecurityExpressionHandler;
import org.springframework.security.access.prepost.PreInvocationAuthorizationAdviceVoter;
import org.springframework.security.access.vote.AuthenticatedVoter;
import org.springframework.security.access.vote.RoleVoter;
import org.springframework.security.access.vote.UnanimousBased;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.method.configuration.GlobalMethodSecurityConfiguration;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.AnnotationConfigWebContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.util.NestedServletException;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.vmturbo.auth.api.authentication.AuthenticationException;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationToken;
import com.vmturbo.auth.api.authorization.jwt.JWTAuthorizationVerifier;
import com.vmturbo.auth.api.authorization.kvstore.AuthStore;
import com.vmturbo.auth.api.authorization.kvstore.IAuthStore;
import com.vmturbo.auth.api.authorization.spring.SpringMethodSecurityExpressionHandler;
import com.vmturbo.auth.api.usermgmt.ActiveDirectoryDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO.PROVIDER;
import com.vmturbo.auth.api.usermgmt.AuthUserModifyDTO;
import com.vmturbo.auth.api.usermgmt.AuthorizeUserInGroupsInputDTO;
import com.vmturbo.auth.api.usermgmt.AuthorizeUserInputDTO;
import com.vmturbo.auth.api.usermgmt.SecurityGroupDTO;
import com.vmturbo.auth.component.policy.UserPolicy;
import com.vmturbo.auth.component.policy.UserPolicy.LoginPolicy;
import com.vmturbo.auth.component.services.AuthUsersController;
import com.vmturbo.auth.component.store.AuthProvider;
import com.vmturbo.auth.component.store.sso.SsoUtil;
import com.vmturbo.kvstore.IPublicKeyStore;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.kvstore.PublicKeyStore;

/**
 * The RestTest implements the REST component tests.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
public class RestTest {
    /**
     * The mock service.
     */
    private MockMvc mockMvc;

    /**
     * The JSON builder.
     */
    private static final Gson GSON = new GsonBuilder().create();

    /**
     * The password prefix.
     */
    private static final String PASSWD_PREFIX = "! PwD&#";

    @Autowired
    private WebApplicationContext wac;

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * The KV store
     */
    private static KeyValueStore kvStore = new MapKeyValueStore();

    private static IPublicKeyStore publicKeyStore = Mockito.mock(PublicKeyStore.class);

    /**
     * The AUTH KV store
     */
    private static IAuthStore apiKVStore = new AuthStore(kvStore, publicKeyStore);

    /**
     * The K/V local auth store.
     */
    private static AuthProvider authStore = new AuthProvider(kvStore, null,
            () -> System.getProperty("com.vmturbo.kvdir"), null, new UserPolicy(LoginPolicy.ALL),
            new SsoUtil(), false);

    /**
     * The verifier.
     */
    private static JWTAuthorizationVerifier verifier = new JWTAuthorizationVerifier(apiKVStore);

    private AnnotationConfigWebApplicationContext applicationContext;

    @BeforeClass
    public static void staticSetUp() {
        System.setProperty("instance_id", "auth-1");
        System.setProperty("identityGeneratorPrefix", "7");
    }

    @Before
    public void setUp() throws IOException {
        System.setProperty("com.vmturbo.keydir", tempFolder.newFolder().getAbsolutePath());
        System.setProperty("com.vmturbo.kvdir", tempFolder.newFolder().getAbsolutePath());
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    private static final String RET_TYPE = MediaType.APPLICATION_JSON_UTF8_VALUE;

    @Configuration
    @EnableGlobalMethodSecurity(prePostEnabled = true)
    static class SecurityConfig extends GlobalMethodSecurityConfiguration {
        /**
         * {@inheritDoc}
         */
        @Override
        protected MethodSecurityExpressionHandler createExpressionHandler() {
            return new SpringMethodSecurityExpressionHandler();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected AccessDecisionManager accessDecisionManager() {
            List<AccessDecisionVoter<? extends Object>>
                    decisionVoters = new ArrayList<>();
            ExpressionBasedPreInvocationAdvice expressionAdvice =
                    new ExpressionBasedPreInvocationAdvice();
            expressionAdvice.setExpressionHandler(getExpressionHandler());
            decisionVoters
                    .add(new PreInvocationAuthorizationAdviceVoter(expressionAdvice));
            decisionVoters.add(new RoleVoter());
            decisionVoters.add(new AuthenticatedVoter());
            return new UnanimousBased(decisionVoters);
        }
    }

    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    static class ContextConfiguration extends WebMvcConfigurerAdapter {

        @Bean
        public KeyValueStore keyValueStore() {
            return kvStore;
        }

        @Bean
        public AuthProvider targetStore() {
            return authStore;
        }

        @Bean
        public AuthUsersController authUsersController() {
            return new AuthUsersController(targetStore());
        }

        @Bean
        public IAuthStore apiKVStore() {
            return apiKVStore;
        }

        @Bean
        public GlobalMethodSecurityConfiguration securityConfiguration() {
            return new SecurityConfig();
        }

        @Bean
        public TestExceptionHandler exceptionHandler() {
            return new TestExceptionHandler();
        }
    }

    private String constructPassword(int suffix) {
        return PASSWD_PREFIX + suffix;
    }

    private String constructEncodedPassword(int suffix) throws UnsupportedEncodingException {
        return URLEncoder.encode(PASSWD_PREFIX + suffix, "UTF-8");
    }

    private String constructAddDTO(int suffix) {
        // ideally we should use Parameterized test, but we already have Spring Runner, so
        // we just randomly change to role to upper case or lower case.
        AuthUserDTO dto = new AuthUserDTO(AuthUserDTO.PROVIDER.LOCAL, "user" + suffix,
                                          constructPassword(suffix), "1.1.1.1", null, null,
                                          ImmutableList.of(new Random().nextBoolean() ? ADMINISTRATOR.toUpperCase()
                                                  : ADMINISTRATOR.toLowerCase(), OBSERVER), null);
        // For debigging purposes.
        String json = GSON.toJson(dto, AuthUserDTO.class);
        return json;
    }


    private String constructAddSSODTO(int suffix) {
        AuthUserDTO dto = new AuthUserDTO(PROVIDER.LDAP, "user" + suffix,
                constructPassword(suffix), "1.1.1.1", null, null,
                ImmutableList.of(new Random().nextBoolean() ? ADMINISTRATOR.toUpperCase()
                        : ADMINISTRATOR.toLowerCase(), OBSERVER), null);
        // For debigging purposes.
        String json = GSON.toJson(dto, AuthUserDTO.class);
        return json;
    }

    private String constructLockDTO(int suffix) {
        AuthUserDTO dto = new AuthUserDTO(AuthUserDTO.PROVIDER.LOCAL, "user" + suffix, null,
                                          ImmutableList.of("ADMIN", "USER"));
        String json = GSON.toJson(dto, AuthUserDTO.class);
        return json;
    }

    private MockHttpServletRequestBuilder postAdd(int suffix) {
        return post("/users/add")
                .content(constructAddDTO(suffix))
                .contentType(RET_TYPE)
                .accept(RET_TYPE);
    }

    private MockHttpServletRequestBuilder postAddSSO(int suffix) {
        return post("/users/add")
                .content(constructAddSSODTO(suffix))
                .contentType(RET_TYPE)
                .accept(RET_TYPE);
    }

    private MockHttpServletRequestBuilder postAddSSO() {

        ActiveDirectoryDTO activeDirectoryDTO = new ActiveDirectoryDTO("corp.vmturbo.com",
                "dell1.corp.vmturbo.com",
                false);
        String json = GSON.toJson(activeDirectoryDTO, ActiveDirectoryDTO.class);
        return post("/users/ad")
                .content(json)
                .contentType(RET_TYPE)
                .accept(RET_TYPE);

    }


    private MockHttpServletRequestBuilder postAddSSOGroup() {

        SecurityGroupDTO activeDirectorySecurityGroupDTO = new SecurityGroupDTO("group",
                "group", new Random().nextBoolean() ? ADMINISTRATOR.toUpperCase()
                        : ADMINISTRATOR.toLowerCase());
        String jsonGroup = GSON.toJson(activeDirectorySecurityGroupDTO, SecurityGroupDTO.class);

        return post("/users/ad/groups")
                .content(jsonGroup)
                .contentType(RET_TYPE)
                .accept(RET_TYPE);
    }

    /**
     * Performs the actual logon from the token passed though.
     */
    private void logon(String role) throws Exception {
        // Local authentication
        Set<GrantedAuthority> grantedAuths = new HashSet<>();
        for (String r : role.split("\\|")) {
            grantedAuths.add(new SimpleGrantedAuthority("ROLE" + "_" + r.toUpperCase()));
        }
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken("admin", "admin000", grantedAuths));
    }

    @Test
    public void testAdd() throws Exception {
        logon("ADMINISTRATOR");
        String result = mockMvc.perform(postAdd(0))
                               .andExpect(status().isOk())
                               .andReturn().getResponse().getContentAsString();
        validateAddUserResult(result);
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * Verify invalid role will throw security exception.
     * @throws Exception security exception.
     */
    @Test
    public void testAddInvalidRole() throws Exception {

        String json = GSON.toJson(new AuthUserDTO(AuthUserDTO.PROVIDER.LOCAL, "user",
                "1", "1.1.1.1", null, null,
                ImmutableList.of("invalid_role"), null), AuthUserDTO.class);


        logon("ADMINISTRATOR");
        String result = mockMvc.perform(post("/users/add")
                .content(json)
                .contentType(RET_TYPE)
                .accept(RET_TYPE))
                .andExpect(status().is4xxClientError())
                .andReturn()
                .getResponse()
                .getContentAsString();
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * Validate that the add user result is a oid which is a string with a long number.
     * @param result from calling post to add a new user.
     */
    private void validateAddUserResult(String result) {
        Assert.assertNotNull(result);
        Assert.assertFalse(result.isEmpty());
        Long value = Long.parseLong(result);
        Assert.assertTrue(value > 0);
    }

    @Test
    public void testAddNoAccess() throws Exception {
        logon("USER");
        String result = mockMvc.perform(postAdd(0))
                               .andExpect(status().isConflict())
                               .andReturn().getResponse().getContentAsString();
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    @Test
    public void testAuthenticate() throws Exception {
        // The logon is here to work around the issue with the WebSecurity setup.
        logon("ADMINISTRATOR");
        String result = mockMvc.perform(postAdd(1))
                               .andExpect(status().isOk())
                               .andReturn().getResponse().getContentAsString();
       validateAddUserResult(result);

        // The authenticate call does not require any prior authentication.
        SecurityContextHolder.getContext().setAuthentication(null);

        mockMvc.perform(get("/users/authenticate/user1/" + constructEncodedPassword(1))
                                .accept(RET_TYPE))
               .andExpect(status().isOk())
               .andReturn().getResponse().getContentAsString();
    }

    @Test
    public void testSetPassword() throws Exception {
        logon("ADMINISTRATOR");
        String result = mockMvc.perform(postAdd(2))
                               .andExpect(status().isOk())
                               .andReturn().getResponse().getContentAsString();
       validateAddUserResult(result);

        AuthUserDTO userToModify = new AuthUserDTO(AuthUserDTO.PROVIDER.LOCAL, "user" + 2,
                                                      constructPassword(2), null, null, null,
                                                      ImmutableList.of(ADMINISTRATOR, "USER"), null);
        AuthUserModifyDTO dto = new AuthUserModifyDTO(userToModify, "password1_" + 2);

        String json = GSON.toJson(dto, AuthUserModifyDTO.class);

        mockMvc.perform(put("/users/setpassword")
                                .content(json)
                                .contentType(RET_TYPE)
                                .accept(RET_TYPE))
               .andExpect(status().isOk());

        // Authenticate against the original user.
        // We throw the SecurityException in our implementation, and the NestedServletException
        // will contain it.
        mockMvc.perform(get("/users/authenticate/user2/" + constructEncodedPassword(2))
                                .accept(RET_TYPE)).andExpect(status().isForbidden());

        // Authenticate against the changed user
        mockMvc.perform(get("/users/authenticate/user2/password1_2")
                                .accept(RET_TYPE))
               .andExpect(status().isOk())
               .andReturn().getResponse().getContentAsString();
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    @Test
    public void testSetRolesHasAccess() throws Exception {
        logon("ADMINISTRATOR");

        // original admin user
        mockMvc.perform(postAdd(11));

        // add new user and edit roles
        String result = mockMvc.perform(postAdd(12))
                               .andExpect(status().isOk())
                               .andReturn().getResponse().getContentAsString();
        validateAddUserResult(result);

        AuthUserDTO dto = new AuthUserDTO(AuthUserDTO.PROVIDER.LOCAL, "user" + 12, null,
                                          ImmutableList.of(ADMINISTRATOR, OBSERVER));
        String json = GSON.toJson(dto, AuthUserDTO.class);

        mockMvc.perform(put("/users/setroles")
                                .content(json)
                                .contentType(RET_TYPE)
                                .accept(RET_TYPE))
               .andExpect(status().isOk());

        // Authenticate against the changed user
        result = mockMvc.perform(get("/users/authenticate/user12/" + constructEncodedPassword(12))
                                         .accept(RET_TYPE))
                        .andExpect(status().isOk())
                        .andReturn().getResponse().getContentAsString();
        JWTAuthorizationToken token = new JWTAuthorizationToken(result);
        verifier.verify(token, ImmutableList.of(ADMINISTRATOR, OBSERVER));

        // delete all other admin users except user11
        String allUsersJson = mockMvc.perform(get("/users")).andReturn().getResponse().getContentAsString();
        for (Object userJson : GSON.fromJson(allUsersJson, List.class)) {
            AuthUserDTO authUserDTO = GSON.fromJson(GSON.toJson(userJson), AuthUserDTO.class);
            if (!"user11".equals(authUserDTO.getUser())) {
                mockMvc.perform(delete("/users/remove/" + authUserDTO.getUser()));
            }
        }
        // expect security exception if trying to change role for last admin user
        dto = new AuthUserDTO(AuthUserDTO.PROVIDER.LOCAL, "user11", null,
            ImmutableList.of(OBSERVER));
        json = GSON.toJson(dto, AuthUserDTO.class);
        mockMvc.perform(put("/users/setroles")
            .content(json)
            .contentType(RET_TYPE)
            .accept(RET_TYPE))
            .andExpect(status().isForbidden());

        SecurityContextHolder.getContext().setAuthentication(null);
    }

    @Test
    public void testSetRolesHasNoAccess() throws Exception {
        logon("ADMINISTRATOR");
        String result = mockMvc.perform(postAdd(25))
                               .andExpect(status().isOk())
                               .andReturn().getResponse().getContentAsString();
        validateAddUserResult(result);

        AuthUserDTO dto = new AuthUserDTO("user" + 25, null, ImmutableList.of("ADMIN", "USER2"));
        String json = GSON.toJson(dto, AuthUserDTO.class);

        SecurityContextHolder.getContext().setAuthentication(null);
        logon("PLAINUSER");
        mockMvc.perform(put("/users/setroles")
                                .content(json)
                                .contentType(RET_TYPE)
                                .accept(RET_TYPE))
               .andExpect(status().isForbidden());
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     * Verify changing to invalid role will throw security exception.
     *
     * @throws Exception security exception.
     */
    @Test
    public void testSetRolesInvalidRole() throws Exception {
        logon("ADMINISTRATOR");
        AuthUserDTO dto = new AuthUserDTO(AuthUserDTO.PROVIDER.LOCAL, "user" + 25, null,
                ImmutableList.of("INVALID ROLE"));
        String json = GSON.toJson(dto, AuthUserDTO.class);
        mockMvc.perform(put("/users/setroles")
                .content(json)
                .contentType(RET_TYPE)
                .accept(RET_TYPE))
                .andExpect(status().is4xxClientError());
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    @Test
    public void testDelete() throws Exception {
        logon("ADMINISTRATOR");
        String result = mockMvc.perform(postAdd(3))
                .andReturn().getResponse().getContentAsString();
        validateAddUserResult(result);
        // Create another local admin user
        mockMvc.perform(postAdd(4))
                .andReturn().getResponse().getContentAsString();

        // delete the first admin user
        mockMvc.perform(delete("/users/remove/user3")
                .accept(RET_TYPE))
                .andExpect(status().isOk())
                .andReturn().getResponse()
                .getContentAsString();

        // Authenticate against the original user.
        // We throw the SecurityException in our implementation, and the NestedServletException
        // will contain it.
        mockMvc.perform(get("/users/authenticate/user3/password3")
                .accept(RET_TYPE)).andExpect(status().isForbidden());

        // Delete the last admin user. We throw the SecurityException in our implementation.
        mockMvc.perform(delete("/users/remove/user3")
                .accept(RET_TYPE))
                .andExpect(status().isForbidden())
                .andReturn().getResponse()
                .getContentAsString();
        SecurityContextHolder.getContext().setAuthentication(null);
    }


    @Test
    public void testLock() throws Exception {
        logon("ADMINISTRATOR");
        String result = mockMvc.perform(postAdd(4))
                               .andExpect(status().isOk())
                               .andReturn().getResponse().getContentAsString();
        validateAddUserResult(result);

        String gson = constructLockDTO(4);
        mockMvc.perform(put("/users/lock")
                                .content(gson)
                                .contentType(RET_TYPE)
                                .accept(RET_TYPE))
               .andExpect(status().isOk())
               .andReturn().getResponse()
               .getContentAsString();

        // Authenticate against the original user.
        // We throw the SecurityException in our implementation, and the NestedServletException
        // will contain it.
        mockMvc.perform(get("/users/authenticate/user4/password4")
                                .accept(RET_TYPE)).andExpect(status().isForbidden());
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    @Test
    public void testUnlock() throws Exception {
        logon("ADMINISTRATOR");
        String result = mockMvc.perform(postAdd(5))
                               .andExpect(status().isOk())
                               .andReturn().getResponse().getContentAsString();
        validateAddUserResult(result);

        String gson = constructLockDTO(5);
        mockMvc.perform(put("/users/lock")
                                .content(gson)
                                .contentType(RET_TYPE)
                                .accept(RET_TYPE))
               .andExpect(status().isOk())
               .andReturn().getResponse()
               .getContentAsString();

        // Authenticate against the original user.
        // We throw the SecurityException in our implementation, and the NestedServletException
        // will contain it.
        mockMvc.perform(get("/users/authenticate/user5/password5")
                                .accept(RET_TYPE)).andExpect(status().isForbidden());

        mockMvc.perform(put("/users/unlock")
                                .content(gson)
                                .contentType(RET_TYPE)
                                .accept(RET_TYPE))
               .andExpect(status().isOk())
               .andReturn().getResponse()
               .getContentAsString();

        mockMvc.perform(get("/users/authenticate/user5/" + constructEncodedPassword(5))
                                .accept(RET_TYPE));
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    private MockHttpServletRequestBuilder constructInitDTO(int suffix) {
        AuthUserDTO dto =
                new AuthUserDTO(null, "user" + suffix, constructPassword(suffix),
                        "1.1.1.1", null, null, ImmutableList.of("USER"), null);
        String json = GSON.toJson(dto, AuthUserDTO.class);
        return post("/users/initAdmin")
                .content(json)
                .contentType(RET_TYPE)
                .accept(RET_TYPE);
    }

    private boolean checkAdminInit() throws Exception {
        String result = mockMvc.perform(get("/users/checkAdminInit")
                                                .accept(RET_TYPE))
                               .andExpect(status().isOk())
                               .andReturn().getResponse().getContentAsString();
        Assert.assertTrue(result.equals("true") || result.equals("false"));
        return Boolean.parseBoolean(result);
    }

    @Test
    public void testAdminCheckInitColdStart() throws Exception {
        Assert.assertFalse(checkAdminInit());
    }

    @Test
    public void testAdminInitColdStart() throws Exception {
        String result = mockMvc.perform(constructInitDTO(10))
                               .andExpect(status().isOk())
                               .andReturn().getResponse().getContentAsString();
        Assert.assertEquals("users://user10", result);
        Assert.assertTrue(checkAdminInit());
    }

    @Test
    public void testAdminInitWarmStart() throws Exception {
        String result = mockMvc.perform(constructInitDTO(13))
                               .andExpect(status().isOk())
                               .andReturn().getResponse().getContentAsString();
        Assert.assertEquals("users://user13", result);
        Assert.assertTrue(checkAdminInit());
        mockMvc.perform(constructInitDTO(14))
               .andExpect(status().isForbidden());
        Assert.assertTrue(checkAdminInit());
    }

    // Happy path
    @Test
    public void testAuthorizeUser() throws Exception {
        // The logon is here to work around the issue with the WebSecurity setup.
        logon("ADMINISTRATOR");
        String result = mockMvc.perform(postAddSSO(11))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        validateAddUserResult(result);

        // The authenticate call does not require any prior authentication.
        //SecurityContextHolder.getContext().setAuthentication(null);
        final AuthorizeUserInputDTO dto = new AuthorizeUserInputDTO("user11", null, "1.1.1.1");

        String json = GSON.toJson(dto, AuthorizeUserInputDTO.class);
        mockMvc.perform(post("/users/authorize")
                .content(json)
                .contentType(RET_TYPE)
                .accept(RET_TYPE))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        SecurityContextHolder.getContext().setAuthentication(null);
    }

    /**
     *  Negative path, test it will reject invalid user.
     */
    @Test(expected = NestedServletException.class)
    public void testAuthorizeInvalidUser() throws Exception {
        // The logon is here to work around the issue with the WebSecurity setup.
        logon("ADMINISTRATOR");
        final AuthorizeUserInputDTO dto =
                new AuthorizeUserInputDTO("wronguser", null, "1.1.1.1");

        String json = GSON.toJson(dto, AuthorizeUserInputDTO.class);
        mockMvc.perform(post("/users/authorize")
                .content(json)
                .contentType(RET_TYPE)
                .accept(RET_TYPE))
                .andExpect(status().is4xxClientError())
                .andReturn().getResponse().getContentAsString();
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    // Happy path
    @Test
    public void testAuthorizeUserWithExternalGroup() throws Exception {
        // The logon is here to work around the issue with the WebSecurity setup.
        logon("ADMINISTRATOR");
        String result = mockMvc.perform(postAddSSO())
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        result = mockMvc.perform(postAddSSOGroup())
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        AuthorizeUserInputDTO dto =
                new AuthorizeUserInputDTO("user1", "group", "1.1.1.1");

        String json = GSON.toJson(dto, AuthorizeUserInputDTO.class);
        mockMvc.perform(post("/users/authorize")
                .content(json)
                .contentType(RET_TYPE)
                .accept(RET_TYPE))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        SecurityContextHolder.getContext().setAuthentication(null);
        // clean security groups so it doesn't affect other tests
        authStore.deleteSecurityGroup("group");
    }


    // Negative path, note the Authorization exception is wrapped by NestedServletException
    @Test(expected = NestedServletException.class)
    public void testAuthorizeUserWithInvalidExternalGroup() throws Exception {
        // The logon is here to work around the issue with the WebSecurity setup.
        logon("ADMINISTRATOR");
        String result = mockMvc.perform(postAddSSO())
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        result = mockMvc.perform(postAddSSOGroup())
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        try {
            AuthorizeUserInputDTO dto =
                    new AuthorizeUserInputDTO("user1", "group1", "1.1.1.1");

            String json = GSON.toJson(dto, AuthorizeUserInputDTO.class);
            mockMvc.perform(post("/users/authorize")
                    .content(json)
                    .contentType(RET_TYPE)
                    .accept(RET_TYPE))
                    .andExpect(status().is4xxClientError())
                    .andReturn().getResponse().getContentAsString();

        } finally {
            SecurityContextHolder.getContext().setAuthentication(null);
            // clean security groups so it doesn't affect other tests
            authStore.deleteSecurityGroup("group");
        }
    }

    /**
     * Happy path - verify authorizing multiple external groups.
     *
     * @throws Exception if exception is thrown.
     */
    @Test
    public void testAuthorizeUserWithExternalGroups() throws Exception {
        // The logon is here to work around the issue with the WebSecurity setup.
        logon("ADMINISTRATOR");
        String result = mockMvc.perform(postAddSSO())
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        result = mockMvc.perform(postAddSSOGroup())
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        AuthorizeUserInGroupsInputDTO dto =
                new AuthorizeUserInGroupsInputDTO("user1", new String[] {"group"}, "1.1.1.1");

        String json = GSON.toJson(dto, AuthorizeUserInGroupsInputDTO.class);
        mockMvc.perform(post("/users/authorize/groups")
                .content(json)
                .contentType(RET_TYPE)
                .accept(RET_TYPE))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        SecurityContextHolder.getContext().setAuthentication(null);
        // clean security groups so it doesn't affect other tests
        authStore.deleteSecurityGroup("group");
    }

    /**
     * Negative path to verify authorizing multiple external groups.
     * Note the Authorization exception is wrapped by NestedServletException.
     */
    @Test(expected = NestedServletException.class)
    public void testAuthorizeUserWithInvalidExternalGroups() throws Exception {
        // The logon is here to work around the issue with the WebSecurity setup.
        logon("ADMINISTRATOR");
        String result = mockMvc.perform(postAddSSO())
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        result = mockMvc.perform(postAddSSOGroup())
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        try {
            AuthorizeUserInGroupsInputDTO dto =
                    new AuthorizeUserInGroupsInputDTO("user1", new String[] {"group1"}, "1.1.1.1");

            String json = GSON.toJson(dto, AuthorizeUserInGroupsInputDTO.class);
            mockMvc.perform(post("/users/authorize/groups")
                    .content(json)
                    .contentType(RET_TYPE)
                    .accept(RET_TYPE))
                    .andExpect(status().is4xxClientError())
                    .andReturn().getResponse().getContentAsString();

        } finally {
            SecurityContextHolder.getContext().setAuthentication(null);
            // clean security groups so it doesn't affect other tests
            authStore.deleteSecurityGroup("group");
        }
    }

    /**
     * Test delete external group with period in the name.
     * @throws Exception if failed.
     */
    @Test
    public void testDeleteSSOGroupWithPeriodInName() throws Exception {
        logon("ADMINISTRATOR");
        try {
            // setup
            final String groupNameWithPeriod = "g.aVirtualCenter_Admin";
            final String jsonGroup = GSON.toJson(
                    new SecurityGroupDTO(groupNameWithPeriod, "group", ADMINISTRATOR.toUpperCase()),
                    SecurityGroupDTO.class);

            // create group with period in the name.
            mockMvc.perform(post("/users/ad/groups").content(jsonGroup)
                    .contentType(RET_TYPE)
                    .accept(RET_TYPE))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse()
                    .getContentAsString();

            // delete the group
            mockMvc.perform(
                    delete("/users/ad/groups/" + groupNameWithPeriod + "/").accept(RET_TYPE))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse()
                    .getContentAsString();
        } finally {
            SecurityContextHolder.getContext().setAuthentication(null);
        }
    }

    @ControllerAdvice
    static class TestExceptionHandler {

        public TestExceptionHandler() {
        }

        @ExceptionHandler(value = AuthenticationException.class)
        @ResponseBody
        public ResponseEntity<String> handleAuthenticationException(AuthenticationException ex) {
            return new ResponseEntity(ex.getMessage(), HttpStatus.FORBIDDEN);
        }

        @ExceptionHandler(value = SecurityException.class)
        @ResponseBody
        public ResponseEntity<String> handleSecurityException(SecurityException ex) {
            return new ResponseEntity(ex.getMessage(), HttpStatus.FORBIDDEN);
        }

        @ExceptionHandler(value = AuthenticationCredentialsNotFoundException.class)
        @ResponseBody
        public ResponseEntity<String> handleAuthenticationCredentialsNotFoundException(
                AuthenticationCredentialsNotFoundException ex) {
            return new ResponseEntity(ex.getMessage(), HttpStatus.FORBIDDEN);
        }

        @ExceptionHandler(value = AccessDeniedException.class)
        @ResponseBody
        public ResponseEntity<String> handleAccessDeniedException(AccessDeniedException ex) {
            return new ResponseEntity(ex.getMessage(), HttpStatus.FORBIDDEN);
        }
    }
}
