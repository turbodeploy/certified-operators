package com.vmturbo.auth.component.services;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.gson.Gson;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.AnnotationConfigWebContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.auth.api.usermgmt.AuthUserDTO;
import com.vmturbo.auth.component.store.ISecureStore;

/**
 * Testing the SecureStorage REST API.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
// Need clean context with no probes/targets registered.
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class SecureStorageControllerTest {

    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    static class ContextConfiguration  extends WebMvcConfigurerAdapter {
        @Bean
        public ISecureStore secureDataStore() {
            return mock(ISecureStore.class);
        }

        @Bean
        public SecureStorageController secureStorageController() {
            return new SecureStorageController(secureDataStore());
        }

        @Override
        public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
            GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
            msgConverter.setGson(new Gson());
            converters.add(msgConverter);

            super.configureMessageConverters(converters);
        }
    }

    private MockMvc mockMvc;
    private ISecureStore secureStore;

    @Autowired
    private WebApplicationContext wac;

    @Before
    public void setUp() throws Exception {
        // Initialize identity generator so that targets can get IDs.
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
        secureStore = wac.getBean(ISecureStore.class);
    }

    @After
    public void shutDown() {
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    @Test
    public void testGetUnauthenticated() throws Exception {
        getAndExpect("/securestorage/get/user/foo", HttpStatus.UNAUTHORIZED);
    }

    @Test
    public void testGetUnauthorized() throws Exception {
        setupUser("user");

        getAndExpect("/securestorage/get/other-thing/foo", HttpStatus.FORBIDDEN);
    }


    @Test
    public void testNotExisting() throws Exception {
        setupUser("user");

        when(secureStore.get("user", "foo")).thenReturn(Optional.empty());
        getAndExpect("/securestorage/get/user/foo", HttpStatus.BAD_REQUEST);
    }

    @Test
    public void testGetAuthenticated() throws  Exception {
        setupUser("user");

        when(secureStore.get("user", "foo")).thenReturn(Optional.of("bar"));
        assertEquals("\"bar\"", getAndExpect("/securestorage/get/user/foo", HttpStatus.OK));
    }

    @Test
    public void testModify() throws Exception {
        setupUser("user");

        when(secureStore.modify("user", "foo", "new")).thenReturn("new");
        assertEquals("\"new\"", putAndExpect("/securestorage/modify/user/foo", "new", HttpStatus.OK));
    }

    @Test
    public void testDelete() throws Exception {
        setupUser("user");

        deleteAndExpect("/securestorage/delete/user/foo", HttpStatus.OK);
    }

    @Test
    public void testGetSqlDBRootPassword() throws Exception {
        when(secureStore.getRootSqlDBPassword()).thenReturn("foo");
        assertEquals("\"foo\"", getAndExpect("/securestorage/getSqlDBRootPassword", HttpStatus.OK));
    }

    @Test
    public void testGetArangoDbRootPassword() throws Exception {
        when(secureStore.getRootArangoDBPassword()).thenReturn("my-arango-password");
        assertEquals("\"my-arango-password\"",
            getAndExpect("/securestorage/getArangoDBRootPassword", HttpStatus.OK));
    }

    @Nonnull
    private String putAndExpect(@Nonnull final String path,
                                @Nonnull final String data,
                                @Nonnull final HttpStatus expectStatus) throws Exception {
        final MvcResult result = mockMvc.perform(put(path)
            .content(data)
            .contentType(MediaType.APPLICATION_JSON)
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(status().is(expectStatus.value()))
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
            .andReturn();
        return result.getResponse().getContentAsString();
    }

    private void deleteAndExpect(@Nonnull final String path,
                                   @Nonnull final HttpStatus expectStatus) throws Exception {
        mockMvc.perform(delete(path)
            .contentType(MediaType.APPLICATION_JSON)
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(status().is(expectStatus.value()))
            .andReturn();
    }

    @Nonnull
    private String getAndExpect(@Nonnull final String path,
                                @Nonnull final HttpStatus expectStatus) throws Exception {
        final MvcResult result = mockMvc.perform(get(path)
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(status().is(expectStatus.value()))
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
            .andReturn();
        return result.getResponse().getContentAsString();
    }

    private void setupUser(@Nonnull final String username) {
        final AuthUserDTO authUserDTO = new AuthUserDTO(username, null, Collections.emptyList());
        final UsernamePasswordAuthenticationToken token =
            new UsernamePasswordAuthenticationToken(authUserDTO, "password");

        // TODO: It is odd that setting the authentication on the contextHolder works.
        // See https://stackoverflow.com/questions/15203485/spring-test-security-how-to-mock-authentication
        SecurityContextHolder.getContext().setAuthentication(token);
    }
}