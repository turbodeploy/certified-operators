package com.vmturbo.topology.processor.rest;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;

import com.google.gson.Gson;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
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

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 * Tests for the {@link IdentityController} class.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class IdentityControllerTest {

    private final Gson gson = new Gson();
    private MockMvc mockMvc;
    private EntityStore entityStore;

    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    static class ContextConfiguration extends WebMvcConfigurerAdapter {

        @Bean
        EntityStore entityStore() {
            return Mockito.mock(EntityStore.class);
        }

        @Bean
        IdentityController identityController() {
            return new IdentityController(entityStore());
        }

        @Override
        public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
            GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
            msgConverter.setGson(ComponentGsonFactory.createGson());
            converters.add(msgConverter);
        }
    }

    @Autowired
    private WebApplicationContext wac;

    /**
     * Set up the test environment.
     */
    @Before
    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
        entityStore = wac.getBean(EntityStore.class);
    }

    /**
     * Test that the the send() method in {@link TopologyController} gets invoked
     * and that the expected response text is received.
     * @throws Exception if the POST fails
     */
    @Test
    public void testExpireOids() throws Exception {
        final int expiredOids = 10;
        when(entityStore.expireOids()).thenReturn(expiredOids);
        final MvcResult result = mockMvc.perform(post("/identity/expire")
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();
        String contentJson = result.getResponse().getContentAsString();
        assertTrue(contentJson.contains(String.format("Expired %d oids", expiredOids)));

    }
}
