package com.vmturbo.components.common;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;

import com.google.gson.Gson;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.components.common.health.CompositeHealthMonitor;
import com.vmturbo.components.common.health.HealthStatus;
import com.vmturbo.components.common.health.HealthStatusProvider;
import com.vmturbo.components.common.health.SimpleHealthStatus;

@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
public class ComponentControllerTest {

    protected static MockMvc mockMvc;

    private static final String API_PREFIX="";
    @Autowired
    IVmtComponent componentMock;

    @Autowired
    private WebApplicationContext wac;

    @Before
    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @Test
    public void testHealthEndpoint() throws Exception {

        // Arrange
        CompositeHealthMonitor yesMon = new CompositeHealthMonitor("Test"); // simple composite health monitor that is always healthy
        yesMon.addHealthCheck(new HealthStatusProvider() {
            @Override
            public String getName() { return "HealthyMcHealthFace"; }
            @Override
            public HealthStatus getHealthStatus() {
                return new SimpleHealthStatus(true,"");
            }
        });
        when(componentMock.getHealthMonitor()).thenReturn(yesMon);

        // Act
        MvcResult result = mockMvc.perform(get(API_PREFIX + "/health")
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().isOk())
                .andReturn();
        // Assert is already covered in the expectation of status.isOk
    }

    @Test
    public void testGetState() throws Exception {

        // Arrange
        when(componentMock.getComponentStatus()).thenReturn(ExecutionStatus.RUNNING);
        // Act
        MvcResult result = mockMvc.perform(get(API_PREFIX + "/state")
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().isOk())
                .andReturn();
        // Assert
        Assert.assertEquals(ExecutionStatus.RUNNING.toString(),
                new Gson().fromJson(result.getResponse().getContentAsString(), String.class));
    }

    @Configuration
    @EnableWebMvc
    public static class TestConfiguration extends WebMvcConfigurerAdapter {
        @Bean
        public ComponentController componentController() {
            return new ComponentController();
        }

        @Bean
        public IVmtComponent theComponent() {
            return Mockito.mock(IVmtComponent.class);
        }

        @Override
        public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
            converters.add(new MappingJackson2HttpMessageConverter());
            converters.add(new StringHttpMessageConverter());
        }

        @Override
        public void configureContentNegotiation(ContentNegotiationConfigurer configurer) {
            configurer.defaultContentType(MediaType.APPLICATION_JSON_UTF8);
        }
    }
}
