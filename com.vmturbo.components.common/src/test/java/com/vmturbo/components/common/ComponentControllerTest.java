package com.vmturbo.components.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.components.common.health.CompositeHealthMonitor;
import com.vmturbo.components.common.health.HealthStatus;
import com.vmturbo.components.common.health.HealthStatusProvider;
import com.vmturbo.components.common.health.SimpleHealthStatus;

@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration("classpath:component-controller-servlet-test.xml")

public class ComponentControllerTest extends WebMvcConfigurerAdapter {

    protected static MockMvc mockMvc;

    private static final String API_PREFIX="/api/v2";
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
        CompositeHealthMonitor yesMon = new CompositeHealthMonitor(); // simple composite health monitor that is always healthy
        yesMon.addHealthCheck("healthyMcHealthFace", new HealthStatusProvider() {
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
        assertThat(result.getResponse().getContentAsString(), is(ExecutionStatus.RUNNING.toString()));
    }

    @Test
    public void testPutState() throws Exception {

        // Arrange
        when(componentMock.getComponentStatus()).thenReturn(ExecutionStatus.RUNNING);
        // Act
        MvcResult result = mockMvc.perform(put(API_PREFIX + "/state")
                .contentType(MediaType.TEXT_PLAIN)
                .content(ExecutionStatus.PAUSED.toString())
                .accept(MediaType.TEXT_PLAIN))
                .andExpect(status().isOk())
                .andReturn();
        // Assert
        verify(componentMock).pauseComponent();
    }
}