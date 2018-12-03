package com.vmturbo.api.component.controller;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.AnnotationConfigWebContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.vmturbo.api.component.external.api.serviceinterfaces.IProbesService;
import com.vmturbo.api.dto.probe.ProbeApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.handler.GlobalExceptionHandler;

/**
 * Tests for {@link ProbesController}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
public class ProbeControllerTest {
    private MockMvc mockMvc;

    @Configuration
    @EnableWebMvc
    static class TestConfig {
        @Bean
        IProbesService probesService() {
            return mock(IProbesService.class);
        }

        @Bean
        ProbesController probesController() {
            return new ProbesController();
        }

        @Bean
        public GlobalExceptionHandler exceptionHandler() {
            return new GlobalExceptionHandler();
        }
    }

    @Autowired
    private WebApplicationContext wac;

    @Autowired
    private IProbesService probesService;

    @Autowired
    private ProbesController probesController;

    @Before
    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
        reset(probesService);
    }

    /**
     * Legitimate call of /probes/{probeId} endpoint.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetProbe() throws Exception {
        when(probesService.getProbe(any())).thenReturn(new ProbeApiDTO());
        mockMvc
            .perform(get("/probes/100/"))
            .andExpect(status().isOk())
            .andReturn();
        verify(probesService, times(1)).getProbe(any());
    }

    /**
     * Call of /probes/{probeId} but probe does not exist.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetProbeNotFound() throws Exception {
        when(probesService.getProbe(any())).thenThrow(UnknownObjectException.class);
        mockMvc
            .perform(get("/probes/100/"))
            .andExpect(status().isNotFound())
            .andReturn();
        verify(probesService, times(1)).getProbe(any());
    }

    /**
     * Call of /probes/{probeId} but probe id does not parse to a number.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetProbeBadProbeId() throws Exception {
        when(probesService.getProbe(any())).thenThrow(OperationFailedException.class);
        mockMvc
            .perform(get("/probes/x/"))
            .andExpect(status().isBadRequest())
            .andReturn();
        verify(probesService, times(1)).getProbe(any());
    }
}
