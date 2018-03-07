package com.vmturbo.plan.orchestrator.diagnostics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
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

@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD) //to count mock interactions
public class DiagnosticsControllerTest {

    @Configuration
    @EnableWebMvc
    static class ContextConfiguration {
        @Bean
        public PlanOrchestratorDiagnosticsController controller() {
            return new PlanOrchestratorDiagnosticsController(handler());
        }

        @Bean
        public PlanOrchestratorDiagnosticsHandler handler() {
            return mock(PlanOrchestratorDiagnosticsHandler.class);
        }
    }

    private static MockMvc mockMvc;

    @Autowired
    private WebApplicationContext context;

    private PlanOrchestratorDiagnosticsHandler handlerMock;

    @Before
    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
        handlerMock = context.getBean(PlanOrchestratorDiagnosticsHandler.class);
    }

    @Test
    public void testDump() throws Exception {
        when(handlerMock.dump(any())).thenReturn(Collections.emptyList());

        final MvcResult result = mockMvc.perform(get("/internal-state").accept("application/zip"))
            .andExpect(status().isOk())
            .andReturn();

        verify(handlerMock).dump(any());
    }

    @Test
    public void testGoodRestore() throws Exception {
        when(handlerMock.restore(any())).thenReturn(Collections.emptyList());
        final byte[] payload = new byte[]{1};

        final MvcResult result = mockMvc.perform(post("/internal-state")
                .contentType("application/zip").content(payload)
                .accept(MediaType.APPLICATION_JSON_UTF8))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
            .andReturn();

        assertEquals("Success\n", result.getResponse().getContentAsString());
        verify(handlerMock).restore(any());
    }

    @Test
    public void testBadRestore() throws Exception {
        final String error = "This restore was bad and you should feel bad.";
        when(handlerMock.restore(any())).thenReturn(Collections.singletonList(error));
        final byte[] payload = new byte[]{1};

        final MvcResult result = mockMvc.perform(post("/internal-state")
                .contentType("application/zip").content(payload)
                .accept(MediaType.APPLICATION_JSON_UTF8))
            .andExpect(status().is(HttpStatus.BAD_REQUEST.value()))
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
            .andReturn();

        assertTrue(result.getResponse().getContentAsString().contains(error));

    }
}
