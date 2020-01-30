package com.vmturbo.components.common.diagnostics;

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
import org.mockito.Mockito;
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

/**
 * Unit test for {@link DiagnosticsController}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
// Need clean context to count interactions with mocks properly.
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class DiagnosticsControllerTest {

    /**
     * Test Spring configuration to use.
     */
    @Configuration
    @EnableWebMvc
    static class ContextConfiguration {
        /**
         * Diagnostics controller.
         *
         * @return diagnostics controller
         */
        @Bean
        public DiagnosticsControllerImportable controller() {
            return new DiagnosticsControllerImportable(diagnosticsHandler());
        }

        /**
         * Diagnostics handler.
         *
         * @return diagnostics handler
         */
        @Bean
        public DiagnosticsHandlerImportable diagnosticsHandler() {
            return mock(DiagnosticsHandlerImportable.class);
        }
    }

    private static MockMvc mockMvc;

    @Autowired
    private WebApplicationContext wac;

    private DiagnosticsHandlerImportable handlerMock;

    /**
     * Initializes the tests.
     *
     * @throws Exception on exceptions occur.
     */
    @Before
    public void setup() throws Exception {
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
        handlerMock = wac.getBean(DiagnosticsHandlerImportable.class);
    }

    /**
     * Tests dumping of diagnostics.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testDump() throws Exception {
        final MvcResult getResult =
                mockMvc.perform(get("/internal-state").accept("application/zip"))
                        .andExpect(status().isOk())
                        .andReturn();

        verify(handlerMock).dump(any());
    }

    /**
     * Tests restoring of diagnostics.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testRestoreSuccess() throws Exception {
        when(handlerMock.restore(any())).thenReturn("success");
        final byte[] content = new byte[]{1};

        final MvcResult postResult = mockMvc.perform(
                post("/internal-state").contentType("application/zip")
                        .content(content)
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();
        assertEquals("success", postResult.getResponse().getContentAsString());
        verify(handlerMock).restore(any());
    }

    /**
     * Tests error while restoring diagnostics from a dump.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testRestoreError() throws Exception {
        final String errorMsg = "TERRIBLE ERROR";
        Mockito.when(handlerMock.restore(any())).thenThrow(
                new DiagnosticsException(Collections.singletonList(errorMsg)));
        final byte[] content = new byte[]{1};

        final MvcResult postResult = mockMvc.perform(
                post("/internal-state").contentType("application/zip")
                        .content(content)
                        .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().is(HttpStatus.BAD_REQUEST.value()))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();
        assertTrue(postResult.getResponse().getContentAsString().contains(errorMsg));
        verify(handlerMock).restore(any());
    }
}