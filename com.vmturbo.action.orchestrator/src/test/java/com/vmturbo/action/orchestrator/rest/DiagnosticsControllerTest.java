package com.vmturbo.action.orchestrator.rest;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipOutputStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
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

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

import com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.diagnostics.ActionOrchestratorDiagnostics;
import com.vmturbo.action.orchestrator.diagnostics.ActionOrchestratorDiagnosticsTest;
import com.vmturbo.action.orchestrator.diagnostics.DiagnosticsController;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.diagnostics.DiagnosticsController.RestoreResponse;
import com.vmturbo.action.orchestrator.store.ActionFactory;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.action.orchestrator.store.IActionFactory;
import com.vmturbo.action.orchestrator.store.IActionStoreFactory;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.DiagnosticsWriter;

import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Tests for the {@link DiagnosticsController}. Mainly to test
 * the REST plumbing, the majority of the special cases are in
 * {@link ActionOrchestratorDiagnosticsTest}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
public class DiagnosticsControllerTest {

    private static final Gson GSON = ComponentGsonFactory.createGson();

    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    static class ContextConfiguration extends WebMvcConfigurerAdapter {

        @Override
        public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
            final GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
            msgConverter.setGson(GSON);

            // Since GET /internal-state returns a plain string, need a StringHttpMessageConverter.
            final StringHttpMessageConverter stringConverter = new StringHttpMessageConverter();

            converters.add(msgConverter);
            converters.add(stringConverter);
            converters.add(new ByteArrayHttpMessageConverter());
            converters.add(new ResourceHttpMessageConverter());
        }

        @Bean
        public ActionStorehouse actionStorehouse() {
            return mock(ActionStorehouse.class);
        }

        @Bean
        public ActionExecutor actionExecutor() {
            return mock(ActionExecutor.class);
        }

        @Bean
        public IActionFactory actionFactory() {
            return new ActionFactory(actionModeCalculator());
        }

        @Bean
        public ActionModeCalculator actionModeCalculator() {
            return mock(ActionModeCalculator.class);
        }

        @Bean
        public ActionOrchestratorDiagnostics diagnostics() {
            return Mockito.spy(new ActionOrchestratorDiagnostics(actionStorehouse(),
                    actionFactory(), diagnosticsWriter(), actionModeCalculator()));
        }

        @Bean
        public DiagnosticsWriter diagnosticsWriter() {
            return new DiagnosticsWriter();
        }

        @Bean
        public DiagnosticsController diagnosticsController() {
            return new DiagnosticsController(diagnostics());
        }
    }

    private static MockMvc mockMvc;

    private ActionStorehouse actionStorehouse;
    private IActionFactory actionFactory;

    private ActionStore actionStore = mock(ActionStore.class);
    private EntitySeverityCache severityCache = mock(EntitySeverityCache.class);
    private final ActionModeCalculator actionModeCalculator = mock(ActionModeCalculator.class);
    private final long realtimeTopologyContextId = 1234L;
    private DiagnosticsWriter diagnosticsWriter;

    @Autowired
    private WebApplicationContext wac;

    @Captor
    private ArgumentCaptor<List<Action>> actionCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();

        actionStorehouse = wac.getBean(ActionStorehouse.class);
        actionFactory = wac.getBean(ActionFactory.class);
        diagnosticsWriter = wac.getBean(DiagnosticsWriter.class);

        Mockito.when(actionStorehouse.getAllStores()).thenReturn(
            ImmutableMap.of(realtimeTopologyContextId, actionStore)
        );
        Mockito.when(actionStorehouse.getStore(realtimeTopologyContextId))
            .thenReturn(Optional.of(actionStore));
        Mockito.when(actionStorehouse.getActionStoreFactory())
            .thenReturn(mock(IActionStoreFactory.class));
    }

    /**
     * Test that the result of the GET call can be processed by the
     * POST call.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetAndPost() throws Exception {
        final Action action = actionFactory.newAction(
                ActionOrchestratorTestUtils.createMoveRecommendation(1), 0L);

        Mockito.when(actionStore.getActions()).thenReturn(ImmutableMap.of(action.getId(), action));
        Mockito.when(actionStore.getEntitySeverityCache()).thenReturn(mock(EntitySeverityCache.class));

        getThenPost();

        Mockito.verify(actionStore).overwriteActions(actionCaptor.capture());

        final List<Action> deserializedActions = actionCaptor.getValue();
        Assert.assertEquals(1, deserializedActions.size());
        final Action deserializedAction = deserializedActions.get(0);

        ActionOrchestratorTestUtils.assertActionsEqual(action, deserializedAction);
    }

    /**
     * Make sure posting garbage returns a "Bad Request".
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testPostNonZipFile() throws Exception {
        final MvcResult postResult = mockMvc.perform(post("/internal-state")
                .contentType("application/zip").content("blahblahblah".getBytes())
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();

        final RestoreResponse restoreResponse = GSON.fromJson(
                postResult.getResponse().getContentAsString(),
                RestoreResponse.class);
        Assert.assertNotNull(restoreResponse.error);
    }

    /**
     * Make sure posting a zip file with the wrong JSON content
     * returns a "Bad Request".
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testPostZipFileWithWrongJson() throws Exception {
        final String wrongJson = "[{ \"key\" : \"value\" }]";

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final ZipOutputStream zipOutputStream = new ZipOutputStream(byteArrayOutputStream);
        diagnosticsWriter.writeZipEntry(ActionOrchestratorDiagnostics.SERIALIZED_FILE_NAME,
                Collections.singletonList(wrongJson), zipOutputStream);
        zipOutputStream.close();

        final MvcResult postResult = mockMvc.perform(post("/internal-state")
                .contentType("application/zip").content(byteArrayOutputStream.toByteArray())
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();
        final RestoreResponse restoreResponse = GSON.fromJson(
                postResult.getResponse().getContentAsString(),
                RestoreResponse.class);
        Assert.assertNotNull(restoreResponse.error);
    }

    /**
     * Make sure posting a zip file with the wrong JSON content
     * returns a "Bad Request".
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testPostZipFileWithWhenFailsToDeserialize() throws Exception {
        final String wrongJson = "{ \"key\" : \"value\" }";

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final ZipOutputStream zipOutputStream = new ZipOutputStream(byteArrayOutputStream);
        diagnosticsWriter.writeZipEntry(ActionOrchestratorDiagnostics.SERIALIZED_FILE_NAME, Collections.singletonList(wrongJson), zipOutputStream);
        zipOutputStream.close();

        final MvcResult postResult = mockMvc.perform(post("/internal-state")
                .contentType("application/zip").content(byteArrayOutputStream.toByteArray())
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();
        final RestoreResponse restoreResponse = GSON.fromJson(
                postResult.getResponse().getContentAsString(),
                RestoreResponse.class);
        Assert.assertNotNull(restoreResponse.error);
    }

    private void getThenPost() throws Exception {
        final MvcResult getResult = mockMvc.perform(get("/internal-state")
                .accept("application/zip"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/zip"))
                .andReturn();

        final byte[] content = getResult.getResponse().getContentAsByteArray();

        final MvcResult postResult = mockMvc.perform(post("/internal-state")
                .contentType("application/zip").content(content)
                .accept(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();

        final RestoreResponse restoreResponse = GSON.fromJson(
                postResult.getResponse().getContentAsString(),
                RestoreResponse.class);
        Assert.assertNull(restoreResponse.error);
    }
}
