package com.vmturbo.topology.processor.rest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Clock;
import java.util.Collections;
import java.util.List;

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

import com.google.gson.Gson;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.settings.SettingsManager;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.rest.TopologyController.SendTopologyResponse;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.templates.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.topology.TopologyHandler;

/**
 * Tests for the {@link TopologyController} class.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class TopologyControllerTest {

    private final Gson gson = new Gson();
    private MockMvc mockMvc;
    private EntityStore entityStore;
    private TopoBroadcastManager topoBroadcastManager;
    private Scheduler scheduler;

    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    static class ContextConfiguration extends WebMvcConfigurerAdapter {
        @Bean
        TopoBroadcastManager apiController() {
            return Mockito.mock(TopoBroadcastManager.class);
        }

        @Bean
        Scheduler scheduler() {
            return Mockito.mock(Scheduler.class);
        }

        @Bean
        IdentityProvider identityProvider() {
            return Mockito.mock(IdentityProvider.class);
        }

        @Bean
        EntityStore entityStore() {
            return Mockito.mock(EntityStore.class);
        }

        @Bean
        PolicyManager policyManager() {
            return Mockito.mock(PolicyManager.class);
        }

        @Bean
        DiscoveredGroupUploader discoveredGroupUploader() {
            return Mockito.mock(DiscoveredGroupUploader.class);
        }

        @Bean
        SettingsManager settingsManager() {
            return Mockito.mock(SettingsManager.class);
        }

        @Bean
        TopologyHandler topologyHandler() {
            return new TopologyHandler(0, apiController(), entityStore(),
                identityProvider(), policyManager(),
                discoveredTemplatesNotifier(), discoveredGroupUploader(),
                settingsManager(), Clock.systemUTC());
        }

        @Bean
        TargetStore targetStore() {
            return Mockito.mock(TargetStore.class);
        }

        @Bean
        DiscoveredTemplateDeploymentProfileNotifier discoveredTemplatesNotifier() {
            return Mockito.mock(DiscoveredTemplateDeploymentProfileNotifier.class);
        }

        @Bean
        TopologyController topologyController() {
            return new TopologyController(
                scheduler(),
                topologyHandler(),
                entityStore(),
                policyManager(),
                targetStore()
            );
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

    @Before
    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
        topoBroadcastManager = wac.getBean(TopoBroadcastManager.class);
        scheduler = wac.getBean(Scheduler.class);
        entityStore = wac.getBean(EntityStore.class);

        IdentityProvider identityProvider = wac.getBean(IdentityProvider.class);
        Mockito.when(identityProvider.generateTopologyId()).thenReturn(0L);
    }

    /**
     * Test that the the send() method in {@link TopologyController} gets invoked
     * and that the expected response text is received.
     * @throws Exception if the POST fails
     */
    @Test
    public void testTopologySend() throws Exception {
        final TopologyBroadcast broadcast = Mockito.mock(TopologyBroadcast.class);
        Mockito.when(entityStore.constructTopology()).thenReturn(Collections.emptyMap());
        Mockito.when(topoBroadcastManager.broadcastTopology(any())).thenReturn(broadcast);

        final MvcResult result = mockMvc.perform(post("/topology/send")
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();
        String contentJson = result.getResponse().getContentAsString();
        SendTopologyResponse response = gson.fromJson(contentJson, SendTopologyResponse.class);
        assertEquals("Sent 0 entities", response.message);
        assertEquals(0, response.numberOfEntities);
        assertEquals(0, response.topologyContextId);

        Mockito.verify(topoBroadcastManager).broadcastTopology(Mockito.any());
        Mockito.verify(broadcast).finish();
        Mockito.verify(broadcast, Mockito.never()).append(any(TopologyEntityDTO.class));
        Mockito.verify(scheduler).resetBroadcastSchedule();
    }
}
