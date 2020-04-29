package com.vmturbo.topology.processor.rest;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Clock;
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

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.rest.TopologyController.SendTopologyResponse;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
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
    private Scheduler scheduler;
    private TopologyHandler topologyHandler;

    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    static class ContextConfiguration extends WebMvcConfigurerAdapter {

        @Bean
        Scheduler scheduler() {
            return Mockito.mock(Scheduler.class);
        }

        @Bean
        EntityStore entityStore() {
            return Mockito.mock(EntityStore.class);
        }

        @Bean
        TopologyHandler topologyHandler() {
            return Mockito.mock(TopologyHandler.class);
        }

        @Bean
        Clock clock() {
            return Clock.systemUTC();
        }

        @Bean
        TopologyController topologyController() {
            return new TopologyController(
                scheduler(),
                topologyHandler(),
                entityStore());
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
        topologyHandler = wac.getBean(TopologyHandler.class);
        scheduler = wac.getBean(Scheduler.class);
        entityStore = wac.getBean(EntityStore.class);
    }

    /**
     * Test that the the send() method in {@link TopologyController} gets invoked
     * and that the expected response text is received.
     * @throws Exception if the POST fails
     */
    @Test
    public void testTopologySend() throws Exception {
        TopologyBroadcastInfo info = Mockito.mock(TopologyBroadcastInfo.class);
        when(info.getEntityCount()).thenReturn(10L);
        when(info.getTopologyContextId()).thenReturn(1L);
        when(info.getTopologyId()).thenReturn(2L);
        when(info.getSerializedTopologySizeBytes()).thenReturn(2L << 10);
        when(topologyHandler.broadcastLatestTopology(any(StitchingJournalFactory.class))).thenReturn(info);

        final MvcResult result = mockMvc.perform(post("/topology/send")
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();
        String contentJson = result.getResponse().getContentAsString();
        SendTopologyResponse response = gson.fromJson(contentJson, SendTopologyResponse.class);
        assertThat(response.message, is("Sent 10 entities"));
        assertThat(response.numberOfEntities, is(10L));
        assertThat(response.topologyContextId, is(1L));
        assertThat(response.topologyId, is(2L));
        assertThat(response.serializedTopologySizeBytes, is(2L << 10));
        assertThat(response.serializedTopologySizeHumanReadable, is("2 KB"));

        Mockito.verify(scheduler).resetBroadcastSchedule();
    }
}
