package com.vmturbo.repository.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import javaslang.control.Either;

import com.vmturbo.repository.ComponentStartUpManager;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.service.GraphDBService;
import com.vmturbo.repository.topology.TopologyID.TopologyType;

@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@DirtiesContext
@TestPropertySource(
    properties = {"instance_id = repository", "actionOrchestratorHost = fake-host",
        "actionOrchestratorPort = 0", "topologyProcessorHost = fake-host",
        "topologyProcessorPort = 0", "marketHost = fake-host", "marketPort = 0",
        "realtimeTopologyContextId = 0", "grpcPingIntervalSeconds = 60"})
@Ignore("TODO: Re-enable after updating spring configuration for this component. Fails now due to Spring configuration issues.")
public class GraphServiceEntityControllerTest {

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ComponentStartUpManager componentStartUpManager;

    @Autowired
    private GraphDBService graphDBService;

    @Autowired
    private ArangoDatabaseFactory arangoDatabaseFactory;

    @Ignore("TODO: Re-enable after updating spring configuration for this component. Fails now due to Spring configuration issues.")
    @Test
    public void testServiceEntitiesOIDSearchURLParsing() throws Exception {

        final Set<Long> oids = Sets.newHashSet(1L, 2L, 3L);
        final long topologyId = 10;

        given(graphDBService.findMultipleEntities(Optional.of(topologyId), oids, TopologyType.SOURCE))
            .willReturn(Either.right(Collections.emptyList()));

        final MvcResult mvcResult = mvc.perform(
            MockMvcRequestBuilders.post("/repository/" + topologyId + "/serviceentity/query/id")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(oids))
                .accept(MediaType.APPLICATION_JSON_UTF8)).andReturn();

        assertThat(mvcResult.getResponse().getStatus()).isEqualTo(200);
        assertThat(mvcResult.getResponse().getContentAsString()).isEqualTo("[]");
    }
}