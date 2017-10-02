package com.vmturbo.repository.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.vmturbo.repository.ComponentStartUpManager;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.service.GraphDBService;
import javaslang.control.Either;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

@RunWith(SpringRunner.class)
@WebMvcTest(GraphServiceEntityController.class)
@DirtiesContext
@TestPropertySource(
    properties = {"instance_id = repository", "actionOrchestratorHost = fake-host",
        "actionOrchestratorPort = 0", "topologyProcessorHost = fake-host",
        "topologyProcessorPort = 0", "marketHost = fake-host", "marketPort = 0",
        "realtimeTopologyContextId = 0", "grpcPingIntervalSeconds = 60"})
public class GraphServiceEntityControllerTest {

    @Autowired
    MockMvc mvc;

    @Autowired
    ObjectMapper objectMapper;

    @MockBean
    DiscoveryClient discoveryClient;

    @MockBean
    ComponentStartUpManager componentStartUpManager;

    @MockBean
    GraphDBService graphDBService;

    @MockBean
    ArangoDatabaseFactory arangoDatabaseFactory;

    @Ignore("TODO: Re-enable after updating spring configuration for this component. Fails now due to Spring configuration issues.")
    @Test
    public void testServiceEntitiesOIDSearchURLParsing() throws Exception {

        final Set<Long> oids = Sets.newHashSet(1L, 2L, 3L);
        final String topologyId = "my-topo-id";

        given(graphDBService.findMultipleEntities(Optional.of(topologyId), oids))
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