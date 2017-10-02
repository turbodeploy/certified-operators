package com.vmturbo.repository.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.google.common.collect.HashMultimap;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.executor.ReactiveGraphDBExecutor;
import com.vmturbo.repository.graph.parameter.GraphCmd.GetGlobalSupplyChain;
import com.vmturbo.repository.graph.result.ImmutableGlobalSupplyChainFluxResult;
import com.vmturbo.repository.graph.result.TypeAndOids;
import com.vmturbo.repository.service.SupplyChainServiceTest.TestConfig;
import com.vmturbo.repository.topology.TopologyDatabase;
import com.vmturbo.repository.topology.TopologyIDManager;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyID;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyType;
import com.vmturbo.repository.topology.TopologyRelationshipRecorder;

/**
 * unit test for {@link SupplyChainService}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class, classes = {TestConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class SupplyChainServiceTest {

    private static final String VMS = "vms";
    private static final String HOSTS = "pms";

    @Autowired
    private TestConfig testConfig;

    /**
     * Tests health status consistency for all the entities reported as normal.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testGlobalSupplyChain() throws Exception {
        final TypeAndOids vms = new TypeAndOids(VMS, Arrays.asList(1L, 2L, 3L, 4L));
        final TypeAndOids hosts = new TypeAndOids(HOSTS, Arrays.asList(5L, 6L, 7L));

        final Flux<TypeAndOids> typesAndOids = Flux.fromIterable(Arrays.asList(vms, hosts));
        final ImmutableGlobalSupplyChainFluxResult supplyChainFluxResult =
            ImmutableGlobalSupplyChainFluxResult.builder().entities(typesAndOids).build();
        Mockito.when(testConfig.reactiveExecutor()
            .executeGlobalSupplyChainCmd(Mockito.any(GetGlobalSupplyChain.class)))
            .thenReturn(supplyChainFluxResult);

        final Mono<Map<String, SupplyChainNode>> globalSupplyChain =
            testConfig.supplyChainService().getGlobalSupplyChain(Optional.empty());
        final Map<String, SupplyChainNode> result = globalSupplyChain.toFuture().get();
        Assert.assertEquals(2, result.size());
        assertThat(result.get(VMS).getMemberOidsList(), containsInAnyOrder(vms.getOids().toArray()));
        assertThat(result.get(HOSTS).getMemberOidsList(), containsInAnyOrder(hosts.getOids().toArray()));
    }

    /**
     * Test configuration to run unit test.
     */
    @Configuration
    public static class TestConfig {
        @Bean
        public TopologyIDManager topologyIDManagerArg() {
            final TopologyIDManager result = Mockito.mock(TopologyIDManager.class);
            final TopologyDatabase topologyDatabase = Mockito.mock(TopologyDatabase.class);
            Mockito.when(result.currentRealTimeDatabase())
                    .thenReturn(Optional.of(topologyDatabase));
            final TopologyID topologyId = new TopologyID(1, 2, TopologyType.SOURCE);
            Mockito.when(result.getCurrentRealTimeTopologyId()).thenReturn(Optional.of(topologyId));
            return result;
        }

        @Bean
        public ReactiveGraphDBExecutor reactiveExecutor() {
            return Mockito.mock(ReactiveGraphDBExecutor.class);
        }

        @Bean
        public GraphDBService graphDbService() {
            return Mockito.mock(GraphDBService.class);
        }

        @Bean
        public GraphDefinition graphDefinition() {
            return Mockito.mock(GraphDefinition.class);
        }

        @Bean
        public TopologyRelationshipRecorder topologyRelationshipRecorder() {
            final TopologyRelationshipRecorder recorder =
                    Mockito.mock(TopologyRelationshipRecorder.class);
            Mockito.when(recorder.getGlobalSupplyChainProviderStructures())
                    .thenReturn(HashMultimap.create());
            return recorder;
        }

        @Bean
        public SupplyChainService supplyChainService() throws IOException {
            return new SupplyChainService(reactiveExecutor(), graphDbService(),
                    graphDefinition(), topologyRelationshipRecorder(), topologyIDManagerArg());
        }
    }
}
