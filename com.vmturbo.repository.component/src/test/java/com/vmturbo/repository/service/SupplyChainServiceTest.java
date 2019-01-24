package com.vmturbo.repository.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.HashMultimap;

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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.ArrayOidSet;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.executor.ReactiveGraphDBExecutor;
import com.vmturbo.repository.graph.parameter.GraphCmd.GetGlobalSupplyChain;
import com.vmturbo.repository.graph.result.ImmutableGlobalSupplyChainFluxResult;
import com.vmturbo.repository.graph.result.SupplyChainOidsGroup;
import com.vmturbo.repository.service.SupplyChainServiceTest.TestConfig;
import com.vmturbo.repository.topology.TopologyDatabase;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
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
        final SupplyChainOidsGroup vms = new SupplyChainOidsGroup(VMS, "ACTIVE", Arrays.asList(1L, 2L, 3L, 4L));
        final SupplyChainOidsGroup hosts = new SupplyChainOidsGroup(HOSTS, "ACTIVE", Arrays.asList(5L, 6L, 7L));

        final Flux<SupplyChainOidsGroup> typesAndOids = Flux.fromIterable(Arrays.asList(vms, hosts));
        final ImmutableGlobalSupplyChainFluxResult supplyChainFluxResult =
            ImmutableGlobalSupplyChainFluxResult.builder().entities(typesAndOids).build();
        Mockito.when(testConfig.reactiveExecutor()
            .executeGlobalSupplyChainCmd(Mockito.any(GetGlobalSupplyChain.class)))
            .thenReturn(supplyChainFluxResult);

        Mockito.when(testConfig.userSessionContext().getUserAccessScope())
                .thenReturn(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE);

        final Mono<Map<String, SupplyChainNode>> globalSupplyChain =
            testConfig.supplyChainService().getGlobalSupplyChain(Optional.empty(),
                    Optional.empty(), Collections.emptySet());
        final Map<String, SupplyChainNode> result = globalSupplyChain.toFuture().get();
        Assert.assertEquals(2, result.size());
        assertThat(RepositoryDTOUtil.getAllMemberOids(result.get(VMS)),
                containsInAnyOrder(vms.getOids().toArray()));
        assertThat(RepositoryDTOUtil.getAllMemberOids(result.get(HOSTS)),
                containsInAnyOrder(hosts.getOids().toArray()));
    }

    /**
     * Test configuration to run unit test.
     */
    @Configuration
    public static class TestConfig {
        @Bean
        public TopologyLifecycleManager topologyManager() {
            final TopologyLifecycleManager result = Mockito.mock(TopologyLifecycleManager.class);
            final TopologyDatabase topologyDatabase = Mockito.mock(TopologyDatabase.class);
            Mockito.when(result.getRealtimeDatabase())
                    .thenReturn(Optional.of(topologyDatabase));
            final TopologyID topologyId = new TopologyID(1, 2, TopologyID.TopologyType.SOURCE);
            Mockito.when(result.getRealtimeTopologyId()).thenReturn(Optional.of(topologyId));
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
        public UserSessionContext userSessionContext() {
            return Mockito.mock(UserSessionContext.class);
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
                    graphDefinition(), topologyRelationshipRecorder(), topologyManager(),
                    userSessionContext());
        }
    }
}
