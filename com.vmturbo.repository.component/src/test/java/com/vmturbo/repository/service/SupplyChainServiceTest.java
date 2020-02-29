package com.vmturbo.repository.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

import reactor.core.publisher.Mono;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.executor.ReactiveGraphDBExecutor;
import com.vmturbo.repository.graph.result.SupplyChainOidsGroup;
import com.vmturbo.repository.service.SupplyChainServiceTest.TestConfig;
import com.vmturbo.repository.topology.GlobalSupplyChain;
import com.vmturbo.repository.topology.GlobalSupplyChainManager;
import com.vmturbo.repository.topology.TopologyDatabase;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager;

/**
 * unit test for {@link SupplyChainService}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class, classes = {TestConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class SupplyChainServiceTest {

    @Autowired
    private TestConfig testConfig;

    /**
     * Tests health status consistency for all the entities reported as normal.
     *
     * @throws Exception on exception occurred
     */
    @Test
    public void testGlobalSupplyChain() throws Exception {
        final String VMS = "VirtualMachine";
        final String HOSTS = "PhysicalMachine";

        final SupplyChainOidsGroup vms = new SupplyChainOidsGroup(VMS, "ACTIVE", Arrays.asList(1L, 2L, 3L, 4L));
        final SupplyChainOidsGroup hosts = new SupplyChainOidsGroup(HOSTS, "ACTIVE", Arrays.asList(5L, 6L, 7L));

        List<TopologyEntityDTO> chunks = new ArrayList<>();
        chunks.add(createEntityDto(1L, EntityType.VIRTUAL_MACHINE_VALUE));
        chunks.add(createEntityDto(2L, EntityType.VIRTUAL_MACHINE_VALUE));
        chunks.add(createEntityDto(3L, EntityType.VIRTUAL_MACHINE_VALUE));
        chunks.add(createEntityDto(4L, EntityType.VIRTUAL_MACHINE_VALUE));
        chunks.add(createEntityDto(5L, EntityType.PHYSICAL_MACHINE_VALUE));
        chunks.add(createEntityDto(6L, EntityType.PHYSICAL_MACHINE_VALUE));
        chunks.add(createEntityDto(7L, EntityType.PHYSICAL_MACHINE_VALUE));

        Mockito.when(testConfig.userSessionContext().getUserAccessScope())
                .thenReturn(EntityAccessScope.DEFAULT_ENTITY_ACCESS_SCOPE);

        TopologyID topologyID = testConfig.getTopologyId();
        GlobalSupplyChain globalSupplyChain = new GlobalSupplyChain(topologyID, testConfig.graphDBExecutor());
        globalSupplyChain.processEntities(chunks);
        globalSupplyChain.seal();
        Mockito.when(testConfig.globalSupplyChainManager().getGlobalSupplyChain(topologyID))
                .thenReturn(Optional.of(globalSupplyChain));

        final Mono<Map<String, SupplyChainNode>> globalSupplyChainNodes =
                testConfig.supplyChainService().getGlobalSupplyChain(Optional.empty(),
                        Optional.empty(), Collections.emptySet());

        final Map<String, SupplyChainNode> result = globalSupplyChainNodes.toFuture().get();
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
        public TopologyID getTopologyId(){
            return new TopologyID(1, 2, TopologyID.TopologyType.SOURCE);
        }

        @Bean
        public TopologyLifecycleManager topologyManager() {
            final TopologyLifecycleManager result = Mockito.mock(TopologyLifecycleManager.class);
            Mockito.when(result.getRealtimeTopologyId()).thenReturn(Optional.of(getTopologyId()));
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
        public GraphDBExecutor graphDBExecutor() {
            return Mockito.mock(GraphDBExecutor.class);
        }

        @Bean
        public UserSessionContext userSessionContext() {
            return Mockito.mock(UserSessionContext.class);
        }

        @Bean
        public GlobalSupplyChainManager globalSupplyChainManager() {
            return Mockito.mock(GlobalSupplyChainManager.class);
        }

        @Bean
        public SupplyChainService supplyChainService() throws IOException {
            return new SupplyChainService(
                    topologyManager(),
                    globalSupplyChainManager(),
                    userSessionContext());
        }
    }

    private TopologyEntityDTO createEntityDto(Long oid, int entityType) {

        return TopologyEntityDTO.newBuilder()
                .setOid(oid)
                .setEntityType(entityType)
                .setEntityState(EntityState.POWERED_ON)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .build();

    }
}
