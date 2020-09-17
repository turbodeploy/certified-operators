package com.vmturbo.repository.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import io.grpc.stub.StreamObserver;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.Spy;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainStatsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainStatsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainGroupBy;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.identity.ArrayOidSet;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology.SourceRealtimeTopologyBuilder;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.graph.search.filter.TopologyFilterFactory;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;

/**
 * Tests the construction of supply chains by the service
 * {@link TopologyGraphSupplyChainRpcService}.
 */
public class TopologyGraphSupplyChainRpcServiceTest {
    private SearchResolver<RepoGraphEntity>
            searchResolver = new SearchResolver<>(new TopologyFilterFactory<RepoGraphEntity>());
    private LiveTopologyStore liveTopologyStore = new LiveTopologyStore(new GlobalSupplyChainCalculator(), searchResolver);
    private final UserSessionContext userSessionContext = Mockito.mock(UserSessionContext.class);

    private final long realTimeContextId = 7L;

    @Spy
    private final SupplyChainStatistician supplyChainStatistician =
            Mockito.mock(SupplyChainStatistician.class);
    private final TopologyGraphSupplyChainRpcService service =
        new TopologyGraphSupplyChainRpcService(
            userSessionContext, liveTopologyStore, Mockito.mock(ArangoSupplyChainRpcService.class),
            supplyChainStatistician, new SupplyChainCalculator(), realTimeContextId);

    private static final long VM_ID = 1L;
    private static final long REG_ID = 2L;
    private static final long ACC_ID = 3L;
    private static final long ONPREM_VM_ID = 4L;
    private static final long ONPREM_IDLE_VM_ID = 7L;
    private static final long HYBRID_VM_ID = 5L;
    private static final long NON_EXISTENT_ID = 100L;

    /**
     * Set up a cloud topology with a VM, a region, and a business account.
     * Add an on-prem and a hybrid VM that are unrelated to the rest of
     * the topology.
     */
    @Before
    public void setUp() {
        final SourceRealtimeTopologyBuilder topologyBuilder =
                liveTopologyStore.newRealtimeSourceTopology(TopologyInfo.getDefaultInstance());
        topologyBuilder.addEntities(ImmutableList.of(
            TopologyEntityDTO.newBuilder()
                .setOid(VM_ID)
                .setDisplayName("vm")
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                                            .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                                            .setConnectedEntityType(EntityType.REGION_VALUE)
                                            .setConnectedEntityId(REG_ID))
                .build(),
            TopologyEntityDTO.newBuilder()
                .setOid(REG_ID)
                .setDisplayName("reg")
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.REGION_VALUE)
                .build(),
            TopologyEntityDTO.newBuilder()
                .setOid(ACC_ID)
                .setDisplayName("acc")
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                                            .setConnectionType(ConnectionType.OWNS_CONNECTION)
                                            .setConnectedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                                            .setConnectedEntityId(VM_ID))
                .build(),
            TopologyEntityDTO.newBuilder()
                .setOid(ONPREM_VM_ID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setDisplayName("onpremvm")
                    .setEntityState(EntityState.POWERED_ON)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .build(),
            TopologyEntityDTO.newBuilder()
                .setOid(ONPREM_IDLE_VM_ID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setDisplayName("inactiveOnPremVm")
                .setEntityState(EntityState.POWERED_OFF)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .build(),
            TopologyEntityDTO.newBuilder()
                .setOid(HYBRID_VM_ID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setDisplayName("hybridvm")
                .setEnvironmentType(EnvironmentType.HYBRID)
                .build()));
        topologyBuilder.finish();

        mockUserScope(VM_ID, REG_ID, ACC_ID);
    }

    /**
     * Tests that global supply chain creation works.
     * The account shouldn't be pulled in.
     *
     * @throws InterruptedException should not happen
     */
    @Test
    public void testGlobalSupplyChain() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final SimpleStreamObserver<GetSupplyChainResponse> responseObserver =
                new SimpleStreamObserver<>(latch);
        service.getSupplyChain(GetSupplyChainRequest.newBuilder()
                        .setContextId(realTimeContextId)
                        .build(),
                responseObserver);
        latch.await();

        Assert.assertFalse(responseObserver.isFailure());
        Assert.assertEquals(1, responseObserver.getResults().size());

        assertCorrectnessOfSupplyChainResponse(responseObserver.getResults().get(0), true, true, false);
    }

    /**
     * Test getting the global supply chain filtered by entity state.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testGlobalSupplyChainWithEntityState() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final SimpleStreamObserver<GetSupplyChainResponse> responseObserver =
            new SimpleStreamObserver<>(latch);
        service.getSupplyChain(GetSupplyChainRequest.newBuilder()
            .setContextId(realTimeContextId)
            .setScope(SupplyChainScope.newBuilder()
                .addEntityStatesToInclude(EntityState.POWERED_OFF))
            .build(), responseObserver);
        latch.await();

        Assert.assertFalse(responseObserver.isFailure());
        Assert.assertEquals(1, responseObserver.getResults().size());

        SupplyChain supplyChain = responseObserver.getResults().get(0).getSupplyChain();
        assertThat(supplyChain.getSupplyChainNodesList().size(), is(1));
        SupplyChainNode node = supplyChain.getSupplyChainNodes(0);
        assertThat(node.getEntityType(), is(ApiEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(RepositoryDTOUtil.getAllMemberOids(node), containsInAnyOrder(ONPREM_IDLE_VM_ID));
    }

    /**
     * Test getting a scoped supply chain filtered by entity state.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testScopedSupplyChainWithEntityState() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final SimpleStreamObserver<GetSupplyChainResponse> responseObserver =
                new SimpleStreamObserver<>(latch);
        service.getSupplyChain(GetSupplyChainRequest.newBuilder()
                .setContextId(realTimeContextId)
                .setScope(SupplyChainScope.newBuilder()
                    .addStartingEntityOid(ONPREM_IDLE_VM_ID)
                    .addStartingEntityOid(ONPREM_VM_ID)
                    .addEntityStatesToInclude(EntityState.POWERED_OFF))
                .build(), responseObserver);
        latch.await();

        Assert.assertFalse(responseObserver.isFailure());
        Assert.assertEquals(1, responseObserver.getResults().size());

        SupplyChain supplyChain = responseObserver.getResults().get(0).getSupplyChain();
        assertThat(supplyChain.getSupplyChainNodesList().size(), is(1));
        SupplyChainNode node = supplyChain.getSupplyChainNodes(0);
        assertThat(node.getEntityType(), is(ApiEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(RepositoryDTOUtil.getAllMemberOids(node), containsInAnyOrder(ONPREM_IDLE_VM_ID));

    }

    /**
     * Tests that global supply chain creation works with environment filtering.
     * Filter for cloud: neither the account nor the on-prem VM shouldn't be pulled in.
     *
     * @throws InterruptedException should not happen
     */
    @Test
    public void testGlobalSupplyChainCloudOnly() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final SimpleStreamObserver<GetSupplyChainResponse> responseObserver =
                new SimpleStreamObserver<>(latch);
        service.getSupplyChain(GetSupplyChainRequest.newBuilder()
                        .setContextId(realTimeContextId)
                        .setScope(SupplyChainScope.newBuilder()
                                        .setEnvironmentType(EnvironmentType.CLOUD))
                        .build(),
                responseObserver);
        latch.await();

        Assert.assertFalse(responseObserver.isFailure());
        Assert.assertEquals(1, responseObserver.getResults().size());

        assertCorrectnessOfSupplyChainResponse(responseObserver.getResults().get(0), false, true, false);
    }

    /**
     * Tests that global supply chain request includes all available entities
     * when "filterForDisplay" is disabled.
     *
     * @throws InterruptedException should not happen
     */
    @Test
    public void testGlobalSupplyChainNoFiltering() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final SimpleStreamObserver<GetSupplyChainResponse> responseObserver =
                new SimpleStreamObserver<>(latch);
        service.getSupplyChain(GetSupplyChainRequest.newBuilder()
                        .setContextId(realTimeContextId)
                        .setFilterForDisplay(false)
                        .build(),
                responseObserver);
        latch.await();

        Assert.assertFalse(responseObserver.isFailure());
        Assert.assertEquals(1, responseObserver.getResults().size());

        assertCorrectnessOfSupplyChainResponse(responseObserver.getResults().get(0), true, true, true);
    }

    /**
     * Tests that scoped supply chain creation works.
     * The account shouldn't be pulled in.
     *
     * @throws InterruptedException should not happen
     */
    @Test
    public void testScopedSupplyChain() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final SimpleStreamObserver<GetSupplyChainResponse> responseObserver =
                new SimpleStreamObserver<>(latch);
        service.getSupplyChain(GetSupplyChainRequest.newBuilder()
                                    .setContextId(realTimeContextId)
                                    .setScope(SupplyChainScope.newBuilder()
                                                    .addStartingEntityOid(VM_ID))
                                    .build(),
                                responseObserver);
        latch.await();

        Assert.assertFalse(responseObserver.isFailure());
        Assert.assertEquals(1, responseObserver.getResults().size());

        assertCorrectnessOfSupplyChainResponse(responseObserver.getResults().get(0), false, false, false);
    }

    /**
     * Tests that a scoped supply chain request w/filtering disabled will retrieve the BusinessAccount.
     *
     * @throws InterruptedException should not happen
     */
    @Test
    public void testScopedSupplyChainNoFiltering() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final SimpleStreamObserver<GetSupplyChainResponse> responseObserver =
                new SimpleStreamObserver<>(latch);
        service.getSupplyChain(GetSupplyChainRequest.newBuilder()
                        .setContextId(realTimeContextId)
                        .setFilterForDisplay(false)
                        .setScope(SupplyChainScope.newBuilder()
                                .addStartingEntityOid(VM_ID))
                        .build(),
                responseObserver);
        latch.await();

        Assert.assertFalse(responseObserver.isFailure());
        Assert.assertEquals(1, responseObserver.getResults().size());

        assertCorrectnessOfSupplyChainResponse(responseObserver.getResults().get(0), false, false, true);
    }

    /**
     * Tests that supply chain creation works, when scoped on account works.
     * The account shouldn't be pulled in.
     *
     * @throws InterruptedException should not happen
     */
    @Test
    public void testAccountScopedSupplyChain() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final SimpleStreamObserver<GetSupplyChainResponse> responseObserver =
                new SimpleStreamObserver<>(latch);
        service.getSupplyChain(GetSupplyChainRequest.newBuilder()
                        .setContextId(realTimeContextId)
                        .setScope(SupplyChainScope.newBuilder()
                                        .addStartingEntityOid(ACC_ID))
                        .build(),
                responseObserver);
        latch.await();

        Assert.assertFalse(responseObserver.isFailure());
        Assert.assertEquals(1, responseObserver.getResults().size());

        assertCorrectnessOfSupplyChainResponse(responseObserver.getResults().get(0), false, false, false);
    }

    /**
     * Tests that scoped supply chain creation works properly
     * when non-existing ids exist in the scope.
     *
     * @throws InterruptedException should not happen
     */
    @Test
    public void testScopedSupplyChainNonExistingEntity() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final SimpleStreamObserver<GetSupplyChainResponse> responseObserver =
                new SimpleStreamObserver<>(latch);
        service.getSupplyChain(GetSupplyChainRequest.newBuilder()
                                    .setContextId(realTimeContextId)
                                    .setScope(SupplyChainScope.newBuilder()
                                                    .addStartingEntityOid(NON_EXISTENT_ID))
                                    .build(),
                                responseObserver);
        latch.await();

        final GetSupplyChainResponse response = responseObserver.getResults().get(0);
        Assert.assertFalse(responseObserver.isFailure());
        Assert.assertEquals(0, response.getSupplyChain().getSupplyChainNodesCount());
    }

    /**
     * Tests environment type filtering. Filtering the cloud topology
     * by on-prem will return nothing.
     *
     * @throws InterruptedException should not happen
     */
    @Test
    public void testScopedSupplyChainEnvironmentFiltering1() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final SimpleStreamObserver<GetSupplyChainResponse> responseObserver =
                new SimpleStreamObserver<>(latch);
        service.getSupplyChain(GetSupplyChainRequest.newBuilder()
                        .setContextId(realTimeContextId)
                        .setScope(SupplyChainScope.newBuilder()
                                        .setEnvironmentType(EnvironmentType.ON_PREM)
                                        .addStartingEntityOid(REG_ID))
                        .build(),
                responseObserver);
        latch.await();

        final GetSupplyChainResponse response = responseObserver.getResults().get(0);
        Assert.assertFalse(responseObserver.isFailure());
        Assert.assertEquals(0, response.getSupplyChain().getSupplyChainNodesCount());
    }

    /**
     * Tests environment type filtering. Filtering the cloud topology
     * by hybrid will return the whole cloud topology.
     *
     * @throws InterruptedException should not happen
     */
    @Test
    public void testScopedSupplyChainEnvironmentFiltering2() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final SimpleStreamObserver<GetSupplyChainResponse> responseObserver =
                new SimpleStreamObserver<>(latch);
        service.getSupplyChain(GetSupplyChainRequest.newBuilder()
                        .setContextId(realTimeContextId)
                        .setScope(SupplyChainScope.newBuilder()
                                    .setEnvironmentType(EnvironmentType.HYBRID)
                                    .addStartingEntityOid(REG_ID))
                        .build(),
                responseObserver);
        latch.await();

        Assert.assertFalse(responseObserver.isFailure());
        Assert.assertEquals(1, responseObserver.getResults().size());

        assertCorrectnessOfSupplyChainResponse(responseObserver.getResults().get(0), false, false, false);
    }

    /**
     * Tests environment type filtering. Filtering the hybrid VM
     * by on-prem will return the hybrid VM.
     *
     * @throws InterruptedException should not happen
     */
    @Test
    public void testScopedSupplyChainEnvironmentFiltering3() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final SimpleStreamObserver<GetSupplyChainResponse> responseObserver =
                new SimpleStreamObserver<>(latch);
        service.getSupplyChain(GetSupplyChainRequest.newBuilder()
                        .setContextId(realTimeContextId)
                        .setScope(SupplyChainScope.newBuilder()
                                .setEnvironmentType(EnvironmentType.ON_PREM)
                                .addStartingEntityOid(HYBRID_VM_ID))
                        .build(),
                responseObserver);
        latch.await();

        Assert.assertFalse(responseObserver.isFailure());
        Assert.assertEquals(1, responseObserver.getResults().size());

        final GetSupplyChainResponse response = responseObserver.getResults().get(0);
        Assert.assertEquals(1, response.getSupplyChain().getSupplyChainNodesCount());
        Assert.assertThat(response.getSupplyChain().getSupplyChainNodesList().stream()
                                .map(SupplyChainNode::getEntityType)
                                .collect(Collectors.toList()),
                          containsInAnyOrder(ApiEntityType.fromType(EntityType.VIRTUAL_MACHINE_VALUE)
                                                .apiStr()));
        Assert.assertEquals(Collections.singletonList(HYBRID_VM_ID),
                            response.getSupplyChain().getSupplyChainNodes(0)
                                .getMembersByStateMap().values().stream()
                                    .flatMap(list -> list.getMemberOidsList().stream())
                                    .collect(Collectors.toList()));
    }

    /**
     * Tests that types can be filtered out in scoped supply chain.
     * If only regions are requested, only the region should be returned,
     * starting from the VM.
     *
     * @throws InterruptedException should not happen
     */
    @Test
    public void testScopedSupplyChainFilterTypes() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final SimpleStreamObserver<GetSupplyChainResponse> responseObserver =
                new SimpleStreamObserver<>(latch);
        service.getSupplyChain(GetSupplyChainRequest.newBuilder()
                                    .setContextId(realTimeContextId)
                                    .setScope(SupplyChainScope.newBuilder()
                                                .addStartingEntityOid(VM_ID)
                                                .addEntityTypesToInclude(
                                                    ApiEntityType.REGION.apiStr()))
                                    .build(),
                               responseObserver);
        latch.await();

        final GetSupplyChainResponse response = responseObserver.getResults().get(0);
        Assert.assertEquals(1, response.getSupplyChain().getSupplyChainNodesCount());
        Assert.assertThat(response.getSupplyChain().getSupplyChainNodesList().stream()
                                .map(SupplyChainNode::getEntityType)
                                .collect(Collectors.toList()),
                          containsInAnyOrder(ApiEntityType.fromType(EntityType.REGION_VALUE).apiStr()));
    }

    /**
     * Checks multiple requests.
     *
     * @throws InterruptedException should not happen
     */
    @Test
    public void testMultipleSupplyChainRequest() throws InterruptedException {
        mockUserScope(VM_ID, NON_EXISTENT_ID);

        final CountDownLatch latch = new CountDownLatch(1);
        final SimpleStreamObserver<GetMultiSupplyChainsResponse> responseObserver =
                new SimpleStreamObserver<>(latch);
        service.getMultiSupplyChains(GetMultiSupplyChainsRequest.newBuilder()
                                        .setContextId(realTimeContextId)
                                        .addSeeds(SupplyChainSeed.newBuilder()
                                                        .setSeedOid(1)
                                                        .setScope(SupplyChainScope.newBuilder()
                                                                    .addStartingEntityOid(VM_ID)))
                                        .addSeeds(SupplyChainSeed.newBuilder()
                                                        .setSeedOid(2)
                                                        .setScope(SupplyChainScope.newBuilder()
                                                                    .addStartingEntityOid(NON_EXISTENT_ID)))
                                        .build(),
                                     responseObserver);
        latch.await();

        Assert.assertFalse(responseObserver.isFailure());
        Assert.assertEquals(2, responseObserver.getResults().size());

        final GetMultiSupplyChainsResponse response0 = responseObserver.getResults().get(0);
        final GetMultiSupplyChainsResponse response1 = responseObserver.getResults().get(1);
        Assert.assertEquals(2, response0.getSupplyChain().getSupplyChainNodesCount());
        Assert.assertThat(response0.getSupplyChain().getSupplyChainNodesList().stream()
                                    .map(SupplyChainNode::getEntityType)
                                    .collect(Collectors.toList()),
                          containsInAnyOrder(ApiEntityType.fromType(EntityType.REGION_VALUE).apiStr(),
                                             ApiEntityType.fromType(EntityType.VIRTUAL_MACHINE_VALUE)
                                                    .apiStr()));
        Assert.assertEquals(0, response1.getSupplyChain().getSupplyChainNodesCount());
    }

    private void mockUserScope(Long... ids) {
        final EntityAccessScope entityAccessScope =
                new EntityAccessScope(null, new ArrayOidSet(Arrays.stream(ids).collect(Collectors.toSet())),
                                      null, null);
        Mockito.when(userSessionContext.getUserAccessScope()).thenReturn(entityAccessScope);
    }

    /**
     * Test getting supply chain stats via the {@link SupplyChainStatistician}.
     *
     * @throws InterruptedException should not happen
     */
    @Test
    public void testSupplyChainStats() throws InterruptedException {
        // ARRANGE
        final CountDownLatch latch1 = new CountDownLatch(1);
        final SimpleStreamObserver<GetSupplyChainResponse> responseObserver1 =
                new SimpleStreamObserver<>(latch1);
        service.getSupplyChain(GetSupplyChainRequest.newBuilder()
                                    .setScope(SupplyChainScope.newBuilder()
                                                .setEnvironmentType(EnvironmentType.CLOUD)
                                                .addStartingEntityOid(REG_ID))
                                    .setContextId(realTimeContextId)
                                    .setScope(SupplyChainScope.newBuilder())
                                    .build(),
                                responseObserver1);
        latch1.await();
        final SupplyChain supplyChain = responseObserver1.getResults().get(0).getSupplyChain();

        // ACT
        final CountDownLatch latch2 = new CountDownLatch(1);
        final SimpleStreamObserver<GetSupplyChainStatsResponse> responseObserver2 =
                new SimpleStreamObserver<>(latch2);
        service.getSupplyChainStats(GetSupplyChainStatsRequest.newBuilder()
                                        .setScope(SupplyChainScope.getDefaultInstance())
                                        .addGroupBy(SupplyChainGroupBy.ENTITY_TYPE)
                                        .build(),
                                    responseObserver2);
        latch2.await();

        // ASSERT
        Mockito.verify(supplyChainStatistician)
               .calculateStats(Mockito.eq(supplyChain),
                               Mockito.eq(Collections.singletonList(SupplyChainGroupBy.ENTITY_TYPE)),
                               Mockito.any(),
                               Mockito.anyLong());
    }

    /**
     * A stream observer that keeps the values sent on a stream in a list.
     *
     * @param <T> type of the values sent on the stream
     */
    private static class SimpleStreamObserver<T> implements StreamObserver<T> {
        private final List<T> results = new ArrayList<>();
        private boolean failure = false;
        private final CountDownLatch countDownLatch;

        /**
         * Create a {@link SimpleStreamObserver} object.
         *
         * @param countDownLatch latch to signal completion
         */
        SimpleStreamObserver(@Nonnull CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void onNext(T t) {
            results.add(t);
        }

        @Override
        public void onError(Throwable throwable) {
            failure = true;
        }

        @Override
        public void onCompleted() {
            countDownLatch.countDown();
        }

        public List<T> getResults() {
            return results;
        }

        public boolean isFailure() {
            return failure;
        }
    }

    private void assertCorrectnessOfSupplyChainResponse(@Nonnull GetSupplyChainResponse response,
                                                        boolean onpremVmIncluded, boolean hybridVmIncluded,
                                                        boolean accountIncluded) {
        final Map<String, Set<Long>> supplyChainEntityIdsPerEntityType =
            response.getSupplyChain().getSupplyChainNodesList().stream()
                .collect(Collectors.toMap(
                    SupplyChainNode::getEntityType,
                    supplyChainNode -> supplyChainNode.getMembersByStateMap().values().stream()
                                            .flatMap(l -> l.getMemberOidsList().stream())
                                            .collect(Collectors.toSet())));
        // region is included
        Assert.assertEquals(
            Collections.singleton(REG_ID),
            supplyChainEntityIdsPerEntityType.get(ApiEntityType.fromType(EntityType.REGION_VALUE).apiStr()));

        // check account
        if (accountIncluded) {
            Assert.assertEquals(
                Collections.singleton(ACC_ID),
                supplyChainEntityIdsPerEntityType.get(ApiEntityType.fromType(
                                                        EntityType.BUSINESS_ACCOUNT_VALUE).apiStr()));
        } else {
            Assert.assertFalse(supplyChainEntityIdsPerEntityType.containsKey(ApiEntityType.fromType(
                                    EntityType.BUSINESS_ACCOUNT_VALUE).apiStr()));
        }

        // check VMs
        final Set<Long> vmIds = supplyChainEntityIdsPerEntityType.get(
                                    ApiEntityType.fromType(EntityType.VIRTUAL_MACHINE_VALUE).apiStr());
        Assert.assertTrue(vmIds.contains(VM_ID));
        Assert.assertEquals(onpremVmIncluded, vmIds.contains(ONPREM_VM_ID));
        Assert.assertEquals(hybridVmIncluded, vmIds.contains(HYBRID_VM_ID));
    }
}
