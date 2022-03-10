package com.vmturbo.stitching.poststitching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsMoles;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.HistoricalValuesImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.PostStitchingOperationLibrary;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;
import com.vmturbo.stitching.journal.IStitchingJournal;

/**
 * Tests for SetCommBoughtUsedFromCommSoldMaxPostStitchingOperation.
 */
public class SetCommBoughtUsedFromCommSoldMaxPostStitchingOperationTest {

    private static final CommodityTypeView CONNECTION = new CommodityTypeImpl().setType(CommodityType.CONNECTION_VALUE);
    private static final CommodityTypeView VCPU = new CommodityTypeImpl().setType(CommodityType.VCPU_VALUE);
    private static final double DELTA = 1e-5;
    private static final double USED = 100;
    private static final double MAX = 200;
    private static final double CAPACITY1 = 1000;
    private static final double CAPACITY2 = 150;

    private IStitchingJournal<TopologyEntity> journal;
    private TopologicalChangelog.EntityChangesBuilder<TopologyEntity> resultBuilder;
    private SetCommBoughtUsedFromCommSoldMaxPostStitchingOperation setCommBoughtOperation;

    private final StatsMoles.StatsHistoryServiceMole statsRpcSpy = spy(new StatsMoles.StatsHistoryServiceMole());

    /**
     * GRPC test server rule.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(statsRpcSpy);

    /**
     * Setup the test class.
     */
    @Before
    public void setup() {
        setCommBoughtOperation = new SetCommBoughtUsedFromCommSoldMaxPostStitchingOperation();
        resultBuilder = new PostStitchingTestUtilities.UnitTestResultBuilder();
        journal = (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);
    }

    /**
     * When comm sold max is less than capacity, then the comm bought used will be set to the comm sold max.
     */
    @Test
    public void testMaxLessThanCapacity() {
        TopologyEntity dbs1 = topologyEntity(CommonDTO.EntityDTO.EntityType.DATABASE_SERVER_VALUE, 1L,
                ImmutableList.of(commoditySoldDTO(VCPU, USED, Optional.of(MAX), CAPACITY1),
                        commoditySoldDTO(CONNECTION, USED, Optional.of(MAX), CAPACITY1)),
                ImmutableList.of(commodityBoughtDTO(CONNECTION, 100), commodityBoughtDTO(VCPU, 100)));
        List<TopologyEntity> entities = ImmutableList.of(dbs1);
        setCommBoughtOperation.performOperation(entities.stream(), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        Map<CommodityTypeView, CommodityBoughtView> commBoughtByType = dbs1.getTopologyEntityImpl()
                .getCommoditiesBoughtFromProviders(0).getCommodityBoughtList()
                .stream().collect(Collectors.toMap(CommodityBoughtView::getCommodityType, Function.identity()));
        // Only connections commodity is changed
        assertEquals(MAX, commBoughtByType.get(CONNECTION).getUsed(), DELTA);
        assertEquals(USED, commBoughtByType.get(VCPU).getUsed(), DELTA);
    }

    /**
     * When comm sold max is greater than capacity, then the comm bought used will be set to the comm sold capacity.
     */
    @Test
    public void testMaxGreaterThanCapacity() {
        TopologyEntity dbs1 = topologyEntity(CommonDTO.EntityDTO.EntityType.DATABASE_SERVER_VALUE, 1L,
                ImmutableList.of(commoditySoldDTO(VCPU, USED, Optional.of(MAX), CAPACITY2),
                        commoditySoldDTO(CONNECTION, USED, Optional.of(MAX), CAPACITY2)),
                ImmutableList.of(commodityBoughtDTO(CONNECTION, 100), commodityBoughtDTO(VCPU, 100)));
        List<TopologyEntity> entities = ImmutableList.of(dbs1);
        setCommBoughtOperation.performOperation(entities.stream(), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        Map<CommodityTypeView, CommodityBoughtView> commBoughtByType = dbs1.getTopologyEntityImpl()
                .getCommoditiesBoughtFromProviders(0).getCommodityBoughtList()
                .stream().collect(Collectors.toMap(CommodityBoughtView::getCommodityType, Function.identity()));
        // Only connections commodity is changed
        assertEquals(CAPACITY2, commBoughtByType.get(CONNECTION).getUsed(), DELTA);
        assertEquals(USED, commBoughtByType.get(VCPU).getUsed(), DELTA);
    }

    /**
     * When max is not available, no changes should be made.
     */
    @Test
    public void testNoMaxQuantity() {
        TopologyEntity dbs1 = topologyEntity(CommonDTO.EntityDTO.EntityType.DATABASE_SERVER_VALUE, 1L,
                ImmutableList.of(commoditySoldDTO(VCPU, USED, Optional.empty(), CAPACITY2),
                        commoditySoldDTO(CONNECTION, USED, Optional.empty(), CAPACITY2)),
                ImmutableList.of(commodityBoughtDTO(CONNECTION, 100), commodityBoughtDTO(VCPU, 100)));
        List<TopologyEntity> entities = ImmutableList.of(dbs1);
        setCommBoughtOperation.performOperation(entities.stream(), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        Map<CommodityTypeView, CommodityBoughtView> commBoughtByType = dbs1.getTopologyEntityImpl()
                .getCommoditiesBoughtFromProviders(0).getCommodityBoughtList()
                .stream().collect(Collectors.toMap(CommodityBoughtView::getCommodityType, Function.identity()));
        // No changes are made
        assertEquals(USED, commBoughtByType.get(CONNECTION).getUsed(), DELTA);
        assertEquals(USED, commBoughtByType.get(VCPU).getUsed(), DELTA);
    }

    /**
     * Test that SetCommBoughtUsedFromCommSoldMaxPostStitchingOperation is performed after
     * SetCommodityMaxQuantityPostStitchingOperation. If someone changes the order in the future, this test will fail
     * and catch this.
     */
    @Test
    public void testOrderOfOperations() {
        final StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub statsServiceClient =
                StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        PostStitchingOperationLibrary postStitchingOperationLibrary =
                new PostStitchingOperationLibrary(
                        new com.vmturbo.stitching.poststitching.CommodityPostStitchingOperationConfig(
                                statsServiceClient, 30, 0), //meaningless values
                        Mockito.mock(DiskCapacityCalculator.class), mock(CpuCapacityStore.class),  Mockito.mock(Clock.class), 0,
                        mock(SetAutoSetCommodityCapacityPostStitchingOperation.MaxCapacityCache.class), true);
        List<PostStitchingOperation> postStitchingOperations = postStitchingOperationLibrary.getPostStitchingOperations();
        List<String> operations = postStitchingOperations.stream().map(op -> op.getClass().getSimpleName()).collect(Collectors.toList());
        int operation1Index = operations.indexOf(SetCommodityMaxQuantityPostStitchingOperation.class.getSimpleName());
        int operation2Index = operations.indexOf(SetCommBoughtUsedFromCommSoldMaxPostStitchingOperation.class.getSimpleName());
        assertTrue(operation1Index < operation2Index);
    }

    private TopologyEntity topologyEntity(int entityType, long oid,
                                          List<CommoditySoldView> commsSold,
                                          List<CommodityBoughtView> commsBought) {
        return TopologyEntity.newBuilder(new TopologyEntityImpl()
                .setOid(oid)
                .setEntityType(entityType)
                .addAllCommoditySoldList(commsSold)
                .addCommoditiesBoughtFromProviders(commodityBoughtGrouping(commsBought))).build();
    }

    private CommoditySoldView commoditySoldDTO(CommodityTypeView commodityType,
                                              double used, Optional<Double> maxUsed, double capacity) {
        final CommoditySoldImpl commSold = new CommoditySoldImpl()
                .setCommodityType(commodityType)
                .setUsed(used)
                .setCapacity(capacity);
        maxUsed.ifPresent(max -> {
            commSold.setHistoricalUsed(new HistoricalValuesImpl().setMaxQuantity(max));
        });
        return commSold;
    }

    private CommoditiesBoughtFromProviderView commodityBoughtGrouping(List<CommodityBoughtView> commsBought) {
        return new CommoditiesBoughtFromProviderImpl().addAllCommodityBought(commsBought);
    }

    private CommodityBoughtView commodityBoughtDTO(CommodityTypeView commodityType, double used) {
        return new CommodityBoughtImpl().setCommodityType(commodityType).setUsed(used);
    }
}