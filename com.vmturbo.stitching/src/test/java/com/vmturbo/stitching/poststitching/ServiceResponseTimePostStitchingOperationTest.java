package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommodityBought;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntity;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntityBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * Unit tests for {@link ServiceResponseTimePostStitchingOperation}.
 */
public class ServiceResponseTimePostStitchingOperationTest {

    private final ServiceResponseTimePostStitchingOperation operation =
            new ServiceResponseTimePostStitchingOperation();

    private EntityChangesBuilder<TopologyEntity> resultBuilder;

    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    private final CommoditySoldDTO transactionSold = makeCommoditySold(CommodityType.TRANSACTION);
    private final TopologyEntity serviceWithoutResponseTime =
            makeTopologyEntity(ImmutableList.of(transactionSold));

    private final CommoditySoldDTO responseTimeSoldWithZeroUsed =
            makeCommoditySold(CommodityType.RESPONSE_TIME, "", 0);
    private final TopologyEntity serviceWithZeroResponseTime =
            makeTopologyEntity(ImmutableList.of(responseTimeSoldWithZeroUsed));

    private final CommoditySoldDTO responseTimeSold =
            makeCommoditySold(CommodityType.RESPONSE_TIME, "", 2000);
    private final TopologyEntity serviceWithoutProviders =
            makeTopologyEntity(ImmutableList.of(responseTimeSold));

    private final CommodityBoughtDTO applicationBought = makeCommodityBought(CommodityType.APPLICATION, "key");
    private final CommoditySoldDTO applicationSold = makeCommoditySold(CommodityType.APPLICATION, "key");
    private final List<CommodityBoughtDTO> boughtListWithoutResponseTime = ImmutableList.of(applicationBought);
    private final CommoditiesBoughtFromProvider commBoughtFromAppWithoutResponseTime =
            CommoditiesBoughtFromProvider.newBuilder()
                    .addAllCommodityBought(boughtListWithoutResponseTime)
                    .setProviderEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                    .build();
    private final TopologyEntity.Builder sidecarApp =
            makeTopologyEntityBuilder(EntityType.APPLICATION_COMPONENT_VALUE,
                    ImmutableList.of(applicationSold), Collections.emptyList());
    private final TopologyEntity serviceWithoutBoughtResponseTime =
            makeTopologyEntity(EntityType.SERVICE_VALUE,
                    ImmutableList.of(responseTimeSold),
                    ImmutableSet.of(commBoughtFromAppWithoutResponseTime),
                    ImmutableList.of(sidecarApp));

    private final TopologyEntity.Builder app1 =
            makeTopologyEntityBuilder(EntityType.APPLICATION_COMPONENT_VALUE,
                    ImmutableList.of(responseTimeSold), Collections.emptyList());
    private final TopologyEntity.Builder app2 =
            makeTopologyEntityBuilder(EntityType.APPLICATION_COMPONENT_VALUE,
                    ImmutableList.of(responseTimeSold), Collections.emptyList());
    private final List<CommodityBoughtDTO> boughtList1 =
            ImmutableList.of(makeCommodityBought(CommodityType.RESPONSE_TIME, 500),
                    makeCommodityBought(CommodityType.APPLICATION, "key"));
    private final List<CommodityBoughtDTO> boughtList2 =
            ImmutableList.of(makeCommodityBought(CommodityType.RESPONSE_TIME, 1500),
                    makeCommodityBought(CommodityType.APPLICATION, "key"));
    private final CommoditiesBoughtFromProvider commBoughtFromApp1 =
            CommoditiesBoughtFromProvider.newBuilder()
                    .addAllCommodityBought(boughtList1)
                    .setProviderEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                    .build();
    private final CommoditiesBoughtFromProvider commBoughtFromApp2 =
            CommoditiesBoughtFromProvider.newBuilder()
                    .addAllCommodityBought(boughtList2)
                    .setProviderEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                    .build();
    private final TopologyEntity serviceWithMultipleApps =
            makeTopologyEntity(EntityType.SERVICE_VALUE,
                    ImmutableList.of(responseTimeSold),
                    ImmutableSet.of(commBoughtFromApp1, commBoughtFromApp2, commBoughtFromAppWithoutResponseTime),
                    ImmutableList.of(app1, app2, sidecarApp));

    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> journal =
            (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();
    }

    /**
     * Test that empty scope produces no changes.
     */
    @Test
    public void testNoEntities() {
        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.empty(), settingsMock, resultBuilder);
        assertTrue(result.getChanges().isEmpty());
    }

    /**
     * Test that service without response time sold produces no changes.
     */
    @Test
    public void testServiceWithoutResponseTime() {
        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(serviceWithoutResponseTime), settingsMock, resultBuilder);
        assertTrue(result.getChanges().isEmpty());
    }

    /**
     * Test that service with 0 used response time sold produces no changes.
     */
    @Test
    public void testServiceWithZeroResponseTime() {
        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(serviceWithZeroResponseTime), settingsMock, resultBuilder);
        assertTrue(result.getChanges().isEmpty());
    }

    /**
     * Test that service that does not have providers produces no changes.
     */
    @Test
    public void testServiceWithoutProviders() {
        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(serviceWithoutProviders), settingsMock, resultBuilder);
        assertTrue(result.getChanges().isEmpty());
    }

    /**
     * Test that service that does not buy response time from any of the providers produces no changes.
     */
    @Test
    public void testServiceWithoutBoughtResponseTime() {
        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(serviceWithoutBoughtResponseTime), settingsMock, resultBuilder);
        assertEquals(1, result.getChanges().size());
        // apply the changes
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        // assert that the used value is not changed
        assertEquals(2000, serviceWithoutBoughtResponseTime.soldCommoditiesByType()
                .get(CommodityType.RESPONSE_TIME_VALUE).get(0).getUsed(), 0.00001);
    }

    /**
     * Test that service that buys response time from multiple providers will result in a change
     * to average the response time sold.
     */
    @Test
    public void testServiceWithMultipleApplicationProviders() {
        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(serviceWithMultipleApps), settingsMock, resultBuilder);
        assertEquals(1, result.getChanges().size());
        // apply the changes
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        final Optional<CommoditySoldDTO.Builder> commoditySoldDTO = serviceWithMultipleApps
                .getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
                .filter(commSoldBuilder -> commSoldBuilder.getCommodityType().getType() == CommodityType.RESPONSE_TIME_VALUE)
                .findFirst();
        // assert that the used value is not changed
        assertTrue(commoditySoldDTO.isPresent());
        assertEquals(1000, commoditySoldDTO.get().getUsed(), 0.00001);
    }
}
