package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditiesBoughtFromProvider;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommodityBoughtBuilder;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntityBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;
import com.vmturbo.stitching.poststitching.PropagateStorageAccessAndLatencyPostStitchingOperation.AccessAndLatencyBought;
import com.vmturbo.stitching.poststitching.PropagateStorageAccessAndLatencyPostStitchingOperation.AccessAndLatencySold;
import com.vmturbo.stitching.utilities.AccessAndLatency;

/**
 * Test {@link PropagateStorageAccessAndLatencyPostStitchingOperation}.
 */
public class PropagateStorageAccessAndLatencyPostStitchingOpTest {

    private static final double EPSILON = 1e-6;

    private final PropagateStorageAccessAndLatencyPostStitchingOperation op =
        new PropagateStorageAccessAndLatencyPostStitchingOperation();

    private final CommodityBoughtImpl storageAccessBought =
        makeCommodityBoughtBuilder(CommodityType.STORAGE_ACCESS);
    private final CommodityBoughtImpl storageLatencyBought =
        makeCommodityBoughtBuilder(CommodityType.STORAGE_LATENCY);
    private final CommodityBoughtImpl cpuBought =
        makeCommodityBoughtBuilder(CommodityType.CPU);

    private final CommoditySoldView storageAccessSold = makeCommoditySold(CommodityType.STORAGE_ACCESS);
    private final CommoditySoldView storageLatencySold = makeCommoditySold(CommodityType.STORAGE_LATENCY);

    private TopologyEntity.Builder diskArray = makeBasicTopologyEntityBuilder(1, EntityType.DISK_ARRAY);
    private TopologyEntity.Builder logicalPool = makeBasicTopologyEntityBuilder(2, EntityType.LOGICAL_POOL);

    private TopologyEntity.Builder storage1 = makeBasicTopologyEntityBuilder(11, EntityType.STORAGE);
    private TopologyEntity.Builder storage2 = makeBasicTopologyEntityBuilder(12, EntityType.STORAGE);
    private TopologyEntity.Builder storage3 = makeBasicTopologyEntityBuilder(13, EntityType.STORAGE);
    private TopologyEntity.Builder storage4 = makeBasicTopologyEntityBuilder(14, EntityType.STORAGE);

    private TopologyEntity.Builder vm1 = makeBasicTopologyEntityBuilder(21, EntityType.VIRTUAL_MACHINE);
    private TopologyEntity.Builder vm2 = makeBasicTopologyEntityBuilder(22, EntityType.VIRTUAL_MACHINE);
    private TopologyEntity.Builder vm3 = makeBasicTopologyEntityBuilder(23, EntityType.VIRTUAL_MACHINE);
    private TopologyEntity.Builder vm4 = makeBasicTopologyEntityBuilder(24, EntityType.VIRTUAL_MACHINE);
    private TopologyEntity.Builder vm5 = makeBasicTopologyEntityBuilder(25, EntityType.VIRTUAL_MACHINE);
    private TopologyEntity.Builder vm6 = makeBasicTopologyEntityBuilder(26, EntityType.VIRTUAL_MACHINE);
    private TopologyEntity.Builder vm7 = makeBasicTopologyEntityBuilder(27, EntityType.VIRTUAL_MACHINE);

    private TopologyEntity.Builder pm1 = makeBasicTopologyEntityBuilder(31, EntityType.PHYSICAL_MACHINE);
    private TopologyEntity.Builder pm2 = makeBasicTopologyEntityBuilder(32, EntityType.PHYSICAL_MACHINE);
    private TopologyEntity.Builder pm3 = makeBasicTopologyEntityBuilder(33, EntityType.PHYSICAL_MACHINE);

    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> stitchingJournal =
        (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    private static final double EXPECTED_ACCESS_SOLD = 165;
    private final ImmutableMap<TopologyEntity.Builder, ImmutableMap<TopologyEntity.Builder, Double>> storageAccessExpectations =
        ImmutableMap.<TopologyEntity.Builder, ImmutableMap<TopologyEntity.Builder, Double>>builder()
            .put(vm1, ImmutableMap.of(storage1, 10.0))
            .put(vm2, ImmutableMap.of(storage1, 15.0))
            .put(vm3, ImmutableMap.of(storage2, 30.0, storage3, 50.0))
            .put(vm4, ImmutableMap.of(storage2, 5.0))
            .put(pm1, ImmutableMap.of(storage2, 80.0))
            .put(pm2, ImmutableMap.of(storage2, 0.0))
            .put(pm3, ImmutableMap.of(storage1, 0.0, storage4, 10.0))
            .put(storage1, ImmutableMap.of(diskArray, 25.0))
            .put(storage2, ImmutableMap.of(diskArray, 80.0))
            .put(storage3, ImmutableMap.of(logicalPool, 50.0))
            .put(storage4, ImmutableMap.of(logicalPool, 10.0))
            .put(logicalPool, ImmutableMap.of(diskArray, 60.0))
            .build();

    private final ImmutableMap<TopologyEntity.Builder, ImmutableMap<TopologyEntity.Builder, Double>>
        vmPmRelationshipsForAccess =
        ImmutableMap.<TopologyEntity.Builder, ImmutableMap<TopologyEntity.Builder,Double>>builder()
            .put(vm1, ImmutableMap.of())
            .put(vm2, ImmutableMap.of())
            .put(vm3, ImmutableMap.of(pm1, 3000.0))
            .put(vm4, ImmutableMap.of(pm1, 5000.0))
            .build();

    private static final double EXPECTED_LATENCY_SOLD = 15;
    private final ImmutableMap<TopologyEntity.Builder, ImmutableMap<TopologyEntity.Builder, Double>>
        storageLatencyExpectations =
        ImmutableMap.<TopologyEntity.Builder, ImmutableMap<TopologyEntity.Builder, Double>>builder()
            .put(vm1, ImmutableMap.of(storage1, 10.0))
            .put(vm2, ImmutableMap.of(storage1, 20.0))
            .put(vm3, ImmutableMap.of(storage2, 10.0))
            .put(vm4, ImmutableMap.of(storage2, 30.0))
            .put(vm5, ImmutableMap.of(storage3, 0.0))
            .put(vm6, ImmutableMap.of(storage3, 0.0))
            .put(vm7, ImmutableMap.of(storage4, 10.0))
            .put(pm1, ImmutableMap.of(storage2, 0.0))
            .put(pm2, ImmutableMap.of(storage2, 0.0))
            .put(pm3, ImmutableMap.of(storage1, 10.0))
            .put(storage1, ImmutableMap.of(diskArray, 10.0)) // Had to change this one because we are being consistent
                                                             // between access and latency calculation with respect
                                                             // to when to favor PM over VM values.
            .put(storage2, ImmutableMap.of(diskArray, 20.0))
            .put(storage3, ImmutableMap.of(logicalPool, 0.0))
            .put(storage4, ImmutableMap.of(logicalPool, 10.0))
            .put(logicalPool, ImmutableMap.of(diskArray, 10.0))
            .build();

    private final ImmutableMap<TopologyEntity.Builder, ImmutableMap<TopologyEntity.Builder, Double>>
        vmPmRelationshipsForLatency =
        ImmutableMap.<TopologyEntity.Builder, ImmutableMap<TopologyEntity.Builder,Double>>builder()
            .put(vm1, ImmutableMap.of(pm3, 1000.0))
            .put(vm2, ImmutableMap.of(pm3, 4000.0))
            .put(vm3, ImmutableMap.of(pm1, 3000.0))
            .put(vm4, ImmutableMap.of(pm2, 5000.0))
            .put(vm5, ImmutableMap.of())
            .put(vm6, ImmutableMap.of())
            .put(vm7, ImmutableMap.of())
            .build();

    private UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    /**
     * See https://vmturbo.atlassian.net/wiki/x/DYEKFw for further details
     *                              50
     * VM1           VM2        VM3+-+     VM4
     * +             +          +    |     + 5
     * |             |       30 |    +-----+------------+
     * |10         15|          |          |            |
     * |             |          |->PM1<----+     PM2    |    ++PM3+--+
     * |             |          |   +80    |      +0    |    |0      |10
     * |             |          |   |      |      |          |       |
     * |    +------------------------------|------------|----+       |
     * |    |        |          |   |      |      |     |            |
     * |    |        |          |   |      |      |     |            |
     * |    v        |          |   |      v      |     v            v
     * +-->ST1<------+          +---+--->ST2<-----+    ST3          ST4
     *      +                             +             +            +
     *      |                             |80           |50          |10
     *      |25                           |             |            |
     *      |                 +-----------+             +->LP<-------+
     *      |                 |                            +
     *      |                 |                            |60
     *      |                 |                            |
     *      |                 v                            |
     *      +---------------->DA<--------------------------+
     *                   25+80+60=165
     */
    private void setupStorageAccessTest() {
        setAccessRelationships();
        Stream.of(vm1, vm2, vm3, vm4, pm1, pm2, pm3)
            .forEach(entity -> storageAccessExpectations.get(entity)
                .forEach((seller, amt) -> loadBuyer(entity, seller.getOid(), EntityType.STORAGE_VALUE, amt, 1)));
        Stream.of(storage1, storage2, storage3, storage4, logicalPool)
            .forEach(entity -> storageAccessExpectations.get(entity)
                .forEach((seller, amt) -> setupBuyerRelationship(entity, seller)));
        vmPmRelationshipsForAccess
            .forEach((vm, relationships) ->
                relationships.forEach((pm, amount) -> setupVmPmCommodities(vm, pm, amount)));
    }

    /**
     * See https://vmturbo.atlassian.net/wiki/x/DYEKFw for further details
     *
     * Note that these tests set Access to 1.0 for simplicity but averages are actually
     * IOPS-weighted averages (See {@link AccessAndLatency})
     *
     *  10
     *  +--+VM1     +VM2+             VM3        VM4+           VM5     VM6             VM7
     *  |  |        |   |20           + +10      +  |30          +        +              +
     *  |  |        |   |             | |        |  |           0|        |0             |10
     *  |  |        |   |           +-+ |     +--+  |            |        |              |
     *  |  |        |   |           |   |     |     |            |        |              |
     *  |  |        |   |           v   |     v     |            |        |              |
     *  |  +>PM3<---+   |          PM1  |    PM2    |            |        |              |
     *  |    +          |           +   |     +     |            |        |              |
     *  |    |10        |          0|   |     |0    |            |        |              |
     *  |    |          |           |   |   +-+     |            |        |              |
     *  |    +----+     |           |   |   |       |            |        |              |
     *  |         |     |           |   |   |       |            |        |              |
     *  |         v     |           |   |   v       |            |        |              v
     *  +------->ST1<---+           +----->ST2<-----+            +->ST3<--+             ST4
     *            +                         +                        +                   +
     *            |10 (VMs are ignored      |(10+30)/2=20            |0                  |10
     *            |    in favor of PMs)     |                        |                   |
     *            |                         |                        |                   |
     *            |                         +-+                      +------->LP<--------+
     *            |                           |                               +
     *            |                           |                               |10
     *            |                           |          +--------------------+
     *            |                           |          |
     *            |                           v          |
     *            +-------------------------->DA<--------+
     *                                (10+10+30+10)/4=15
     */
    private void setupStorageLatencyTest() {
        setLatencyRelationships();

        Stream.of(vm1, vm2, vm3, vm4, vm5, vm6, vm7, pm1, pm2, pm3).forEach(entity ->
            storageLatencyExpectations.get(entity).forEach((seller, amount) ->
                loadBuyer(entity, seller.getOid(), EntityType.STORAGE_VALUE, amount == 0 ? 0 : 1, amount)));
        Stream.of(storage1, storage2, storage3, storage4, logicalPool)
            .forEach(entity -> storageAccessExpectations.get(entity)
                .forEach((seller, amt) -> setupBuyerRelationship(entity, seller)));

        vmPmRelationshipsForLatency
            .forEach((vm, relationships) ->
                relationships.forEach((pm, amount) -> setupVmPmCommodities(vm, pm, amount)));
    }

    @Test
    public void testCalculationAndPropagationOfAccess() {
        setupStorageAccessTest();

        final TopologyEntity diskArrayEntity = diskArray.build();
        op.performOperation(Stream.of(diskArrayEntity), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        checkAccessAmounts(diskArrayEntity);
    }

    @Test
    public void testCalculationAndPropagationOfLatency() {
        setupStorageLatencyTest();

        final TopologyEntity diskArrayEntity = diskArray.build();
        op.performOperation(Stream.of(diskArrayEntity), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        checkLatencyAmounts(diskArrayEntity);
    }

    @Test
    public void testPropagationWhenStoragesNotSelling() {
        setupStorageAccessTest();
        removeCommoditiesSold(diskArray, CommodityType.STORAGE_LATENCY, CommodityType.STORAGE_ACCESS);
        removeCommoditiesSold(storage1, CommodityType.STORAGE_LATENCY, CommodityType.STORAGE_ACCESS);
        removeCommoditiesSold(storage2, CommodityType.STORAGE_LATENCY, CommodityType.STORAGE_ACCESS);
        removeCommoditiesBought(storage1, CommodityType.STORAGE_LATENCY, CommodityType.STORAGE_ACCESS);
        removeCommoditiesBought(storage2, CommodityType.STORAGE_LATENCY, CommodityType.STORAGE_ACCESS);

        final TopologyEntity diskArrayEntity = diskArray.build();
        op.performOperation(Stream.of(diskArrayEntity), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        checkAccessAmounts(diskArrayEntity);
    }

    @Test
    public void testVmPmNotBuyingAccess() {
        setupStorageAccessTest();

        // Because these entities were not contributing to the overall number, our expectations
        // do not need to change, but also the implementation should be able to properly handle
        // these entities not possessing the commodities it is looking for on the VM/PM.
        removeCommoditiesBought(vm4, CommodityType.STORAGE_LATENCY, CommodityType.STORAGE_ACCESS);
        removeCommoditiesBought(pm2, CommodityType.STORAGE_LATENCY, CommodityType.STORAGE_ACCESS);

        final TopologyEntity diskArrayEntity = diskArray.build();
        op.performOperation(Stream.of(diskArrayEntity), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        checkAccessAmounts(diskArrayEntity);
    }

    @Test
    public void testProbeProvidesLatencyButNotAccess() {
        setupStorageLatencyTest();
        getCommoditySold(diskArray, CommodityType.STORAGE_LATENCY)
            .setUsed(123.4);
        assertEquals(0, getCommoditySold(diskArray, CommodityType.STORAGE_ACCESS).getUsed(), EPSILON);

        final TopologyEntity diskArrayEntity = diskArray.build();
        op.performOperation(Stream.of(diskArrayEntity), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        // We should NOT have overridden the value provided by the probe.
        assertEquals(123.4, getUsedSold(diskArrayEntity, CommodityType.STORAGE_LATENCY).get(), EPSILON);

        // However, we SHOULD have overridden the un-supplied Access value.
        assertNotEquals(0, getUsedSold(diskArrayEntity, CommodityType.STORAGE_ACCESS).get(), EPSILON);
    }

    @Test
    public void testProbeProvidesAccessButNotLatency() {
        setupStorageAccessTest();

        getCommoditySold(diskArray, CommodityType.STORAGE_ACCESS)
            .setUsed(100.0);
        final TopologyEntity diskArrayEntity = diskArray.build();
        op.performOperation(Stream.of(diskArrayEntity), settingsMock, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));

        // We should NOT have overridden the value provided by the probe.
        assertEquals(100.0, getUsedSold(diskArrayEntity, CommodityType.STORAGE_ACCESS).get(), EPSILON);
    }

    @Test
    public void testAccessAndLatencySoldUpdateIfUnset() {
        final TopologyEntity diskArrayEntity = diskArray.build();

        assertFalse(getUsedSold(diskArrayEntity, CommodityType.STORAGE_ACCESS).isPresent());
        assertFalse(getUsedSold(diskArrayEntity, CommodityType.STORAGE_LATENCY).isPresent());

        final AccessAndLatencySold accessAndLatencySold = new AccessAndLatencySold(diskArrayEntity);
        accessAndLatencySold.updateIfUnset(accessAndLatencySold.access, 12.3);
        assertEquals(12.3, getUsedSold(diskArrayEntity, CommodityType.STORAGE_ACCESS).get(), EPSILON);

        accessAndLatencySold.updateIfUnset(accessAndLatencySold.latency, 11.1);
        assertEquals(11.1, getUsedSold(diskArrayEntity, CommodityType.STORAGE_LATENCY).get(), EPSILON);

        // Calling again should not further update
        accessAndLatencySold.updateIfUnset(accessAndLatencySold.access, 99.9);
        assertEquals(12.3, getUsedSold(diskArrayEntity, CommodityType.STORAGE_ACCESS).get(), EPSILON);
    }

    @Test
    public void testAccessAndLatencyBoughtUpdateIfUnset() {
        setAccessRelationships();
        loadBuyer(vm1, storage1.getOid(), EntityType.STORAGE_VALUE, 0, 0);

        final TopologyEntity vmEntity = vm1.build();
        final TopologyEntity storageEntity = storage1.build();

        assertEquals(0, getUsedBought(vmEntity, storage1.getOid(), CommodityType.STORAGE_ACCESS).get(), EPSILON);
        assertEquals(0, getUsedBought(vmEntity, storage1.getOid(), CommodityType.STORAGE_LATENCY).get(), EPSILON);

        final AccessAndLatencyBought accessAndLatencyBought = new AccessAndLatencyBought(vmEntity, storageEntity);
        accessAndLatencyBought.updateIfUnset(accessAndLatencyBought.access, () -> 12.3);
        assertEquals(12.3, getUsedBought(vmEntity, storage1.getOid(), CommodityType.STORAGE_ACCESS).get(), EPSILON);

        accessAndLatencyBought.updateIfUnset(accessAndLatencyBought.latency, () -> 11.1);
        assertEquals(11.1, getUsedBought(vmEntity, storage1.getOid(),CommodityType.STORAGE_LATENCY).get(), EPSILON);

        // Calling again should not further update
        accessAndLatencyBought.updateIfUnset(accessAndLatencyBought.access, () -> 99.9);
        assertEquals(12.3, getUsedBought(vmEntity, storage1.getOid(),CommodityType.STORAGE_ACCESS).get(), EPSILON);
    }

    private CommoditySoldImpl getCommoditySold(@Nonnull final TopologyEntity.Builder entity,
                                               final CommodityType commodityType) {
        return entity.getTopologyEntityImpl().getCommoditySoldListImplList().stream()
            .filter(commodity -> commodity.getCommodityType().getType() == commodityType.getNumber())
            .findFirst()
            .get();
    }

    private void removeCommoditiesSold(@Nonnull final TopologyEntity.Builder entity,
                                       final CommodityType... commodityTypes) {
        final List<CommoditySoldImpl> impls = entity.getTopologyEntityImpl().getCommoditySoldListImplList();
        for (CommodityType commodityType : commodityTypes) {
            for (int i = 0; i < impls.size(); i++) {
                if (impls.get(i).getCommodityType().getType() == commodityType.getNumber()) {
                    entity.getTopologyEntityImpl().removeCommoditySoldList(i);
                    break;
                }
            }
        }
    }

    private void removeCommoditiesBought(@Nonnull final TopologyEntity.Builder entity,
                                       final CommodityType... commodityTypes) {
        for (CommodityType commodityType : commodityTypes) {
            entity.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersImplList().stream().forEach(list -> {
                for (int i = 0; i < list.getCommodityBoughtList().size(); i++) {
                    if (list.getCommodityBoughtList().get(i)
                        .getCommodityType().getType() == commodityType.getNumber()) {
                        list.removeCommodityBought(i);
                        break;
                    }
                }
            });
        }
    }

    private TopologyEntity.Builder makeBasicTopologyEntityBuilder(final long oid, @Nonnull final EntityType type) {
        return makeTopologyEntityBuilder(
            oid, type.getNumber(), Arrays.asList(storageAccessSold, storageLatencySold),
            Arrays.asList(storageAccessBought, storageLatencyBought)
        );
    }

    private void loadBuyer(@Nonnull final TopologyEntity.Builder buyer,
                           final long sellerOid, final int sellerType,
                           final double accessUsed, final double latencyUsed) {
        buyer.getTopologyEntityImpl().addCommoditiesBoughtFromProviders(
            makeCommoditiesBoughtFromProvider(sellerOid, sellerType,
                Arrays.asList(storageAccessBought.setUsed(accessUsed),
                    storageLatencyBought.setUsed(latencyUsed))));
    }

    private void setupBuyerRelationship(Builder entity, Builder seller) {
        entity.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersImplList()
            .get(0)
            .setProviderId(seller.getOid())
            .setProviderEntityType(seller.getEntityType());
    }

    private void setupVmPmCommodities(@Nonnull final TopologyEntity.Builder vm,
                                      @Nonnull final TopologyEntity.Builder pm, final double amountUsed) {
        vm.getTopologyEntityImpl().addCommoditiesBoughtFromProviders(
            makeCommoditiesBoughtFromProvider(pm.getOid(), EntityType.PHYSICAL_MACHINE_VALUE,
                Collections.singletonList(cpuBought.setUsed(amountUsed))));
    }

    private void setAccessRelationships() {
        storageAccessExpectations.forEach((entity, relationships) ->
            relationships.forEach((provider, amount) -> {
                entity.addProvider(provider);
                provider.addConsumer(entity);
            }));

        vmPmRelationshipsForAccess.forEach((vm, relationships) ->
            relationships.forEach((seller, amount) -> {
                vm.addProvider(seller);
                seller.addConsumer(vm);
            }));
    }

    private void setLatencyRelationships() {
        storageLatencyExpectations.forEach((entity, relationships) ->
            relationships.forEach((provider, amount) -> {
                entity.addProvider(provider);
                provider.addConsumer(entity);
            }));

        vmPmRelationshipsForLatency.forEach((vm, relationships) ->
            relationships.forEach((seller, amount) -> {
                vm.addProvider(seller);
                seller.addConsumer(vm);
            }));
    }

    private Optional<Double> findExpectedAccess(@Nonnull final TopologyEntity consumer,
                                                @Nonnull final TopologyEntity provider) {
        return findExpectedValue(storageAccessExpectations, consumer, provider);
    }

    private Optional<Double> findExpectedLatency(@Nonnull final TopologyEntity consumer,
                                                 @Nonnull final TopologyEntity provider) {
        return findExpectedValue(storageLatencyExpectations, consumer, provider);
    }

    private Optional<Double> findExpectedValue(
        @Nonnull final ImmutableMap<TopologyEntity.Builder, ImmutableMap<TopologyEntity.Builder, Double>> expectationMap,
        @Nonnull final TopologyEntity consumer,
        @Nonnull final TopologyEntity provider) {
        return expectationMap.entrySet().stream()
            .filter(entry -> entry.getKey().getOid() == consumer.getOid())
            .map(Entry::getValue)
            .findFirst()
            .flatMap(resources -> resources.entrySet().stream()
                .filter(entry -> entry.getKey().getOid() == provider.getOid())
                .map(Entry::getValue)
                .findFirst());
    }

    private void checkAccessAmounts(@Nonnull final TopologyEntity diskArr) {
        getUsedSold(diskArr, CommodityType.STORAGE_ACCESS).ifPresent(daSoldAccess ->
            assertEquals(EXPECTED_ACCESS_SOLD, daSoldAccess, EPSILON));

        diskArr.getConsumers().forEach(storageOrLP -> {
            getUsedSold(storageOrLP, CommodityType.STORAGE_ACCESS).ifPresent(consumerSoldAccess ->
                assertEquals(EXPECTED_ACCESS_SOLD, consumerSoldAccess, EPSILON));

            final double expectedBought1 = findExpectedAccess(storageOrLP, diskArr).get();
            getUsedBought(storageOrLP, diskArr.getOid(), CommodityType.STORAGE_ACCESS).ifPresent(consumerBought ->
                assertEquals(expectedBought1, consumerBought, EPSILON));

            storageOrLP.getConsumers().forEach(storageVmOrPm -> {

                if (storageVmOrPm.getEntityType() == EntityType.STORAGE_VALUE) {
                    final double innerConsumerSoldAccess =
                        getUsedSold(storageVmOrPm, CommodityType.STORAGE_ACCESS).get();
                    assertEquals(innerConsumerSoldAccess, EXPECTED_ACCESS_SOLD, EPSILON);
                }

                getUsedBought(storageVmOrPm, storageOrLP.getOid(), CommodityType.STORAGE_ACCESS)
                    .ifPresent(actualAccess ->
                        assertEquals(findExpectedAccess(storageVmOrPm, storageOrLP).get(), actualAccess, EPSILON));

                if (storageVmOrPm.getEntityType() == EntityType.STORAGE_VALUE) {
                    storageVmOrPm.getConsumers()
                        .forEach(vmOrPm ->
                            getUsedBought(vmOrPm, storageVmOrPm.getOid(), CommodityType.STORAGE_ACCESS)
                                .ifPresent(actual ->
                                    assertEquals(findExpectedAccess(vmOrPm, storageVmOrPm).get(), actual, EPSILON)));
                }
            });
        });
    }

    private void checkLatencyAmounts(@Nonnull final TopologyEntity diskArr) {
        getUsedSold(diskArr, CommodityType.STORAGE_LATENCY).ifPresent(daSoldLatency ->
            assertEquals(EXPECTED_LATENCY_SOLD, daSoldLatency, EPSILON));


        diskArr.getConsumers().forEach(storageOrLP -> {

            getUsedSold(storageOrLP, CommodityType.STORAGE_LATENCY).ifPresent(soldLatency ->
                assertEquals(EXPECTED_LATENCY_SOLD, soldLatency, EPSILON));
            getUsedBought(storageOrLP, diskArr.getOid(), CommodityType.STORAGE_LATENCY).ifPresent(boughtLatency -> {
                final double expectedBought = findExpectedLatency(storageOrLP, diskArr).get();
                assertEquals(expectedBought, boughtLatency, EPSILON);
            });

            storageOrLP.getConsumers().forEach(storageVmOrPm -> {
                if (storageVmOrPm.getEntityType() == EntityType.STORAGE_VALUE) {
                    final double soldLatency2 =
                        getUsedSold(storageVmOrPm, CommodityType.STORAGE_LATENCY).get();
                    assertEquals(soldLatency2, EXPECTED_LATENCY_SOLD, EPSILON);
                }

                final double boughtLatency2 =
                    getUsedBought(storageVmOrPm, storageOrLP.getOid(), CommodityType.STORAGE_LATENCY).get();
                final double expectedBought2 = findExpectedLatency(storageVmOrPm, storageOrLP).get();
                assertEquals(boughtLatency2, expectedBought2, EPSILON);

                if (storageVmOrPm.getEntityType() != EntityType.PHYSICAL_MACHINE_VALUE) {
                    storageVmOrPm.getConsumers().forEach(vmOrPm -> {
                        final double boughtLatency3 =
                            getUsedBought(vmOrPm, storageVmOrPm.getOid(), CommodityType.STORAGE_LATENCY).get();
                        final double expectedBought3 = findExpectedLatency(vmOrPm, storageVmOrPm).get();
                        assertEquals(boughtLatency3, expectedBought3, EPSILON);
                    });
                }
            });
        });

    }

    private Optional<Double> getUsedSold(@Nonnull final TopologyEntity entity,
                                         @Nonnull final CommodityType type) {
        return entity.getTopologyEntityImpl().getCommoditySoldListList().stream()
            .filter(commodity -> commodity.getCommodityType().getType() == type.getNumber())
            .findAny()
            .filter(CommoditySoldView::hasUsed)
            .map(CommoditySoldView::getUsed);
    }

    private Optional<Double> getUsedBought(@Nonnull final TopologyEntity buyer,
                                 final long sellerOid,
                                 @Nonnull final CommodityType type) {
        return buyer.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().stream()
            .filter(commodity -> commodity.getProviderId() == sellerOid)
            .flatMap(commodity -> commodity.getCommodityBoughtList().stream())
            .filter(commodity -> commodity.getCommodityType().getType() == type.getNumber())
            .findAny()
            .filter(CommodityBoughtView::hasUsed)
            .map(CommodityBoughtView::getUsed);
    }
}
