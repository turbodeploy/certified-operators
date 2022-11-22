package com.vmturbo.topology.processor.stitching.prestitching;

import static com.vmturbo.stitching.PreStitchingOperationLibrary.AZURE_ENTITY_TYPES;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeTierData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.PreStitchingOperationLibrary;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.prestitching.SharedCloudEntityPreStitchingOperation;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.StitchingOperationScopeFactory;
import com.vmturbo.topology.processor.stitching.StitchingResultBuilder;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test for SharedCloudEntityPreStitchingOperation.
 */
public class SharedCloudEntityPreStitchingOperationTest {

    private static final Collection<PreStitchingOperation> AZURE_OPERATIONS =
            AZURE_ENTITY_TYPES.entrySet().stream()
                    .map(entry -> PreStitchingOperationLibrary.createCloudEntityPreStitchingOperation(
                            ImmutableSet.of(SDKProbeType.AZURE), entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList());
    private static final long azureProbeId = 100000001L;
    private static final int TARGETS = 400;
    private static final int SHARED_COMPUTE_TIERS_PER_TARGET = 368;
    private static final int SHARED_REGIONS_PER_TARGET = 30;

    private final ProbeStore probeStore = mock(ProbeStore.class);
    private final TargetStore targetStore = mock(TargetStore.class);

    /**
     * Run before every test.
     */
    @Before
    public void before() {
        when(probeStore.getProbeIdForType(SDKProbeType.AZURE.getProbeType()))
                .thenReturn(Optional.of(azureProbeId));
        // 0 ~ 399 are target ids
        final List<Target> targets = IntStream.range(0, TARGETS).boxed().map(targetId -> {
            Target target = mock(Target.class);
            when(target.getId()).thenReturn((long)targetId);
            return target;
        }).collect(Collectors.toList());
        when(targetStore.getProbeTargets(eq(azureProbeId))).thenReturn(targets);
    }

    /**
     * Test that SharedCloudEntityPreStitchingOperation merged all shared entities as expected.
     * This test use the azure shared entities as an example. It verifies that connections of
     * shared entity are merged, and properties of azure regions are also merged.
     *
     * <p>target1:
     *     vm1 --> compute tier1 --> [region1, storage tier1]
     * target2:
     *     vm2 --> compute tier1 --> [region1, region2, storage tier2]
     * After merge:
     *     [vm1, vm2] --> compute tier1 --> [region1, region2, storageTier1, storageTier2]
     */
    @Test
    public void testMergeSharedCloudEntitiesForAzure() {
        final long targetId1 = 1;
        final long targetId2 = 2;
        final long vmOid1 = 11;
        final long vmOid2 = 12;
        final long ctOid1 = 21;
        final long stOid1 = 31;
        final long stOid2 = 32;
        final long regionOid1 = 41;
        final long regionOid2 = 42;

        final String vmId1 = String.valueOf(vmOid1);
        final String vmId2 = String.valueOf(vmOid2);
        final String ctId1 = "azure::VMPROFILE::Standard_D4a_v4";
        final String stId1 = "SC1";
        final String stId2 = "STANDARD";
        final String regionId1 = "azure-West US";
        final String regionId2 = "azure-North Europe";
        final String propertyName1 = "property1";
        final String propertyValue1 = "value1";
        final String propertyName2 = "property2";
        final String propertyValue2 = "value2";

        final StitchingContext.Builder stitchingContextBuilder =
                StitchingContext.newBuilder(9, targetStore)
                        .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));

        // target1
        final EntityDTO.Builder vm1 = newEntity(vmId1, EntityType.VIRTUAL_MACHINE);
        final EntityDTO.Builder ct1 = newEntity(ctId1, EntityType.COMPUTE_TIER)
                .addCommoditiesSold(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.CPU)
                        .setCapacity(1024))
                .addLayeredOver(regionId1)
                .addLayeredOver(stId1);
        vm1.addCommoditiesBought(CommodityBought.newBuilder()
                .addBought(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.CPU)
                        .setUsed(20))
                .setProviderId(ctId1));
        final EntityDTO.Builder st1 = newEntity(stId1, EntityType.STORAGE_TIER);
        final EntityDTO.Builder region1 = newEntity(regionId1, EntityType.REGION)
                .addEntityProperties(EntityProperty.newBuilder()
                        .setNamespace(SDKUtil.DEFAULT_NAMESPACE)
                        .setName(propertyName1)
                        .setValue(propertyValue1));
        Map<String, StitchingEntityData> entityByLocalId1 = new HashMap<>();
        addStitchingEntity(entityByLocalId1, vm1, vmOid1, targetId1);
        addStitchingEntity(entityByLocalId1, ct1, ctOid1, targetId1);
        addStitchingEntity(entityByLocalId1, st1, stOid1, targetId1);
        addStitchingEntity(entityByLocalId1, region1, regionOid1, targetId1);
        entityByLocalId1.values().forEach(d -> stitchingContextBuilder.addEntity(d, entityByLocalId1));

        // target2
        final EntityDTO.Builder ct2 = newEntity(ctId1, EntityType.COMPUTE_TIER)
                .addCommoditiesSold(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.CPU)
                        .setCapacity(1024))
                .addLayeredOver(regionId1)
                .addLayeredOver(regionId2)
                .addLayeredOver(stId2);
        final EntityDTO.Builder vm2 = newEntity(vmId2, EntityType.VIRTUAL_MACHINE);
        vm2.addCommoditiesBought(CommodityBought.newBuilder()
                .addBought(CommodityDTO.newBuilder()
                        .setCommodityType(CommodityType.CPU)
                        .setUsed(30))
                .setProviderId(ctId1));
        final EntityDTO.Builder st2 = newEntity(stId2, EntityType.STORAGE_TIER);
        final EntityDTO.Builder region11 = EntityDTO.newBuilder(region1.build())
                .clearEntityProperties()
                .addEntityProperties(EntityProperty.newBuilder()
                        .setNamespace(SDKUtil.DEFAULT_NAMESPACE)
                        .setName(propertyName2)
                        .setValue(propertyValue2));
        final EntityDTO.Builder region2 = newEntity(regionId2, EntityType.REGION);

        Map<String, StitchingEntityData> entityByLocalId2 = new HashMap<>();
        addStitchingEntity(entityByLocalId2, vm2, vmOid2, targetId2);
        addStitchingEntity(entityByLocalId2, ct2, ctOid1, targetId2);
        addStitchingEntity(entityByLocalId2, st2, stOid2, targetId2);
        addStitchingEntity(entityByLocalId2, region11, regionOid1, targetId2);
        addStitchingEntity(entityByLocalId2, region2, regionOid2, targetId2);
        entityByLocalId2.values().forEach(d -> stitchingContextBuilder.addEntity(d, entityByLocalId2));

        // perform operations
        final StitchingContext stitchingContext = stitchingContextBuilder.build();
        performOperations(stitchingContext, AZURE_OPERATIONS);

        // total of 2 entities
        assertEquals(7, stitchingContext.getStitchingGraph().entityCount());
        // 2 vms, 1 compute tier, 2 storage tiers and 2 regions
        assertEquals(2, stitchingContext.getEntitiesOfType(EntityType.VIRTUAL_MACHINE).count());
        assertEquals(1, stitchingContext.getEntitiesOfType(EntityType.COMPUTE_TIER).count());
        assertEquals(2, stitchingContext.getEntitiesOfType(EntityType.STORAGE_TIER).count());
        assertEquals(2, stitchingContext.getEntitiesOfType(EntityType.REGION).count());
        // get all entity after stitching
        final TopologyStitchingEntity vmEntity1 = stitchingContext.getEntity(vm1).get();
        final TopologyStitchingEntity vmEntity2 = stitchingContext.getEntity(vm2).get();
        final TopologyStitchingEntity ctEntity1 = stitchingContext.getEntitiesOfType(EntityType.COMPUTE_TIER).findAny().get();
        final TopologyStitchingEntity stEntity1 = stitchingContext.getEntity(st1).get();
        final TopologyStitchingEntity stEntity2 = stitchingContext.getEntity(st2).get();
        final TopologyStitchingEntity regionEntity1 = stitchingContext.getEntity(region1).get();
        final TopologyStitchingEntity regionEntity2 = stitchingContext.getEntity(region2).get();
        // verify vm1 still buys from compute tier
        assertEquals(1, vmEntity1.getCommodityBoughtListByProvider().size());
        assertEquals(1, vmEntity1.getCommodityBoughtListByProvider().get(ctEntity1).size());
        CommodityDTO.Builder boughtComm1 = vmEntity1.getCommodityBoughtListByProvider()
                .get(ctEntity1).get(0).getBoughtList().get(0);
        assertEquals(CommodityType.CPU, boughtComm1.getCommodityType());
        assertEquals(20, boughtComm1.getUsed(), 0);
        // verify vm2 still buys from compute tier
        assertEquals(1, vmEntity2.getCommodityBoughtListByProvider().size());
        assertEquals(1, vmEntity2.getCommodityBoughtListByProvider().get(ctEntity1).size());
        CommodityDTO.Builder boughtComm2 = vmEntity2.getCommodityBoughtListByProvider()
                .get(ctEntity1).get(0).getBoughtList().get(0);
        assertEquals(CommodityType.CPU, boughtComm2.getCommodityType());
        assertEquals(30, boughtComm2.getUsed(), 0);
        // verify compute tier still sells commodities
        assertEquals(1, ctEntity1.getCommoditiesSold().count());
        assertEquals(CommodityType.CPU, ctEntity1.getCommoditiesSold().findAny().get().getCommodityType());
        assertEquals(1024, ctEntity1.getCommoditiesSold().findAny().get().getCapacity(), 0);
        // check merge info
        assertEquals(1, ctEntity1.getMergeInformation().size());
        // verify compute tier connected to
        assertEquals(2, ctEntity1.getConnectedToByType().size());
        assertThat(ctEntity1.getConnectedToByType().get(ConnectionType.NORMAL_CONNECTION),
                containsInAnyOrder(stEntity1, stEntity2));
        assertThat(ctEntity1.getConnectedToByType().get(ConnectionType.AGGREGATED_BY_CONNECTION),
                containsInAnyOrder(regionEntity1, regionEntity2));
        // verify storage tier connected from
        assertEquals(1, stEntity1.getConnectedFromByType().size());
        assertThat(stEntity1.getConnectedFromByType().get(ConnectionType.NORMAL_CONNECTION),
                containsInAnyOrder(ctEntity1));
        assertThat(stEntity2.getConnectedFromByType().get(ConnectionType.NORMAL_CONNECTION),
                containsInAnyOrder(ctEntity1));
        // verify region connected from
        assertEquals(1, regionEntity1.getConnectedFromByType().size());
        assertThat(regionEntity1.getConnectedFromByType().get(ConnectionType.AGGREGATED_BY_CONNECTION),
                containsInAnyOrder(ctEntity1));
        assertThat(regionEntity2.getConnectedFromByType().get(ConnectionType.AGGREGATED_BY_CONNECTION),
                containsInAnyOrder(ctEntity1));
        // check merge info
        assertEquals(1, regionEntity1.getMergeInformation().size());
        // verify region properties are merged
        final List<String> properties = regionEntity1.getEntityBuilder().getEntityPropertiesList()
                .stream()
                .filter(p -> p.getNamespace().equals(SDKUtil.DEFAULT_NAMESPACE))
                .map(p -> p.getName() + p.getValue())
                .collect(Collectors.toList());
        assertThat(properties, containsInAnyOrder(propertyName1 + propertyValue1,
                propertyName2 + propertyValue2));
    }

    /**
     * Test that shared entities are merged properly when one of the entities is a proxy.
     */
    @Test
    public void testMergeSharedEntitiesWithProxy() {
        final StitchingContext.Builder stitchingContextBuilder =
                StitchingContext.newBuilder(2, targetStore)
                        .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));

        final long regionOid = 1;
        final String regionId = "azure-West US";

        // Target 1 sends proxy Region entity with more recent last update time
        final EntityDTO.Builder region1 = newEntity(regionId, EntityType.REGION)
                .setOrigin(EntityOrigin.PROXY);
        final Map<String, StitchingEntityData> entityByLocalId1 = new HashMap<>();
        addStitchingEntity(entityByLocalId1, region1, regionOid, 1, 2000);
        entityByLocalId1.values().forEach(d -> stitchingContextBuilder.addEntity(d, entityByLocalId1));

        // Target 2 sends real Region entity
        final EntityDTO.Builder region2 = newEntity(regionId, EntityType.REGION)
                .setOrigin(EntityOrigin.DISCOVERED);
        final Map<String, StitchingEntityData> entityByLocalId2 = new HashMap<>();
        addStitchingEntity(entityByLocalId2, region2, regionOid, 2, 1000);
        entityByLocalId2.values().forEach(d -> stitchingContextBuilder.addEntity(d, entityByLocalId2));

        final StitchingContext stitchingContext = stitchingContextBuilder.build();

        // Perform operations
        performOperations(stitchingContext, AZURE_OPERATIONS);

        // We expect one merged Region entity
        assertEquals(1, stitchingContext.getStitchingGraph().entityCount());
        assertEquals(1, stitchingContext.getEntitiesOfType(EntityType.REGION).count());

        // The proxy region should be discarded, the real region should be preserved
        assertFalse(stitchingContext.getEntity(region1).isPresent());
        assertTrue(stitchingContext.getEntity(region2).isPresent());
    }

    /**
     * Benchmark for {@link SharedCloudEntityPreStitchingOperation}.
     */
    @Ignore("Time-consuming test")
    @Test
    public void testBenchmarkAzure() {
        final StitchingContext.Builder stitchingContextBuilder =
                StitchingContext.newBuilder(TARGETS * SHARED_COMPUTE_TIERS_PER_TARGET, targetStore)
                        .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));

        for (int targetId = 0; targetId < TARGETS; targetId++) {
            Map<String, StitchingEntityData> entityByLocalId = new HashMap<>();
            // 368 compute tiers for each target
            for (int ctOid = 0; ctOid < SHARED_COMPUTE_TIERS_PER_TARGET; ctOid++) {
                final EntityDTO.Builder ct = newEntity(String.valueOf(ctOid), EntityType.COMPUTE_TIER)
                        .setComputeTierData(ComputeTierData.newBuilder().setFamily("xlarge"));
                addStitchingEntity(entityByLocalId, ct, ctOid, targetId);
                // each tier is connected to 30 regions
                for (int regionOid = 1000; regionOid < SHARED_REGIONS_PER_TARGET + 1000; regionOid++) {
                    final EntityDTO.Builder region = newEntity(String.valueOf(regionOid), EntityType.REGION);
                    addStitchingEntity(entityByLocalId, region, regionOid, targetId);
                    // ct layered over region
                    ct.addLayeredOver(region.getId());
                }
            }
            entityByLocalId.values().forEach(d -> stitchingContextBuilder.addEntity(d, entityByLocalId));
        }

        final StitchingContext stitchingContext = stitchingContextBuilder.build();
        performOperations(stitchingContext, SharedCloudEntityPreStitchingOperationTest.AZURE_OPERATIONS);
    }

    private EntityDTO.Builder newEntity(String id, EntityType entityType) {
        return EntityDTO.newBuilder()
                .setId(id)
                .setDisplayName(id)
                .setEntityType(entityType);
    }

    private static void addStitchingEntity(
            final Map<String, StitchingEntityData> entityByLocalId,
            final Builder entityDtoBuilder,
            final long oid,
            final long targetId,
            final long lastUpdatedTime) {
        entityByLocalId.put(entityDtoBuilder.getId(),
                StitchingEntityData.newBuilder(entityDtoBuilder)
                        .oid(oid)
                        .targetId(targetId)
                        .supportsConnectedTo(true)
                        .lastUpdatedTime(lastUpdatedTime)
                        .build());
    }

    private static void addStitchingEntity(
            final Map<String, StitchingEntityData> entityByLocalId,
            final Builder entityDtoBuilder,
            final long oid,
            final long targetId) {
        addStitchingEntity(entityByLocalId, entityDtoBuilder, oid, targetId, 0);
    }

    private void performOperations(StitchingContext stitchingContext,
            Collection<PreStitchingOperation> preStitchingOperations) {
        final StitchingOperationScopeFactory scopeFactory = new StitchingOperationScopeFactory(
                stitchingContext, probeStore, targetStore);
        final IStitchingJournal<StitchingEntity> stitchingJournal =
                StitchingJournalFactory.emptyStitchingJournalFactory()
                        .stitchingJournal(stitchingContext);
        preStitchingOperations.forEach(preStitchingOperation -> {
            final StitchingResultBuilder stitchingResultBuilder = new StitchingResultBuilder(stitchingContext);
            final List<StitchingEntity> sharedEntities = preStitchingOperation.getScope(scopeFactory).entities()
                    .collect(Collectors.toList());
            final Stopwatch stopwatch = Stopwatch.createStarted();
            preStitchingOperation.performOperation(sharedEntities.stream(), stitchingResultBuilder)
                    .getChanges()
                    .forEach(change -> change.applyChange(stitchingJournal));
            stopwatch.stop();
            System.out.println(String.format("Time: %s for %s", stopwatch.toString(), preStitchingOperation.getOperationName()));
        });
    }
}
