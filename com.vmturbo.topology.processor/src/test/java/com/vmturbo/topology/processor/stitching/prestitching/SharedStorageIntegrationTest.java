package com.vmturbo.topology.processor.stitching.prestitching;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.sdkDtosFromFile;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.visitNeighbors;
import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.writeSdkDtosToFile;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.PostStitchingOperationLibrary;
import com.vmturbo.stitching.PreStitchingOperationLibrary;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperationLibrary;
import com.vmturbo.stitching.poststitching.DiskCapacityCalculator;
import com.vmturbo.stitching.poststitching.SetCommodityMaxQuantityPostStitchingOperationConfig;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityMetadataMissingException;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderException;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.StandardProbeOrdering;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity.CommoditySold;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Integration test for shared storage.
 *
 * Loads a minified topology containing overlapping storages and runs preStitching on them to verify that
 * storages are merged as desired.
 */
public class SharedStorageIntegrationTest {

    private StatsHistoryServiceMole statsRpcSpy = spy(new StatsHistoryServiceMole());
    private StatsHistoryServiceBlockingStub statsServiceClient;
    private final StitchingOperationLibrary stitchingOperationLibrary = new StitchingOperationLibrary();
    private final StitchingOperationStore stitchingOperationStore =
        new StitchingOperationStore(stitchingOperationLibrary);
    private final PreStitchingOperationLibrary preStitchingOperationLibrary =
        new PreStitchingOperationLibrary();
    private PostStitchingOperationLibrary postStitchingOperationLibrary;

    private final long targetAId = 1111L;
    private final long targetBId = 2222L;
    private final long sharedStorageOid = 3333L;
    private final long sharedDiskArrayOid = 44444L;

    private IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
    private final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
    private final TargetStore targetStore = Mockito.mock(TargetStore.class);
    private final Clock entityClock = Mockito.mock(Clock.class);
    private EntityStore entityStore = new EntityStore(targetStore, identityProvider, entityClock);
    private final DiskCapacityCalculator diskCapacityCalculator =
        Mockito.mock(DiskCapacityCalculator.class);
    private final Clock clock = Mockito.mock(Clock.class);

    private final Target targetA = Mockito.mock(Target.class);
    private final Target targetB = Mockito.mock(Target.class);

    private final String sharedStorageId = "9bd4ee88-99c64661";
    private final String sharedDiskArrayId = "DiskArray-9bd4ee88-99c64661";

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(statsRpcSpy);

    @Before
    public void setup() {
        statsServiceClient = StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        postStitchingOperationLibrary =
            new PostStitchingOperationLibrary(
                new SetCommodityMaxQuantityPostStitchingOperationConfig(
                    statsServiceClient, 30, 10), //meaningless values
                diskCapacityCalculator, clock, 0);
        when(targetA.getId()).thenReturn(targetAId);
        when(targetB.getId()).thenReturn(targetBId);
        when(probeStore.getProbeOrdering()).thenReturn(new StandardProbeOrdering(probeStore));
    }

    /**
     * Merging shared storages: [
     * STORAGE 9bd4ee88-99c64661 QS1:NFSShare oid-72192369194440 tgt-72203478139616 cnsms-3 prvds-1,
     * STORAGE 9bd4ee88-99c64661 QS1:NFSShare oid-72192369194440 tgt-72226908063456 cnsms-5 prvds-1
     */
    @Test
    public void testSharedStorageCalculation() throws Exception {
        final Map<Long, EntityDTO> targetAEntities =
            sdkDtosFromFile(getClass(), "protobuf/messages/vc_shared_storage_a.json", 1L);
        final Map<Long, EntityDTO> targetBEntities =
            sdkDtosFromFile(getClass(), "protobuf/messages/vc_shared_storage_b.json", targetAEntities.size() + 1L);

        // The OID for the shared storage must be the same in the two maps, so replace the auto-assigned
        // OIDs with a shared one.
        replaceSharedStorageAndDiskArrayOid(targetAEntities);
        replaceSharedStorageAndDiskArrayOid(targetBEntities);

        addEntities(targetAEntities, targetAId, System.currentTimeMillis() - 1000L);
        addEntities(targetBEntities, targetBId, System.currentTimeMillis()); // Make targetB more up-to-date so we keep its instance.

        final StitchingManager stitchingManager =
            new StitchingManager(stitchingOperationStore, preStitchingOperationLibrary,
                postStitchingOperationLibrary, probeStore, targetStore);

        when(probeStore.getProbeIdsForCategory(eq(ProbeCategory.HYPERVISOR)))
            .thenReturn(Collections.singletonList(5678L));
        when(probeStore.getProbeIdForType(SDKProbeType.HYPERV.getProbeType())).thenReturn(Optional.of(5678L));
        when(probeStore.getProbeIdForType(SDKProbeType.VMM.getProbeType())).thenReturn(Optional.of(5679L));
        when(targetStore.getProbeTargets(eq(5678L))).thenReturn(Arrays.asList(targetA, targetB));
        // the probe type doesn't matter here, just return any non-cloud probe type so it gets
        // treated as normal probe
        when(targetStore.getProbeTypeForTarget(Mockito.anyLong())).thenReturn(Optional.of(SDKProbeType.HYPERV));

        final StitchingJournal<StitchingEntity> journal = new StitchingJournal<>();
        final StitchingContext beforeContext = entityStore.constructStitchingContext();
        final int numEntitiesBefore = beforeContext.size();
        final StitchingEntity keepEntity = beforeContext.getStitchingGraph().entities()
            .filter(entity -> entity.getOid() == sharedStorageOid && entity.getTargetId() == targetBId)
            .findFirst()
            .get();
        final StitchingEntity removeEntity = beforeContext.getStitchingGraph().entities()
            .filter(entity -> entity.getOid() == sharedStorageOid && entity.getTargetId() == targetAId)
            .findFirst()
            .get();
        final int numCombinedConsumers = keepEntity.getConsumers().size() + removeEntity.getConsumers().size();

        final StitchingContext afterContext = entityStore.constructStitchingContext();
        stitchingManager.stitch(afterContext, journal);

        // There should be 2 less entities in the topology after stitching because we removed the
        assertEquals(numEntitiesBefore - 2, afterContext.size());

        // The keep entity should still be in the topology, the remove entity should be gone.
        final StitchingEntity mergeResult = afterContext.getStitchingGraph().entities()
            .filter(entity -> entity.getOid() == sharedStorageOid)
            .findFirst()
            .get();
        final StitchingEntity mergedDiskArray = afterContext.getStitchingGraph().entities()
            .filter(entity -> entity.getOid() == sharedDiskArrayOid)
            .findFirst()
            .get();

        assertEquals(keepEntity.getTargetId(), mergeResult.getTargetId());
        assertNotEquals(removeEntity.getTargetId(), mergeResult.getTargetId());
        assertEquals(numCombinedConsumers, keepEntity.getConsumers().size());

        assertThat(mergeResult.getProviders(), hasItem(mergedDiskArray));
    }

    private void replaceSharedStorageAndDiskArrayOid(@Nonnull final Map<Long, EntityDTO> entityMap) {
        final Map.Entry<Long, EntityDTO> sharedStorageEntry = entityMap.entrySet().stream()
            .filter(entry -> entry.getValue().getId().equals(sharedStorageId))
            .findFirst()
            .get();

        entityMap.put(sharedStorageOid, entityMap.remove(sharedStorageEntry.getKey()));

        final Map.Entry<Long, EntityDTO> sharedDiskArrayEntry = entityMap.entrySet().stream()
            .filter(entry -> entry.getValue().getId().equals(sharedDiskArrayId))
            .findFirst()
            .get();

        entityMap.put(sharedDiskArrayOid, entityMap.remove(sharedDiskArrayEntry.getKey()));
    }

    private void addEntities(@Nonnull final Map<Long, EntityDTO> entities, final long targetId,
                             final long discoveryTime)
        throws IdentityUninitializedException, IdentityMetadataMissingException, IdentityProviderException {
        final long probeId = 0;
        when(identityProvider.getIdsForEntities(
            eq(probeId), eq(new ArrayList<>(entities.values()))))
            .thenReturn(entities);
        when(entityClock.millis()).thenReturn(discoveryTime);

        entityStore.entitiesDiscovered(probeId, targetId,
                new ArrayList<>(entities.values()));
    }

    /**
     * Write out only the entities that are related to the entities to be stitched so that we can have a smaller
     * test file.
     *
     * @throws Exception If something goes wrong.
     */
    private void writeMinimizedTopology() throws Exception {
        final TopologyStitchingEntity startA = entityStore.constructStitchingContext()
            .getStitchingGraph()
            .entities()
            .filter(e -> e.getLocalId().equals("9bd4ee88-99c64661") && e.getTargetId() == targetAId)
            .findFirst()
            .get();

        final TopologyStitchingEntity startB = entityStore.constructStitchingContext()
            .getStitchingGraph()
            .entities()
            .filter(e -> e.getLocalId().equals("9bd4ee88-99c64661") && e.getTargetId() == targetBId)
            .findFirst()
            .get();

        final Set<StitchingEntity> aTree = new HashSet<>();
        visitNeighbors(startA, aTree, Collections.singleton(EntityType.PHYSICAL_MACHINE), StitchingEntity::getConsumers);
        visitNeighbors(startA, aTree, Collections.emptySet(), StitchingEntity::getProviders);
        startA.getTopologyCommoditiesSold().stream()
            .filter(commoditySold -> commoditySold.accesses != null)
            .forEach(commoditySold -> aTree.add(commoditySold.accesses));
        aTree.forEach(entity -> {
            if (entity.getEntityType() != EntityType.STORAGE && entity.getEntityType() != EntityType.DISK_ARRAY) {
                List<StitchingEntity> providersToRemove = new ArrayList<>();
                entity.getCommoditiesBoughtByProvider().keySet().forEach(key -> {
                    if (key.getOid() != sharedStorageOid) {
                        providersToRemove.add(key);
                    }
                });
                providersToRemove.forEach(provider -> entity.getCommoditiesBoughtByProvider().remove(provider));

                entity.getCommoditiesBoughtByProvider().clear();
                final TopologyStitchingEntity e = (TopologyStitchingEntity)entity;
                final List<CommoditySold> commoditiesSold = e.getCommoditiesSold()
                    .filter(commodity -> commodity.getCommodityType().name().toLowerCase().contains("storage"))
                    .map(c -> new CommoditySold(c, null))
                    .collect(Collectors.toList());
                e.setCommoditiesSold(commoditiesSold);
            }
        });

        final Set<StitchingEntity> bTree = new HashSet<>();
        visitNeighbors(startB, bTree, Collections.singleton(EntityType.PHYSICAL_MACHINE), StitchingEntity::getConsumers);
        visitNeighbors(startB, bTree, Collections.emptySet(), StitchingEntity::getProviders);
        startB.getTopologyCommoditiesSold().stream()
            .filter(commoditySold -> commoditySold.accesses != null)
            .forEach(commoditySold -> bTree.add(commoditySold.accesses));
        bTree.forEach(entity -> {
            if (entity.getEntityType() != EntityType.STORAGE && entity.getEntityType() != EntityType.DISK_ARRAY) {
                List<StitchingEntity> providersToRemove = new ArrayList<>();
                entity.getCommoditiesBoughtByProvider().keySet().forEach(key -> {
                    if (key.getOid() != sharedStorageOid) {
                        providersToRemove.add(key);
                    }
                });
                providersToRemove.forEach(provider -> entity.getCommoditiesBoughtByProvider().remove(provider));

                final TopologyStitchingEntity e = (TopologyStitchingEntity)entity;
                final List<CommoditySold> commoditiesSold = e.getCommoditiesSold()
                    .filter(commodity -> commodity.getCommodityType().name().toLowerCase().contains("storage"))
                    .map(c -> new CommoditySold(c, null))
                    .collect(Collectors.toList());
                e.setCommoditiesSold(commoditiesSold);
            }
        });

        writeSdkDtosToFile("a.json", aTree);
        writeSdkDtosToFile("b.json", bTree);
    }
}
