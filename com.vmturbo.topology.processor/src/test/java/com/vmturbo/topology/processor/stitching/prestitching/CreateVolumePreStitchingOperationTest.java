package com.vmturbo.topology.processor.stitching.prestitching;

import static com.vmturbo.stitching.prestitching.CreateVirtualVolumePreStitchingOperation.ID_SEPARATOR;
import static com.vmturbo.stitching.prestitching.CreateVirtualVolumePreStitchingOperation.STORAGE_BROWSING_ID_PREFIX;
import static com.vmturbo.stitching.prestitching.CreateVirtualVolumePreStitchingOperation.VIRTUAL_VOLUME_ID_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.prestitching.CreateVirtualVolumePreStitchingOperation;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.StitchingResultBuilder;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.util.Probes;

public class CreateVolumePreStitchingOperationTest {

    private StitchingContext stitchingContext;

    private StitchingResultBuilder resultBuilder;

    private TopologyStitchingEntity vmStitchingEntity1;

    private TopologyStitchingEntity storageStitchingEntity1;

    private TopologyStitchingEntity storageStitchingEntity2;

    private final StitchingJournal<StitchingEntity> journal = new StitchingJournal<>();

    private final IdentityProvider identityProvider = Mockito.mock(IdentityProviderImpl.class);

    private final CreateVirtualVolumePreStitchingOperation operation = Mockito.spy(
            new CreateVirtualVolumePreStitchingOperation());

    private final static String vmId1 = "vm1";
    private final static String stId1 = "st1";
    private final static String stId2 = "st2";
    private final static String expectedVolumeId1 = VIRTUAL_VOLUME_ID_PREFIX +
        STORAGE_BROWSING_ID_PREFIX + vmId1 + ID_SEPARATOR + STORAGE_BROWSING_ID_PREFIX + stId1;
    private final static String expectedVolumeId2 = VIRTUAL_VOLUME_ID_PREFIX +
        STORAGE_BROWSING_ID_PREFIX + vmId1 + ID_SEPARATOR + STORAGE_BROWSING_ID_PREFIX + stId2;
    private final static String expectedVolumeDisplayName1 = VIRTUAL_VOLUME_ID_PREFIX + vmId1 + ID_SEPARATOR + stId1;
    private final static String expectedVolumeDisplayName2 = VIRTUAL_VOLUME_ID_PREFIX + vmId1 + ID_SEPARATOR + stId2;

    private final static long targetId1 = 111L;
    private final static long probeId1 = 112L;
    private final static long volumeOid1 = 11L;
    private final static long volumeOid2 = 12L;

    private final EntityDTO.Builder vmDto1 = EntityDTO.newBuilder()
            .setId(vmId1)
            .setDisplayName(vmId1)
            .setEntityType(EntityType.VIRTUAL_MACHINE)
            .addCommoditiesBought(CommodityBought.newBuilder().setProviderId(stId1))
            .addCommoditiesBought(CommodityBought.newBuilder().setProviderId(stId2));

    private final EntityDTO.Builder storageDto1 = EntityDTO.newBuilder()
            .setId(stId1)
            .setDisplayName(stId1)
            .setEntityType(EntityType.STORAGE);

    private final EntityDTO.Builder storageDto2 = EntityDTO.newBuilder()
            .setId(stId2)
            .setDisplayName(stId2)
            .setEntityType(EntityType.STORAGE);

    private final EntityDTO expectedVolumeDto1 = EntityDTO.newBuilder()
        .setId(expectedVolumeId1)
        .setDisplayName(expectedVolumeDisplayName1)
        .setEntityType(EntityType.VIRTUAL_VOLUME)
        .build();

    private final EntityDTO expectedVolumeDto2 = EntityDTO.newBuilder()
        .setId(expectedVolumeId2)
        .setDisplayName(expectedVolumeDisplayName2)
        .setEntityType(EntityType.VIRTUAL_VOLUME)
        .build();

    private final StitchingEntityData vmEntity1 =
            StitchingEntityData.newBuilder(vmDto1)
                .oid(11)
                .targetId(targetId1)
                .lastUpdatedTime(100L)
                .build();

    private final StitchingEntityData stEntity1 =
            StitchingEntityData.newBuilder(storageDto1)
                .oid(21)
                .targetId(targetId1)
                .lastUpdatedTime(100L)
                .build();

    private final StitchingEntityData stEntity2 =
            StitchingEntityData.newBuilder(storageDto2)
                .oid(22)
                .targetId(targetId1)
                .lastUpdatedTime(100L)
                .build();

    @Before
    public void setup() throws Exception {
        // mock target
        TargetStore targetStore = Mockito.mock(TargetStore.class);
        ProbeStore probeStore = Mockito.mock(ProbeStore.class);
        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(Probes.defaultProbe));
        final TargetRESTApi.TargetSpec spec = new TargetRESTApi.TargetSpec(probeId1, Arrays.asList(
            new InputField("password", "ThePassValue", Optional.empty()),
            new InputField("user", "theUserName", Optional.empty()),
            new InputField("targetId", "targetId", Optional.empty())), Optional.empty());
        final Target target = new Target(targetId1, probeStore, spec.toDto(), false);
        Mockito.doReturn(Optional.of(target)).when(targetStore).getTarget(targetId1);
        Mockito.when(targetStore.getAll()).thenReturn(Collections.emptyList());

        StitchingContext.Builder stitchingContextBuilder = StitchingContext.newBuilder(3, targetStore)
            .setIdentityProvider(identityProvider);

        Map<String, StitchingEntityData> stitchingEntityDataMap = ImmutableMap.of(
            vmEntity1.getLocalId(), vmEntity1,
            stEntity1.getLocalId(), stEntity1,
            stEntity2.getLocalId(), stEntity2
        );

        stitchingContextBuilder.addEntity(vmEntity1, stitchingEntityDataMap);
        stitchingContextBuilder.addEntity(stEntity1, stitchingEntityDataMap);
        stitchingContextBuilder.addEntity(stEntity2, stitchingEntityDataMap);

        stitchingContext = stitchingContextBuilder.build();
        resultBuilder = new StitchingResultBuilder(stitchingContext);

        vmStitchingEntity1 = stitchingContext.getEntity(vmDto1).get();
        storageStitchingEntity1 = stitchingContext.getEntity(storageDto1).get();
        storageStitchingEntity2 = stitchingContext.getEntity(storageDto2).get();
    }

    @Test
    public void testNewVirtualVolumeCreated() throws Exception {
        // mock identity provider
        Mockito.doReturn(ImmutableMap.of(volumeOid1, expectedVolumeDto1, volumeOid2, expectedVolumeDto2))
            .when(identityProvider).getIdsForEntities(Mockito.anyLong(), Mockito.anyListOf(EntityDTO.class));

        // before pre-stitching
        assertEquals(3, stitchingContext.size());

        // perform operation
        operation.performOperation(Stream.of(vmStitchingEntity1), resultBuilder).getChanges()
            .forEach(change -> change.applyChange(journal));

        // there are a total of 5 entities in graph after virtual volumes are created
        assertEquals(5, stitchingContext.size());

        // 5 entities for target1
        assertEquals(5, stitchingContext.internalEntities(targetId1).count());

        // there are 2 virtual volumes
        assertEquals(2, stitchingContext.getEntitiesOfType(EntityType.VIRTUAL_VOLUME).count());

        // find volumes
        TopologyStitchingEntity volumeStitchingEntity1 = stitchingContext.getEntitiesOfType(EntityType.VIRTUAL_VOLUME)
            .filter(entity -> entity.getOid() == volumeOid1).findAny().get();
        TopologyStitchingEntity volumeStitchingEntity2 = stitchingContext.getEntitiesOfType(EntityType.VIRTUAL_VOLUME)
            .filter(entity -> entity.getOid() == volumeOid2).findAny().get();

        // check displayName of new created volumes
        assertEquals(expectedVolumeDisplayName1, volumeStitchingEntity1.getDisplayName());
        assertEquals(expectedVolumeDisplayName2, volumeStitchingEntity2.getDisplayName());

        // check that vm1 is connected to volume1 and volume2
        assertEquals(1, vmStitchingEntity1.getConnectedToByType().size());
        assertEquals(2, vmStitchingEntity1.getConnectedToByType().get(ConnectionType.NORMAL_CONNECTION).size());
        assertTrue(vmStitchingEntity1.getConnectedToByType().get(ConnectionType.NORMAL_CONNECTION).contains(volumeStitchingEntity1));
        assertTrue(vmStitchingEntity1.getConnectedToByType().get(ConnectionType.NORMAL_CONNECTION).contains(volumeStitchingEntity2));

        // check that volume id is set on the commodity bought set of vm1
        assertEquals(Long.valueOf(volumeOid1), vmStitchingEntity1.getCommodityBoughtListByProvider().get(storageStitchingEntity1).get(0).getVolumeId());
        assertEquals(Long.valueOf(volumeOid2), vmStitchingEntity1.getCommodityBoughtListByProvider().get(storageStitchingEntity2).get(0).getVolumeId());

        // check that volume1 is connected to storage1
        assertEquals(1, volumeStitchingEntity1.getConnectedToByType().size());
        assertEquals(1, volumeStitchingEntity1.getConnectedToByType().get(ConnectionType.NORMAL_CONNECTION).size());
        assertTrue(volumeStitchingEntity1.getConnectedToByType().get(ConnectionType.NORMAL_CONNECTION).contains(storageStitchingEntity1));

        // check that volume2 is connected to storage2
        assertEquals(1, volumeStitchingEntity2.getConnectedToByType().size());
        assertEquals(1, volumeStitchingEntity2.getConnectedToByType().get(ConnectionType.NORMAL_CONNECTION).size());
        assertTrue(volumeStitchingEntity2.getConnectedToByType().get(ConnectionType.NORMAL_CONNECTION).contains(storageStitchingEntity2));
    }
    @Test
    public void testNoVirtualVolumeCreatedIfAlreadyExisting() throws Exception {
        // mock identity provider
        Mockito.doReturn(ImmutableMap.of(volumeOid2, expectedVolumeDto2))
            .when(identityProvider).getIdsForEntities(Mockito.anyLong(), Mockito.anyListOf(EntityDTO.class));

        // set up the env that there is already volume between vm1 and storage1, but not vm1 and storage 2
        TopologyStitchingEntity volumeStitchingEntity1 = new TopologyStitchingEntity(
            expectedVolumeDto1.toBuilder(), volumeOid1, targetId1, 0);
        vmStitchingEntity1.addConnectedTo(ConnectionType.NORMAL_CONNECTION, volumeStitchingEntity1);
        volumeStitchingEntity1.addConnectedTo(ConnectionType.NORMAL_CONNECTION, storageStitchingEntity1);

        // perform operation
        operation.performOperation(Stream.of(vmStitchingEntity1), resultBuilder).getChanges().forEach(
            change -> change.applyChange(journal));

        // verify that no new volume is created for vm1 and storage1
        Mockito.verify(operation, Mockito.never()).createVirtualVolume(vmStitchingEntity1, storageStitchingEntity1);
        // verify that new volume is created for vm1 and storage2
        Mockito.verify(operation).createVirtualVolume(vmStitchingEntity1, storageStitchingEntity2);
    }
}
