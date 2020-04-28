package com.vmturbo.topology.processor.stitching.prestitching;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageData;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.prestitching.StorageVolumePreStitchingOperation;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.StitchingResultBuilder;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.targets.TargetStore;

public class StorageVolumePreStitchingOperationTest {

    private StitchingContext stitchingContext;

    private StitchingResultBuilder resultBuilder;

    private StitchingEntity storageStitchingEntity1;

    private StitchingEntity storageStitchingEntity2;

    private StitchingEntity storageVolumeStitchingEntity1;

    private StitchingEntity storageVolumeStitchingEntity2;

    final StitchingJournal<StitchingEntity> journal = new StitchingJournal<>();

    private final StorageVolumePreStitchingOperation
            operation = new StorageVolumePreStitchingOperation();

    private final EntityDTO.Builder storageDto1 = EntityDTO.newBuilder()
            .setId("st1")
            .setEntityType(EntityType.STORAGE)
            .setStorageData(StorageData.newBuilder()
                    .addExternalName("1001A"));

    private final EntityDTO.Builder storageDto2 = EntityDTO.newBuilder()
            .setId("st2")
            .setEntityType(EntityType.STORAGE)
            .setStorageData(StorageData.newBuilder()
                    .addExternalName("1002A"));

    private final EntityDTO.Builder storageVolumeDto1 = EntityDTO.newBuilder()
            .setId("sv1")
            .setEntityType(EntityType.STORAGE_VOLUME)
            .setStorageData(StorageData.newBuilder()
                    .addExternalName("1001A")
                    .addExternalName("1001B"));

    private final EntityDTO.Builder storageVolumeDto2 = EntityDTO.newBuilder()
            .setId("sv2")
            .setEntityType(EntityType.STORAGE_VOLUME)
            .setStorageData(StorageData.newBuilder()
                    .addExternalName("1003A")
                    .addExternalName("1003B"));

    private final StitchingEntityData storageEntity1 =
            StitchingEntityData.newBuilder(storageDto1)
                    .oid(11)
                    .targetId(111L)
                    .lastUpdatedTime(100L)
                    .build();

    private final StitchingEntityData storageEntity2 =
            StitchingEntityData.newBuilder(storageDto2)
                    .oid(12)
                    .targetId(111L)
                    .lastUpdatedTime(100L)
                    .build();

    private final StitchingEntityData storageVolumeEntity1 =
            StitchingEntityData.newBuilder(storageVolumeDto1)
                    .oid(21)
                    .targetId(112L)
                    .lastUpdatedTime(101L)
                    .build();

    private final StitchingEntityData storageVolumeEntity2 =
            StitchingEntityData.newBuilder(storageVolumeDto2)
                    .oid(22)
                    .targetId(112L)
                    .lastUpdatedTime(101L)
                    .build();

    @Before
    public void setup() {
        TargetStore targetStore = Mockito.mock(TargetStore.class);
        Mockito.when(targetStore.getAll()).thenReturn(Collections.emptyList());
        StitchingContext.Builder stitchingContextBuider = StitchingContext.newBuilder(4, targetStore)
            .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));

        stitchingContextBuider.addEntity(storageEntity1,
                ImmutableMap.of(storageEntity1.getLocalId(), storageEntity1));
        stitchingContextBuider.addEntity(storageEntity2,
                ImmutableMap.of(storageEntity2.getLocalId(), storageEntity2));
        stitchingContextBuider.addEntity(storageVolumeEntity1,
                ImmutableMap.of(storageVolumeEntity1.getLocalId(), storageVolumeEntity1));
        stitchingContextBuider.addEntity(storageVolumeEntity2,
                ImmutableMap.of(storageVolumeEntity2.getLocalId(), storageVolumeEntity2));

        stitchingContext = stitchingContextBuider.build();
        resultBuilder = new StitchingResultBuilder(stitchingContext);

        storageStitchingEntity1 = stitchingContext.getEntity(storageDto1).get();
        storageStitchingEntity2 = stitchingContext.getEntity(storageDto2).get();
        storageVolumeStitchingEntity1 = stitchingContext.getEntity(storageVolumeDto1).get();
        storageVolumeStitchingEntity2 = stitchingContext.getEntity(storageVolumeDto2).get();
    }

    @Test
    public void testExternalNamesUpdated() {
        // before pre-stitching
        assertEquals(4, stitchingContext.size());

        // perform operation
        operation.performOperation(Stream.of(storageStitchingEntity1, storageStitchingEntity2,
                storageVolumeStitchingEntity1, storageVolumeStitchingEntity2), resultBuilder)
                .getChanges().forEach(change -> change.applyChange(journal));

        // storage volumes are removed, only storages are left
        assertEquals(2, stitchingContext.size());
        assertTrue(stitchingContext.hasEntity(storageStitchingEntity1));
        assertTrue(stitchingContext.hasEntity(storageStitchingEntity2));

        // storage1 externalNames are updated
        List<String> externalNamesSt1 = storageStitchingEntity1.getEntityBuilder().getStorageData()
                .getExternalNameList();
        assertEquals(2, externalNamesSt1.size());
        assertThat(externalNamesSt1, containsInAnyOrder("1001A", "1001B"));

        // storage2 externalNames are not updated
        List<String> externalNamesSt2 = storageStitchingEntity2.getEntityBuilder().getStorageData()
                .getExternalNameList();
        assertEquals(1, externalNamesSt2.size());
        assertThat(externalNamesSt2, containsInAnyOrder("1002A"));
    }
}
