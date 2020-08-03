package com.vmturbo.topology.processor.stitching.prestitching;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.prestitching.SharedVirtualVolumePreStitchingOperation;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.stitching.StitchingResultBuilder;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournal;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test the functionality of the SharedVolumePreStitchingOperation.
 */
public class SharedVolumePreStitchingOperationTest {
    private final String oid1 = "firstVolume";
    private final String oid2 = "secondVolume";
    private final String commonFile = "wastedFile";
    private final String[] volume1Files = {"foo1", "bar1", commonFile};
    private final String[] volume2Files = {commonFile, "foo2", "bar2"};

    private final EntityDTO.Builder oldVolumeDto = EntityDTO.newBuilder()
        .setId(oid1)
        .setDisplayName(oid1)
        .setEntityType(EntityType.VIRTUAL_VOLUME)
        .setVirtualVolumeData(VirtualVolumeData.newBuilder()
            .addAllFile(Arrays.stream(volume1Files)
                .map(path -> VirtualVolumeFileDescriptor.newBuilder().setPath(path).build())
                .collect(Collectors.toList()))
            .build());

    private final EntityDTO.Builder newVolumeDto = EntityDTO.newBuilder()
        .setId(oid2)
        .setDisplayName(oid2)
        .setEntityType(EntityType.VIRTUAL_VOLUME)
        .setVirtualVolumeData(VirtualVolumeData.newBuilder()
            .addAllFile(Arrays.stream(volume2Files)
                .map(path -> VirtualVolumeFileDescriptor.newBuilder().setPath(path).build())
                .collect(Collectors.toList()))
            .build());

    private StitchingResultBuilder resultBuilder;

    private StitchingEntity oldVolume;
    private StitchingEntity newVolume;
    private StitchingContext stitchingContext;

    final SharedVirtualVolumePreStitchingOperation operation =
        new SharedVirtualVolumePreStitchingOperation();

    /**
     * Set up the test environment.
     */
    @Before
    public void setup() {
        setupEntities(oldVolumeDto, newVolumeDto);

        oldVolume = stitchingContext.getEntity(oldVolumeDto).get();
        newVolume = stitchingContext.getEntity(newVolumeDto).get();
    }

    /**
     * Test that the pre stitching operation kept the newer volume but kept the display name that
     * comes first lexicographically.
     */
    @Test
    public void testVolumeDisplayNameKeepsLexicographicallyFirst() {
        assertEquals(2, stitchingContext.size());
        operation.performOperation(Stream.of(newVolume, oldVolume), resultBuilder)
            .getChanges().forEach(change -> change.applyChange(new StitchingJournal<>()));

        assertEquals(1, stitchingContext.size());
        assertTrue(stitchingContext.hasEntity(newVolume));
        assertFalse(stitchingContext.hasEntity(oldVolume));

        assertEquals(oid1, newVolume.getDisplayName());
    }

    /**
     * Test that we intersected the filelists of the two volumes when we merged them.
     */
    @Test
    public void testVolumeFilelistIntersection() {
        operation.performOperation(Stream.of(newVolume, oldVolume), resultBuilder)
            .getChanges().forEach(change -> change.applyChange(new StitchingJournal<>()));

        assertEquals(1,
            newVolume.getEntityBuilder().getVirtualVolumeData().getFileCount());
        assertEquals(commonFile,
            newVolume.getEntityBuilder().getVirtualVolumeData().getFile(0).getPath());
    }

    private void setupEntities(@Nonnull final EntityDTO.Builder... entities) {
        final long targetIncrement = 111L;
        final long lastUpdatedIncrement = 100L;
        TargetStore targetStore = Mockito.mock(TargetStore.class);
        Mockito.when(targetStore.getAll()).thenReturn(Collections.emptyList());

        long oid = 1L;
        long targetId = targetIncrement;
        long lastUpdated = lastUpdatedIncrement;

        final List<StitchingEntityData> entityDataList = new ArrayList<>();
        for (EntityDTO.Builder dto : entities) {
            final StitchingEntityData stitchingData = StitchingEntityData.newBuilder(dto)
                .oid(oid)
                .targetId(targetId += targetIncrement)
                .lastUpdatedTime(lastUpdated += lastUpdatedIncrement)
                .build();

            entityDataList.add(stitchingData);
        }

        final StitchingContext.Builder builder = StitchingContext.newBuilder(entities.length, targetStore)
            .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));
        entityDataList.forEach(entity -> builder.addEntity(entity,
            ImmutableMap.of(entity.getLocalId(), entity)));

        stitchingContext = builder.build();
        resultBuilder = new StitchingResultBuilder(stitchingContext);
    }
}
