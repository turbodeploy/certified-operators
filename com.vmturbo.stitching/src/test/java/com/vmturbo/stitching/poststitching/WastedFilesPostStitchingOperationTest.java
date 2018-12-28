package com.vmturbo.stitching.poststitching;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

public class WastedFilesPostStitchingOperationTest {

    private final IStitchingJournal journal = mock(IStitchingJournal.class);

    private final WastedFilesPostStitchingOperation wastedFilesPostOp =
            new WastedFilesPostStitchingOperation();

    /**
     * Create a topology graph with one storage, two VMs, and the related VirtualVolumes.
     * Assign files to each volume so that there are 3 files that are used by a VM and 1 files
     * that is not used by any VM.  After the operation, the Volume corresponding to the wasted
     * files should only have that one file on it.  The other Volumes should be unchanged by the
     * operation.
     */
    @Test
    public void  testWastedFilesPostStitching() {
        final long storageOid = 111L;
        final long vm1Oid = 211L;
        final long vm2Oid = 212L;
        final long virtualVolume1Oid = 311L;
        final long virtualVolume2Oid = 312L;
        final long virtualVolumeWastedOid = 313L;
        final String [] allFiles = {"foo1", "foo2", "foo3", "foo4"};
        final String [] vm1Files = {"foo1"};
        final String [] vm2Files = {"foo2", "foo3"};

        final TopologyEntity.Builder storage =
                PostStitchingTestUtilities.makeTopologyEntityBuilder(
                        storageOid,
                        EntityType.STORAGE.getNumber(),
                        Collections.emptyList(),
                        Collections.emptyList());

        final TopologyEntity.Builder virtualVolume1 =
                PostStitchingTestUtilities.makeTopologyEntityBuilder(
                        virtualVolume1Oid,
                        EntityType.VIRTUAL_VOLUME.getNumber(),
                        Collections.emptyList(),
                        Collections.emptyList());

        final TopologyEntity.Builder virtualVolume2 =
                PostStitchingTestUtilities.makeTopologyEntityBuilder(
                        virtualVolume2Oid,
                        EntityType.VIRTUAL_VOLUME.getNumber(),
                        Collections.emptyList(),
                        Collections.emptyList());

        final TopologyEntity.Builder virtualVolumeWasted =
                PostStitchingTestUtilities.makeTopologyEntityBuilder(
                        virtualVolumeWastedOid,
                        EntityType.VIRTUAL_VOLUME.getNumber(),
                        Collections.emptyList(),
                        Collections.emptyList());

        final TopologyEntity.Builder virtualMachine1 =
                PostStitchingTestUtilities.makeTopologyEntityBuilder(
                        vm1Oid,
                        EntityType.VIRTUAL_MACHINE.getNumber(),
                        Collections.emptyList(),
                        Collections.emptyList());

        final TopologyEntity.Builder virtualMachine2 =
                PostStitchingTestUtilities.makeTopologyEntityBuilder(
                        vm2Oid,
                        EntityType.VIRTUAL_MACHINE.getNumber(),
                        Collections.emptyList(),
                        Collections.emptyList());

        virtualMachine1.addConnectedTo(virtualVolume1);
        virtualMachine2.addConnectedTo(virtualVolume2);
        virtualVolume1.addConnectedTo(storage);
        virtualVolume1.addConnectedFrom(virtualMachine1);
        virtualVolume2.addConnectedTo(storage);
        virtualVolume2.addConnectedFrom(virtualMachine2);
        virtualVolumeWasted.addConnectedTo(storage);
        storage.addConnectedFrom(virtualVolume1);
        storage.addConnectedFrom(virtualVolume2);
        storage.addConnectedFrom(virtualVolumeWasted);
        addFilesToVirtualVolume(virtualVolume1, vm1Files);
        addFilesToVirtualVolume(virtualVolume2, vm2Files);
        addFilesToVirtualVolume(virtualVolumeWasted, allFiles);

        TopologyEntity storageEntity = storage.build();
        TopologyEntity virtVol1 = virtualVolume1.build();
        TopologyEntity virtVol2 = virtualVolume2.build();
        TopologyEntity virtVolWasted = virtualVolumeWasted.build();
        TopologyEntity virtMachine1 = virtualMachine1.build();
        TopologyEntity virtMachine2 = virtualMachine2.build();
        assertTrue(virtMachine1.getConnectedToEntities().contains(virtVol1));
        assertTrue(virtMachine2.getConnectedToEntities().contains(virtVol2));
        assertTrue(virtVol1.getConnectedToEntities().contains(storageEntity));
        assertTrue(virtVol2.getConnectedToEntities().contains(storageEntity));
        assertTrue(virtVolWasted.getConnectedToEntities().contains(storageEntity));
        assertEquals(1, virtVol1.getTopologyEntityDtoBuilder()
                .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
        assertEquals(2, virtVol2.getTopologyEntityDtoBuilder()
                .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
        assertEquals(4, virtVolWasted.getTopologyEntityDtoBuilder()
                .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        wastedFilesPostOp.performOperation(
                Stream.of(storageEntity), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        assertEquals(1, virtVol1.getTopologyEntityDtoBuilder()
                .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
        assertEquals(2, virtVol2.getTopologyEntityDtoBuilder()
                .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
        assertEquals(1, virtVolWasted.getTopologyEntityDtoBuilder()
                .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
        assertEquals("foo4", virtVolWasted.getTopologyEntityDtoBuilder()
                .getTypeSpecificInfo().getVirtualVolume().getFiles(0).getPath());
        assertThat(virtVol1.getTopologyEntityDtoBuilder()
                        .getTypeSpecificInfo().getVirtualVolume().getFilesList().stream()
                        .map(VirtualVolumeFileDescriptor::getPath)
                        .collect(Collectors.toSet()),
                containsInAnyOrder(vm1Files));
        assertThat(virtVol2.getTopologyEntityDtoBuilder()
                        .getTypeSpecificInfo().getVirtualVolume().getFilesList().stream()
                        .map(VirtualVolumeFileDescriptor::getPath)
                        .collect(Collectors.toSet()),
                containsInAnyOrder(vm2Files));

    }

    /**
     * Add VirtualVolumeFileDescriptors corresponding to a list of path names to a VirtualVolume.
     *
     * @param builder A builder for the VirtualVolume's TopologyEntity
     * @param pathNames A String array with the pathnames of the files to add.
     */
    private void addFilesToVirtualVolume(@Nonnull TopologyEntity.Builder builder,
                                         @Nonnull String[] pathNames) {
        builder.getEntityBuilder().setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualVolume(
                        builder.getEntityBuilder()
                                .getTypeSpecificInfo()
                                .getVirtualVolume()
                                .toBuilder()
                                .addAllFiles(Stream.of(pathNames).map(pathName ->
                                        VirtualVolumeFileDescriptor.newBuilder().setPath(pathName)
                                                .build())
                                        .collect(Collectors.toList()))));
    }
}
