package com.vmturbo.stitching.poststitching;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
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

    private final static long storage1Oid = 111L;
    private final static long storage2Oid = 111L;
    private final static long vm1Oid = 211L;
    private final static long vm2Oid = 212L;
    private final static long virtualVolume1Oid = 311L;
    private final static long virtualVolume2Oid = 312L;
    private final static long virtualVolume1WastedOid = 313L;
    private final static long virtualVolume2WastedOid = 314L;
    private final static String ignoreFile = "/usr/local/foo4.log";
    private final static String ignoreDirFile = "/.snapshot/foo2/foo5";
    private final static String wastedFile = "/local/foo/bar.hlog";
    private final static String [] allFiles = {"/opt/dev/foo1.iso", "/etc/foo2.snap", "/usr/lib/foo3",
            ignoreFile, ignoreDirFile, wastedFile};
    private final static String [] vm1Files = {"/opt/dev/foo1.iso"};
    private final static String [] vm2Files = {"/etc/foo2.snap", "/usr/lib/foo3"};
    private final static Set<String> wastedFilesNoFiltering = Sets.newHashSet(wastedFile, ignoreFile,
            ignoreDirFile);
    private final static Set<String> wastedFilesAfterFiltering = Sets.newHashSet(vm1Files[0],
            vm2Files[0], vm2Files[1], wastedFile);
    private static TopologyEntity storageEntity1;
    private static TopologyEntity storageEntity2;
    private static TopologyEntity virtVol1;
    private static TopologyEntity virtVol2;
    private static TopologyEntity virtVolWasted1;
    private static TopologyEntity virtVolWasted2;
    private static EntitySettingsCollection settingsCollection = mock(EntitySettingsCollection.class);

    private static void initialSetupForMainTest() {
        // Create main test environment - Storage with three VirtualVolumes, 2 connected to VMs and
        // one for wasted storage.
        final TopologyEntity.Builder storage1 =
            PostStitchingTestUtilities.makeTopologyEntityBuilder(
                storage1Oid,
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

        final TopologyEntity.Builder virtualVolumeWasted1 =
            PostStitchingTestUtilities.makeTopologyEntityBuilder(
                virtualVolume1WastedOid,
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
        virtualVolume1.addConnectedTo(storage1);
        virtualVolume1.addConnectedFrom(virtualMachine1);
        virtualVolume2.addConnectedTo(storage1);
        virtualVolume2.addConnectedFrom(virtualMachine2);
        virtualVolumeWasted1.addConnectedTo(storage1);
        storage1.addConnectedFrom(virtualVolume1);
        storage1.addConnectedFrom(virtualVolume2);
        storage1.addConnectedFrom(virtualVolumeWasted1);
        addFilesToVirtualVolume(virtualVolume1, vm1Files);
        addFilesToVirtualVolume(virtualVolume2, vm2Files);
        addFilesToVirtualVolume(virtualVolumeWasted1, allFiles);

        storageEntity1 = storage1.build();
        virtVol1 = virtualVolume1.build();
        virtVol2 = virtualVolume2.build();
        virtVolWasted1 = virtualVolumeWasted1.build();
        TopologyEntity virtMachine1 = virtualMachine1.build();
        TopologyEntity virtMachine2 = virtualMachine2.build();
        assertTrue(virtMachine1.getConnectedToEntities().contains(virtVol1));
        assertTrue(virtMachine2.getConnectedToEntities().contains(virtVol2));
        assertTrue(virtVol1.getConnectedToEntities().contains(storageEntity1));
        assertTrue(virtVol2.getConnectedToEntities().contains(storageEntity1));
        assertTrue(virtVolWasted1.getConnectedToEntities().contains(storageEntity1));
        assertEquals(vm1Files.length, virtVol1.getTopologyEntityDtoBuilder()
            .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
        assertEquals(vm2Files.length, virtVol2.getTopologyEntityDtoBuilder()
            .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
        assertEquals(allFiles.length, virtVolWasted1.getTopologyEntityDtoBuilder()
            .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
    }

    /**
     * Setup environment to test filtering based on ignore files and directory settings.
     * Just a single storage with a single volume connected to it.  Volume has all the files
     * associated with it.
     */
    private static void initialSetupForIgnoreSettingsTest() {
        final TopologyEntity.Builder storage2 =
            PostStitchingTestUtilities.makeTopologyEntityBuilder(
                storage2Oid,
                EntityType.STORAGE.getNumber(),
                Collections.emptyList(),
                Collections.emptyList());

        final TopologyEntity.Builder virtualVolumeWasted2 =
            PostStitchingTestUtilities.makeTopologyEntityBuilder(
                virtualVolume2WastedOid,
                EntityType.VIRTUAL_VOLUME.getNumber(),
                Collections.emptyList(),
                Collections.emptyList());

        virtualVolumeWasted2.addConnectedTo(storage2);
        storage2.addConnectedFrom(virtualVolumeWasted2);
        addFilesToVirtualVolume(virtualVolumeWasted2, allFiles);
        storageEntity2 = storage2.build();
        virtVolWasted2 = virtualVolumeWasted2.build();
        assertTrue(virtVolWasted2.getConnectedToEntities().contains(storageEntity2));
        assertEquals(allFiles.length, virtVolWasted2.getTopologyEntityDtoBuilder()
            .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
    }

    /**
     * Create a topology graph with one storage, two VMs, and the related VirtualVolumes.
     * Assign files to each volume so that there are 3 files that are used by a VM and 1 files
     * that is not used by any VM.  After the operation, the Volume corresponding to the wasted
     * files should only have that one file on it.  The other Volumes should be unchanged by the
     * operation.
     */
    @Test
    public void testWastedFilesPostStitching() {
        initialSetupForMainTest();
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        // setup getEntitySetting to return empty filters
        StringSettingValue filterNothing = StringSettingValue.newBuilder().setValue("").build();
        Setting fileFilterNothing =
            Setting.newBuilder().setStringSettingValue(filterNothing).build();
        Setting directoryFilterNothing =
            Setting.newBuilder().setStringSettingValue(filterNothing).build();
        Mockito.when(settingsCollection.getEntitySetting(storage1Oid,
            EntitySettingSpecs.IgnoreDirectories))
            .thenReturn(Optional.of(directoryFilterNothing));
        Mockito.when(settingsCollection.getEntitySetting(storage1Oid,
            EntitySettingSpecs.IgnoreFiles)).thenReturn(Optional.of(fileFilterNothing));
        wastedFilesPostOp.performOperation(
            Stream.of(storageEntity1), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        assertEquals(vm1Files.length, virtVol1.getTopologyEntityDtoBuilder()
            .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
        assertEquals(vm2Files.length, virtVol2.getTopologyEntityDtoBuilder()
            .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
        assertEquals(wastedFilesNoFiltering.size(),
            virtVolWasted1.getTopologyEntityDtoBuilder().getTypeSpecificInfo()
                .getVirtualVolume().getFilesCount());
        assertEquals(wastedFilesNoFiltering, virtVolWasted1.getTopologyEntityDtoBuilder()
            .getTypeSpecificInfo().getVirtualVolume().getFilesList().stream()
            .map(VirtualVolumeFileDescriptor::getPath)
            .collect(Collectors.toSet()));
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
     * Test that the ignore settings work.  Set the file ignore to ignore .log files and set the
     * directory to the default value which will ignore .snapshot directories.  Make sure the
     * files we expect to be filtered out are.
     */
    @Test
    public void testIgnoreSettings() {
        initialSetupForIgnoreSettingsTest();
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        StringSettingValue filterLogFiles =
            StringSettingValue.newBuilder().setValue(".*\\.log").build();
        StringSettingValue filterDirectory =
            StringSettingValue.newBuilder()
                .setValue("\\.dvsData.*|\\.snapshot.*|\\.vSphere-HA.*|\\.naa.*|\\.etc.*|lost\\+found.*|stCtlVM-.*")
                .build();
        Setting fileFilterLogFiles =
            Setting.newBuilder().setStringSettingValue(filterLogFiles).build();
        Setting directoryFilterDevFoo1 =
            Setting.newBuilder().setStringSettingValue(filterDirectory).build();
        Mockito.when(settingsCollection.getEntitySetting(storage2Oid,
            EntitySettingSpecs.IgnoreDirectories))
            .thenReturn(Optional.of(directoryFilterDevFoo1));
        Mockito.when(settingsCollection.getEntitySetting(storage2Oid,
            EntitySettingSpecs.IgnoreFiles)).thenReturn(Optional.of(fileFilterLogFiles));
        wastedFilesPostOp.performOperation(
            Stream.of(storageEntity2), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        assertEquals(wastedFilesAfterFiltering, virtVolWasted2.getTopologyEntityDtoBuilder()
            .getTypeSpecificInfo().getVirtualVolume().getFilesList().stream()
            .map(VirtualVolumeFileDescriptor::getPath)
            .collect(Collectors.toSet()));
    }

    /**
     * Add VirtualVolumeFileDescriptors corresponding to a list of path names to a VirtualVolume.
     *
     * @param builder   A builder for the VirtualVolume's TopologyEntity
     * @param pathNames A String array with the pathnames of the files to add.
     */
    private static void addFilesToVirtualVolume(@Nonnull TopologyEntity.Builder builder,
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
