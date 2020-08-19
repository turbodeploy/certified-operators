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

import com.google.common.collect.Sets;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * Test the WastedFile post stitching operation.
 */
public class WastedFilesPostStitchingOperationTest {

    private final IStitchingJournal journal = mock(IStitchingJournal.class);

    private final WastedFilesPostStitchingOperation wastedFilesPostOp =
            new WastedFilesPostStitchingOperation();

    private static final long storage1Oid = 111L;
    private static final long storage2Oid = 112L;
    private static final long storage3Oid = 113L;
    private static final long vm1Oid = 211L;
    private static final long vm2Oid = 212L;
    private static final long virtualVolume1Oid = 311L;
    private static final long virtualVolume2Oid = 312L;
    private static final long virtualVolume1WastedOid = 313L;
    private static final long virtualVolume2WastedOid = 314L;
    private static final long virtualVolume3WastedOid = 315L;
    private static final String ignoreFile = "/usr/local/foo4.log";
    private static final String ignoreDirFile = "/.snapshot/foo2/foo5";
    private static final String wastedFile = "/local/foo/bar.hlog";
    private static final String[] allFiles = {"/opt/dev/foo1.iso", "/etc/foo2.snap", "/usr/lib/foo3",
            ignoreFile, ignoreDirFile, wastedFile};
    private static final String validPath3 = "/a8436c5c-b286-0137-b2a0-005056b8004b/xyz.hlog";
    private static final String[] otherLinks3 = {"/.snapshot/xyz.hlog", "/good/file.name"};
    private static final String[] vm1Files = {"/opt/dev/foo1.iso"};
    private static final String[] vm2Files = {"/etc/foo2.snap", "/usr/lib/foo3"};
    private static final Set<String> wastedFilesNoFiltering = Sets.newHashSet(wastedFile, ignoreFile,
            ignoreDirFile);
    private static final Set<String> wastedFilesAfterFiltering = Sets.newHashSet(vm1Files[0],
            vm2Files[0], vm2Files[1], wastedFile);
    private static final Set<String> wastedOtherPathFile = Sets.newHashSet(validPath3);
    private static TopologyEntity storageEntity1;
    private static TopologyEntity storageEntity2;
    private static TopologyEntity storageEntity3;
    private static TopologyEntity virtVol1;
    private static TopologyEntity virtVol2;
    private static TopologyEntity virtVolWasted1;
    private static TopologyEntity virtVolWasted2;
    private static TopologyEntity virtVolWasted3;
    private static EntitySettingsCollection settingsCollection = mock(EntitySettingsCollection.class);

    private void initialSetupForMainTest() {
        // Create main test environment - Storage with three VirtualVolumes, 2 connected to VMs and
        // one for wasted storage.
        final TopologyEntity.Builder storage1 =
            makeEntityBuilder(storage1Oid, EntityType.STORAGE);

        final TopologyEntity.Builder virtualVolume1 =
            makeEntityBuilder( virtualVolume1Oid, EntityType.VIRTUAL_VOLUME);

        final TopologyEntity.Builder virtualVolume2 =
            makeEntityBuilder(virtualVolume2Oid, EntityType.VIRTUAL_VOLUME);

        final TopologyEntity.Builder virtualVolumeWasted1 =
            makeEntityBuilder(virtualVolume1WastedOid, EntityType.VIRTUAL_VOLUME);

        final TopologyEntity.Builder virtualMachine1 =
            makeEntityBuilder(vm1Oid, EntityType.VIRTUAL_MACHINE);

        final TopologyEntity.Builder virtualMachine2 =
            makeEntityBuilder(vm2Oid, EntityType.VIRTUAL_MACHINE);

        connect(virtualMachine1, virtualVolume1);
        connect(virtualMachine2, virtualVolume2);
        connect(virtualVolume1, storage1);
        connect(virtualVolume2, storage1);
        connect(virtualVolumeWasted1, storage1);
        PostStitchingTestUtilities.addFilesToVirtualVolume(virtualVolume1, vm1Files);
        PostStitchingTestUtilities.addFilesToVirtualVolume(virtualVolume2, vm2Files);
        PostStitchingTestUtilities.addFilesToVirtualVolume(virtualVolumeWasted1,
            new String[] {wastedFile, ignoreFile, ignoreDirFile});

        storageEntity1 = storage1.build();
        virtVol1 = virtualVolume1.build();
        virtVol2 = virtualVolume2.build();
        virtVolWasted1 = virtualVolumeWasted1.build();
        TopologyEntity virtMachine1 = virtualMachine1.build();
        TopologyEntity virtMachine2 = virtualMachine2.build();
        assertTrue(virtMachine1.getOutboundAssociatedEntities().contains(virtVol1));
        assertTrue(virtMachine2.getOutboundAssociatedEntities().contains(virtVol2));
        assertTrue(virtVol1.getOutboundAssociatedEntities().contains(storageEntity1));
        assertTrue(virtVol2.getOutboundAssociatedEntities().contains(storageEntity1));
        assertTrue(virtVolWasted1.getOutboundAssociatedEntities().contains(storageEntity1));
        assertEquals(vm1Files.length, virtVol1.getTopologyEntityDtoBuilder()
            .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
        assertEquals(vm2Files.length, virtVol2.getTopologyEntityDtoBuilder()
            .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
        assertEquals(3, virtVolWasted1.getTopologyEntityDtoBuilder()
            .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
    }

    /**
     * Setup environment to test filtering based on ignore files and directory settings.
     * Just a single storage with a single volume connected to it.  Volume has all the files
     * associated with it.
     */
    private void initialSetupForIgnoreSettingsTest() {
        final TopologyEntity.Builder storage2 = makeEntityBuilder(storage2Oid, EntityType.STORAGE);

        final TopologyEntity.Builder virtualVolumeWasted2 =
            makeEntityBuilder(virtualVolume2WastedOid, EntityType.VIRTUAL_VOLUME);

        connect(virtualVolumeWasted2, storage2);
        PostStitchingTestUtilities.addFilesToVirtualVolume(virtualVolumeWasted2, allFiles);
        storageEntity2 = storage2.build();
        virtVolWasted2 = virtualVolumeWasted2.build();
        assertTrue(virtVolWasted2.getOutboundAssociatedEntities().contains(storageEntity2));
        assertEquals(allFiles.length, virtVolWasted2.getTopologyEntityDtoBuilder()
            .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
    }

    private TopologyEntity.Builder makeEntityBuilder(long oid, EntityType entityType) {
        return PostStitchingTestUtilities.makeTopologyEntityBuilder(
            oid,
            entityType.getNumber(),
            Collections.emptyList(),
            Collections.emptyList());
     }

     private void connect(TopologyEntity.Builder source, TopologyEntity.Builder dest) {
        source.addOutboundAssociation(dest);
        dest.addInboundAssociation(source);
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
     * Setup environment to test filtering of other paths based on ignore directory settings.
     * Just a single storage with a single volume connected to it.  Volume has all the files
     * associated with it.
     */
    private void initialSetupForIgnoreSettingsOtherPathsTest() {
        final TopologyEntity.Builder storage3 = makeEntityBuilder(storage3Oid, EntityType.STORAGE);

        final TopologyEntity.Builder virtualVolumeWasted3 =
            makeEntityBuilder(virtualVolume3WastedOid, EntityType.VIRTUAL_VOLUME);

        connect(virtualVolumeWasted3, storage3);
        PostStitchingTestUtilities
            .addFileToVirtualVolume(virtualVolumeWasted3, validPath3, otherLinks3);
        storageEntity3 = storage3.build();
        virtVolWasted3 = virtualVolumeWasted3.build();
        assertTrue(virtVolWasted3.getOutboundAssociatedEntities().contains(storageEntity3));
        assertEquals(1, virtVolWasted3.getTopologyEntityDtoBuilder()
            .getTypeSpecificInfo().getVirtualVolume().getFilesCount());
    }

    /**
     * Test that the ignore directories settings work for additional paths.  Set the
     * directory to the default value which will ignore .snapshot directories.  Make sure the
     * files we expect to be filtered out are.
     */
    @Test
    public void testIgnoreSettingsOtherPaths() {
        initialSetupForIgnoreSettingsOtherPathsTest();
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        StringSettingValue filterNothing = StringSettingValue.newBuilder().setValue("").build();
        Setting fileFilterNothing =
            Setting.newBuilder().setStringSettingValue(filterNothing).build();
        StringSettingValue filterDirectory =
            StringSettingValue.newBuilder()
                .setValue("\\.dvsData.*|\\.snapshot.*|\\.vSphere-HA.*|\\.naa.*|\\.etc.*|lost\\+found.*|stCtlVM-.*")
                .build();
        Setting directoryFilterDevFoo1 =
            Setting.newBuilder().setStringSettingValue(filterDirectory).build();
        Mockito.when(settingsCollection.getEntitySetting(storage3Oid,
            EntitySettingSpecs.IgnoreDirectories))
            .thenReturn(Optional.of(directoryFilterDevFoo1));
        Mockito.when(settingsCollection.getEntitySetting(storage3Oid,
            EntitySettingSpecs.IgnoreFiles)).thenReturn(Optional.of(fileFilterNothing));
        wastedFilesPostOp.performOperation(
            Stream.of(storageEntity3), settingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        assertEquals(Collections.emptySet(), virtVolWasted3.getTopologyEntityDtoBuilder()
            .getTypeSpecificInfo().getVirtualVolume().getFilesList().stream()
            .map(VirtualVolumeFileDescriptor::getPath)
            .collect(Collectors.toSet()));
    }
}
