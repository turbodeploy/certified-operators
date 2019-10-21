package com.vmturbo.stitching.poststitching;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * Test ProtectSharedStorageWastedFilesPostStitchingOperation.
 */
public class ProtectSharedStorageWastedFilesPostStitchingOperationTest {

    private final IStitchingJournal journal = mock(IStitchingJournal.class);

    private final ProtectSharedStorageWastedFilesPostStitchingOperation protectWastedFilesPostOp =
        new ProtectSharedStorageWastedFilesPostStitchingOperation();

    private static final long storage1Oid = 111L;
    private static final long vm1Oid = 211L;
    private static final long virtualVolume1Oid = 311L;
    private static final long virtualVolume1WastedOid = 313L;
    private static final String[] storage1Files = {"/opt/dev/foo1.iso", "file2", "file3"};
    private static final String[] vm1Files = {"/opt/dev/foo1.iso"};
    private static TopologyEntity sharedStorageEntity;
    private static TopologyEntity virtualVolumeVmEntity;
    private static TopologyEntity virtVolWastedEntity;
    private static EntitySettingsCollection settingsCollection =
        mock(EntitySettingsCollection.class);

    /**
     * Created the test environment: A shared storage with two virtual volumes connected to it, one
     * of which is also connected to a VM.  The other virtual volume represents wasted storage.
     * The first virtual volume has one file associated with it and the wasted storage volume has
     * that file in addition to two other files associated with it.
     */
    @Before
    public void setup() {
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

        virtualMachine1.addConnectedTo(virtualVolume1);
        virtualVolume1.addConnectedTo(storage1);
        virtualVolume1.addConnectedFrom(virtualMachine1);
        virtualVolumeWasted1.addConnectedTo(storage1);
        storage1.addConnectedFrom(virtualVolume1);
        storage1.addConnectedFrom(virtualVolumeWasted1);
        PostStitchingTestUtilities.addFilesToVirtualVolume(virtualVolume1, vm1Files);
        PostStitchingTestUtilities.addFilesToVirtualVolume(virtualVolumeWasted1, storage1Files);

        sharedStorageEntity = storage1.build();
        virtualVolumeVmEntity = virtualVolume1.build();
        virtVolWastedEntity = virtualVolumeWasted1.build();
    }

    /**
     * Check that the {@link ProtectSharedStorageWastedFilesPostStitchingOperation} marks the
     * storage as ignoreWastedFiles == true.
     */
   @Test
   public void testProtectSharedStorageWastedFilesPostStitchingOperation() {
       UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
       // before operation, ignoreWastedFiles == false
       assertFalse(sharedStorageEntity.getTopologyEntityDtoBuilder()
           .getTypeSpecificInfo().getStorage().getIgnoreWastedFiles());
       protectWastedFilesPostOp.performOperation(
           Stream.of(sharedStorageEntity), settingsCollection, resultBuilder);
       resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
       // after the operation storage is marked as ignoreWastedFiles == true
       assertTrue(sharedStorageEntity.getTopologyEntityDtoBuilder()
           .getTypeSpecificInfo().getStorage().getIgnoreWastedFiles());
   }
}
