package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntity;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntityBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * DisableActionForHxControllerVmAndDsOperationTest.
 * Mark HX Storage controller VMs and their Storages as controllable false so we don't recommend
 * actions for This type of VMs and Storages.
 */
public class DisableActionForHxControllerVmAndDsOperationTest {
    private final DisableActionForHxControllerVmAndDsOperation operation =
            new DisableActionForHxControllerVmAndDsOperation();

    private static final EntitySettingsCollection settingsCollection = mock(EntitySettingsCollection.class);

    // Storage provider
    private static final TopologyEntity.Builder vcStorageProvider = makeTopologyEntityBuilder(
            EntityType.STORAGE_VALUE,
            "SpringpathDS-VC", 1L);

    private static final TopologyEntity.Builder hypervStorageProvider = makeTopologyEntityBuilder(
            EntityType.STORAGE_VALUE,
            "hyperv-_F:", 2L);

    private static final TopologyEntity.Builder otherStorageProvider = makeTopologyEntityBuilder(
            EntityType.STORAGE_VALUE,
            "other", 3L);

    private static final TopologyEntity.Builder vcStorageProvider2 = makeTopologyEntityBuilder(
            EntityType.STORAGE_VALUE,
            "SpringpathDS-VC2", 4L);

    private static final TopologyEntity.Builder vcStorageProvider3 = makeTopologyEntityBuilder(
            EntityType.STORAGE_VALUE,
            "SpringpathDS-VC2", 5L);

    // VC storage controller VM
    private static final TopologyEntity vcStCtlVM = makeTopologyEntity(EntityType.VIRTUAL_MACHINE_VALUE,
            "stCtlVm-VC", 6L,
            ImmutableList.of(vcStorageProvider));

    // Hyperv storage controller VM
    private static final TopologyEntity hypervStCtlVM = makeTopologyEntity(EntityType.VIRTUAL_MACHINE_VALUE,
            "StCtlVm-HyperV", 7L,
            ImmutableList.of(hypervStorageProvider));

    // other non-storage-controller VM
    private static final TopologyEntity otherNonStCtlVM = makeTopologyEntity(EntityType.VIRTUAL_MACHINE_VALUE,
            "OTHER", 8L,
            ImmutableList.of(otherStorageProvider));

    // other non-storage-controller VM
    private static final TopologyEntity otherNonStCtlVM2 = makeTopologyEntity(EntityType.VIRTUAL_MACHINE_VALUE,
            "OTHER2", 9L,
            ImmutableList.of(vcStorageProvider2));

    // VC storage-controller VM with a different name pattern
    private static final TopologyEntity vcStCtlVM2 = makeTopologyEntity(EntityType.VIRTUAL_MACHINE_VALUE,
            "hxCtlVm1", 10L,
            ImmutableList.of(vcStorageProvider3));

    /**
     * test DisableActionForHxControllerVmAndDsOperation with no entity.
     */
    @Test
    public void testNoEntities() {
        //test no Entities
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        operation.performOperation(Stream.empty(), settingsCollection, resultBuilder);
        assertTrue(resultBuilder.getChanges().isEmpty());
    }

    /**
     * test DisableActionForHxControllerVmAndDsOperation with VC StorageController Vm.
     */
    @Test
    public void testVcStorageControllerVm() {
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        operation.performOperation(Stream.of(vcStCtlVM), settingsCollection, resultBuilder);
        assertEquals(2, resultBuilder.getChanges().size());
    }

    /**
     * test DisableActionForHxControllerVmAndDsOperation with VC StorageController Vm.
     */
    @Test
    public void testOtherVcStorageControllerVm() {
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        operation.performOperation(Stream.of(vcStCtlVM2), settingsCollection, resultBuilder);
        assertEquals(2, resultBuilder.getChanges().size());
    }

    /**
     * test DisableActionForHxControllerVmAndDsOperation with hyperVStorageController Vm.
     */
    @Test
    public void testhyperVStorageControllerVm() {
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        operation.performOperation(Stream.of(hypervStCtlVM), settingsCollection, resultBuilder);
        assertEquals(2, resultBuilder.getChanges().size());
    }

    /**
     * test DisableActionForHxControllerVmAndDsOperation with other non-StorageController Vm.
     */
    @Test
    public void testNonStorageControllerVm() {
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        operation.performOperation(Stream.of(otherNonStCtlVM), settingsCollection, resultBuilder);
        assertEquals(0, resultBuilder.getChanges().size());
    }

    /**
     * test DisableActionForHxControllerVmAndDsOperation with other non-StorageController Vm.
     * VM name will match the pattern but DS name does not.
     */
    @Test
    public void testNonStorageControllerVm2() {
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        operation.performOperation(Stream.of(otherNonStCtlVM2), settingsCollection, resultBuilder);
        assertEquals(0, resultBuilder.getChanges().size());
    }
}