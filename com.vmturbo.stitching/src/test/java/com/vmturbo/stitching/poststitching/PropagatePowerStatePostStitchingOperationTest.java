package com.vmturbo.stitching.poststitching;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * Test the PropagatePowerStatePostStitchingOperation.
 */
public class PropagatePowerStatePostStitchingOperationTest {

    private final IStitchingJournal journal = mock(IStitchingJournal.class);

    private static EntitySettingsCollection settingsCollection = mock(EntitySettingsCollection.class);

    private final PropagatePowerStatePostStitchingOperation propagatePowerStateOp =
            new PropagatePowerStatePostStitchingOperation();

    private static final long vmOid1 = 11L;
    private static final long vmOid2 = 12L;
    private static final long appOid1 = 21L;
    private static final long appOid2 = 22L;
    private static final long appOid3 = 23L;

    private static TopologyEntity vmEntity1;
    private static TopologyEntity vmEntity2;
    private static TopologyEntity appEntity1;
    private static TopologyEntity appEntity2;
    private static TopologyEntity appEntity3;

    @Before
    public void setup() {
        final TopologyEntity.Builder app1 = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(appOid1)
                .setDisplayName("RealApp")
                .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                .setEntityState(EntityState.POWERED_ON));

        final TopologyEntity.Builder app2 = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(appOid2)
                .setDisplayName("GuestLoad[vm1]")
                .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                .setEntityState(EntityState.POWERED_OFF)
                .putEntityPropertyMap(
                    GuestLoadAppPostStitchingOperation.APPLICATION_TYPE_PATH,
                    SupplyChainConstants.GUEST_LOAD));

        final TopologyEntity.Builder app3 = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(appOid3)
                .setDisplayName("GuestLoad[vm2]")
                .setEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                .setEntityState(EntityState.POWERED_ON)
                .putEntityPropertyMap(
                    GuestLoadAppPostStitchingOperation.APPLICATION_TYPE_PATH,
                    SupplyChainConstants.GUEST_LOAD));

        final TopologyEntity.Builder vm1 = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(vmOid1)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEntityState(EntityState.POWERED_OFF));

        final TopologyEntity.Builder vm2 = TopologyEntity.newBuilder(
            TopologyEntityDTO.newBuilder()
                .setOid(vmOid2)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEntityState(EntityState.POWERED_ON));

        // vm1 is powered off, which hosts two apps:
        // 1. app1 (powered on): real app discovered from acm target
        // 2. app2 (powered off): guestload app discovered from vc target, with same state as vm
        vm1.addConsumer(app1);
        vm1.addConsumer(app2);
        // vm2 is powered on, which hosts one app:
        // 1. app2 (powered on): guestload app discovered from vc target
        vm2.addConsumer(app3);

        appEntity1 = app1.build();
        appEntity2 = app2.build();
        appEntity3 = app3.build();
        vmEntity1 = vm1.build();
        vmEntity2 = vm2.build();
    }

    @Test
    public void testPropagatePowerState() {
        // before operation
        assertEquals(EntityState.POWERED_ON, appEntity1.getEntityState());
        assertEquals(EntityState.POWERED_OFF, appEntity2.getEntityState());
        assertEquals(EntityState.POWERED_ON, appEntity3.getEntityState());
        assertEquals(EntityState.POWERED_OFF, vmEntity1.getEntityState());
        assertEquals(EntityState.POWERED_ON, vmEntity2.getEntityState());

        // apply operation
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        propagatePowerStateOp.performOperation(Stream.of(vmEntity1, vmEntity2),
            settingsCollection, resultBuilder);
        // check that only one VM's consumers' states are changed
        assertEquals(1, resultBuilder.getChanges().size());
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));

        // after operation
        // verify that app1 is set to unknown, app2 is still powered off, since it's GuestLoad
        // and app3 is not changed, also vm1 and vm2 are not changed
        assertEquals(EntityState.UNKNOWN, appEntity1.getEntityState());
        assertEquals(EntityState.POWERED_OFF, appEntity2.getEntityState());
        assertEquals(EntityState.POWERED_ON, appEntity3.getEntityState());
        assertEquals(EntityState.POWERED_OFF, vmEntity1.getEntityState());
        assertEquals(EntityState.POWERED_ON, vmEntity2.getEntityState());
    }

    @Test
    public void testNotPropagatePowerState() {
        // apply operation
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        // if vm state is maintenance, it should not propagate
        vmEntity1.getTopologyEntityDtoBuilder().setEntityState(EntityState.MAINTENANCE);
        propagatePowerStateOp.performOperation(Stream.of(vmEntity1), settingsCollection, resultBuilder);
        // check that no changes applied
        assertEquals(0, resultBuilder.getChanges().size());
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        // after operation, verify that app1 and app2 states are not changed
        assertEquals(EntityState.POWERED_ON, appEntity1.getEntityState());
        assertEquals(EntityState.POWERED_OFF, appEntity2.getEntityState());
        assertEquals(EntityState.MAINTENANCE, vmEntity1.getEntityState());
    }
}
