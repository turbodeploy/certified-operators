package com.vmturbo.mediation.aws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.services.ec2.model.VolumeType;

import org.junit.Test;

import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData;

/**
 * Test the XL-only functionality within the MoveVolumeExecutor.
 */
public class MoveVolumeExecutorTest {

    private static final String TEST_REGION_NAME = "test-region-name";
    private static final String TEST_VOLUME_ID = "test-volume-id";
    private static final String TEST_ORIGINAL_PROBE_ID = String.join(
        MoveVolumeExecutor.PROBE_ID_SEPARATOR, "aws", TEST_REGION_NAME, "VL", TEST_VOLUME_ID);
    private static final double TEST_VOLUME_SIZE = 12345.0;
    private static final VolumeType TEST_VOLUME_TYPE = VolumeType.St1;
    private static final String TEST_ACTION_UUID = "test-action-uuid";
    private static final String TEST_TIER_ID = "test-tier-id";
    private static final String TEST_ASSOCIATED_VM_ID = "associated-vm-id";
    private static final String TEST_ENTITY_PROPERTY_NAMESPACE = "test-entity-property-namespace";
    private static final float DELTA = .001f;

    /**
     * Test for volumes without an associated VM property.
     */
    @Test
    public void testVolumeWithoutVM() {

        final EntityDTO target = EntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_VOLUME)
            .setId(TEST_ORIGINAL_PROBE_ID)
            .setVirtualVolumeData(VirtualVolumeData.newBuilder()
                .setStorageAmountCapacity((float)
                    (TEST_VOLUME_SIZE * MoveVolumeExecutor.MEBIBYTES_PER_GIBIBYTE)))
            .build();

        final ActionItemDTO actionItemDTO = ActionItemDTO.newBuilder()
            .setUuid(TEST_ACTION_UUID)
            .setActionType(ActionType.MOVE)
            .setTargetSE(target)
            .build();

        final EntityDTO tier = EntityDTO.newBuilder()
            .setId(TEST_TIER_ID)
            .setEntityType(EntityType.STORAGE_TIER)
            .setDisplayName(TEST_VOLUME_TYPE.toString().toUpperCase())
            .build();

        final MoveVolumeExecutor moveVolumeExecutor = new MoveVolumeExecutor();
        assertEquals(ActionType.MOVE, moveVolumeExecutor.getActionType());
        assertEquals(EntityType.VIRTUAL_VOLUME, moveVolumeExecutor.getEntityType());

        assertEquals(TEST_REGION_NAME, moveVolumeExecutor.getRegionName(actionItemDTO));

        assertEquals(MoveVolumeExecutor.VM_UNAVAILABLE_MESSAGE,
            moveVolumeExecutor.getVirtualMachineInstanceId(actionItemDTO));

        assertTrue(moveVolumeExecutor.getVolumeId(actionItemDTO).isPresent());
        assertEquals(TEST_VOLUME_ID, moveVolumeExecutor.getVolumeId(actionItemDTO).get());

        assertTrue(moveVolumeExecutor.getVolumeSize(actionItemDTO).isPresent());
        assertEquals(TEST_VOLUME_SIZE,
            moveVolumeExecutor.getVolumeSize(actionItemDTO).get().floatValue(), DELTA);

        assertTrue(moveVolumeExecutor.getVolumeType(tier).isPresent());
        assertEquals(TEST_VOLUME_TYPE, moveVolumeExecutor.getVolumeType(tier).get());
    }

    /**
     * Test for volumes with an associated VM property.
     */
    @Test
    public void testVolumeWithVM() {
        final MoveVolumeExecutor moveVolumeExecutor = new MoveVolumeExecutor();

        final EntityDTO target = EntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_VOLUME)
            .setId(TEST_ORIGINAL_PROBE_ID)
            .addEntityProperties(EntityProperty.newBuilder()
                .setNamespace(TEST_ENTITY_PROPERTY_NAMESPACE)
                .setName(MoveVolumeExecutor.ASSOCIATED_VIRTUAL_MACHINE_PROPERTY)
                .setValue(TEST_ASSOCIATED_VM_ID))
            .build();

        final ActionItemDTO actionItem = ActionItemDTO.newBuilder()
            .setUuid(TEST_ACTION_UUID)
            .setActionType(ActionType.MOVE)
            .setTargetSE(target)
            .build();

        assertEquals(TEST_ASSOCIATED_VM_ID,
            moveVolumeExecutor.getVirtualMachineInstanceId(actionItem));
    }
}
