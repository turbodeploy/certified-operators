package com.vmturbo.topology.processor.actions.data;

import static com.vmturbo.topology.processor.actions.ActionExecutionTestUtils.createActionEntity;

import java.util.List;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.ContextData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class DataRequirementSpecTest {

    @Test
    public void testContainerResizeSpec() {
        final String vappUuid = "0001238566";
        // Create a spec for container resize
        DataRequirementSpec spec = new DataRequirementSpecBuilder()
                .addMatchCriteria(actionInfo -> actionInfo.hasResize())
                .addMatchCriteria(actionInfo -> EntityType.CONTAINER.equals(
                        EntityType.forNumber(actionInfo.getResize().getTarget().getType())))
                .addDataRequirement(SDKConstants.VAPP_UUID, actionInfo -> vappUuid)
                .build();

        // Create a move action info that won't match the spec and assert that it doesn't match
        final ActionDTO.ActionInfo moveActionInfo = ActionInfo.newBuilder()
                .setMove(ActionDTO.Move.newBuilder()
                        .setTarget(createActionEntity(7, EntityType.VIRTUAL_MACHINE))
                        .addChanges(ChangeProvider.newBuilder()
                                .setSource(createActionEntity(2, EntityType.PHYSICAL_MACHINE))
                                .setDestination(createActionEntity(3, EntityType.PHYSICAL_MACHINE))))
                .build();
        Assert.assertFalse(spec.matchesAllCriteria(moveActionInfo));

        // Create a resize action info that won't match the spec and assert that it doesn't match
        final ActionDTO.ActionInfo vmResizeActionInfo = ActionInfo.newBuilder()
                .setResize(ActionDTO.Resize.newBuilder()
                        .setTarget(createActionEntity(7, EntityType.VIRTUAL_MACHINE))
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.MEM_VALUE)
                                .setKey("key"))
                        .setOldCapacity(10)
                        .setNewCapacity(20))
                .build();
        Assert.assertFalse(spec.matchesAllCriteria(vmResizeActionInfo));

        // Create an action info that will match the spec and assert that it does match
        final ActionDTO.ActionInfo containerResizeActionInfo = ActionInfo.newBuilder()
                .setResize(ActionDTO.Resize.newBuilder()
                        .setTarget(createActionEntity(7, EntityType.CONTAINER))
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.MEM_VALUE)
                                .setKey("key"))
                        .setOldCapacity(10)
                        .setNewCapacity(20))
                .build();
        Assert.assertTrue(spec.matchesAllCriteria(containerResizeActionInfo));

        // Retrieve the context data for the matching action and assert that the vappUuid is there
        List<ContextData> contextDataList = spec.retrieveRequiredData(containerResizeActionInfo);
        Assert.assertFalse(contextDataList.isEmpty());
        final Optional<ContextData> vappUuidContextData = contextDataList.stream()
                .filter(contextData -> SDKConstants.VAPP_UUID.equals(contextData.getContextKey()))
                .findFirst();
        Assert.assertTrue(vappUuidContextData.isPresent());
        // Check that the proper value was provided for the vappUuid
        Assert.assertEquals(vappUuid, vappUuidContextData.get().getContextValue());
    }
}
