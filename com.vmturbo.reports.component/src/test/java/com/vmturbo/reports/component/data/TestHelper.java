package com.vmturbo.reports.component.data;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;


/**
 * Helper class for creating test objects.
 */
public class TestHelper {
    @Nonnull
    public static ActionSpec resizeActionSpec(final long targetId) {
        return baseActionSpec(1L)
            .setRecommendation(createResizeRecommendation(targetId, 1L, CommodityDTO.CommodityType.VMEM))
            .build();
    }

    private static ActionSpec.Builder baseActionSpec(final long actionPlanId) {
        return ActionSpec.newBuilder()
            .setActionPlanId(actionPlanId)
            .setRecommendationTime(System.currentTimeMillis())
            .setActionMode(ActionMode.MANUAL)
            .setIsExecutable(true);
    }

    @Nonnull
    private static ActionDTO.Action createResizeRecommendation(final long targetId, final long actionId,
                                                               @Nonnull final CommodityDTO.CommodityType resizeCommodity) {
        return createResizeRecommendation(actionId, targetId, resizeCommodity, 1.0, 2.0);
    }

    @Nonnull
    private static ActionDTO.Action createResizeRecommendation(final long actionId,
                                                               final long targetId,
                                                               @Nonnull final CommodityDTO.CommodityType resizeCommodity,
                                                               final double oldCapacity,
                                                               final double newCapacity) {
        return baseAction(actionId)
            .setInfo(ActionDTO.ActionInfo.newBuilder()
                .setResize(ActionDTO.Resize.newBuilder()
                    .setCommodityType(CommodityType.newBuilder().setType(resizeCommodity.getNumber()))
                    .setOldCapacity((float) oldCapacity)
                    .setNewCapacity((float) newCapacity)
                    .setTarget(createActionEntity(targetId))))
            .build();
    }

    private static ActionDTO.Action.Builder baseAction(final long actionId) {
        return ActionDTO.Action.newBuilder()
            .setId(actionId)
            .setDeprecatedImportance(0)
            .setExecutable(true)
            .setExplanation(Explanation.newBuilder().build());
    }

    private static ActionEntity createActionEntity(long id) {
        return ActionEntity.newBuilder()
            .setId(id)
            // set some fake type for now
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();
    }
}
