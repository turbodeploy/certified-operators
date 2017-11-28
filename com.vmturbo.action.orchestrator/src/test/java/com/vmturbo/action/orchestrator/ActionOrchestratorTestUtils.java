package com.vmturbo.action.orchestrator;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.junit.Assert;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Utility methods for Action Orchestrator tests.
 */
public class ActionOrchestratorTestUtils {

    private ActionOrchestratorTestUtils() {}

    public static final long TARGET_ID = 11;

    private static final AtomicLong idCounter = new AtomicLong();

    @Nonnull
    public static Action createMoveAction(final long actionId, final long actionPlanId) {
        return new Action(createMoveRecommendation(actionId), actionPlanId);
    }

    @Nonnull
    public static ActionDTO.Action createMoveRecommendation(final long actionId) {
        return createMoveRecommendation(actionId, idCounter.incrementAndGet(), idCounter.incrementAndGet(),
            idCounter.incrementAndGet());
    }

    @Nonnull
    public static ActionDTO.Action createMoveRecommendation(final long actionId, final long targetId,
                                                            final long sourceId, final long destinationId) {
        return baseAction(actionId)
                .setInfo(ActionDTO.ActionInfo.newBuilder()
                        .setMove(ActionDTO.Move.newBuilder()
                            .setSourceId(sourceId)
                            .setDestinationId(destinationId)
                            .setTargetId(targetId)))
                .build();
    }

    @Nonnull
    public static ActionDTO.Action createResizeRecommendation(final long actionId,
                                                              final long targetId,
                                                              @Nonnull final CommodityDTO.CommodityType resizeCommodity,
                                                              final double oldCapacity,
                                                              final double newCapacity) {
        return baseAction(actionId)
            .setInfo(ActionDTO.ActionInfo.newBuilder()
                .setResize(ActionDTO.Resize.newBuilder()
                    .setCommodityType(CommodityType.newBuilder().setType(resizeCommodity.getNumber()))
                    .setOldCapacity((float)oldCapacity)
                    .setNewCapacity((float)newCapacity)
                    .setTargetId(targetId)))
            .build();
    }

    @Nonnull
    public static ActionDTO.Action createResizeRecommendation(final long actionId,
                                                              @Nonnull final CommodityDTO.CommodityType resizeCommodity) {
        return createResizeRecommendation(actionId, TARGET_ID, resizeCommodity, 1.0, 2.0);
    }

    @Nonnull
    public static ActionSpec resizeActionSpec(final long actionId, final long actionPlanId) {
        return baseActionSpec(actionPlanId)
                .setRecommendation(createResizeRecommendation(actionId, CommodityDTO.CommodityType.VMEM))
                .build();
    }

    @Nonnull
    public static ActionSpec moveActionSpec(final long actionId, final long actionPlanId) {
        return baseActionSpec(actionPlanId)
                .setRecommendation(createMoveRecommendation(actionId))
                .build();
    }

    public static void assertActionsEqual(@Nonnull final Action expected,
                                    @Nonnull final Action got) {
        Assert.assertEquals(expected.getId(), got.getId());
        Assert.assertEquals(expected.getRecommendation(), got.getRecommendation());
        Assert.assertEquals(expected.getRecommendationTime(), got.getRecommendationTime());
        Assert.assertEquals(expected.getState(), got.getState());
        Assert.assertEquals(expected.getActionPlanId(), got.getActionPlanId());
        if (expected.getExecutableStep().isPresent()) {
            Assert.assertTrue(got.getExecutableStep().isPresent());
            assertExecutionStepsEqual(expected.getExecutableStep().get(), got.getExecutableStep().get());
        } else {
            Assert.assertFalse(got.getExecutableStep().isPresent());
        }

        Assert.assertEquals(expected.getDecision(), got.getDecision());
    }

    public static void assertExecutionStepsEqual(@Nonnull final ExecutableStep expected,
                                           @Nonnull final ExecutableStep got) {
        Assert.assertEquals(expected.getTargetId(), got.getTargetId());
        Assert.assertEquals(expected.getProgressPercentage(), got.getProgressPercentage());
        Assert.assertEquals(expected.getCompletionTime(), got.getCompletionTime());
        Assert.assertEquals(expected.getEnqueueTime(), got.getEnqueueTime());
        Assert.assertEquals(expected.getErrors(), got.getErrors());
        Assert.assertEquals(expected.getStartTime(), got.getStartTime());
        Assert.assertEquals(expected.getStatus(), got.getStatus());
        Assert.assertEquals(expected.getProgressDescription(), got.getProgressDescription());
    }

    private static ActionDTO.Action.Builder baseAction(final long actionId) {
        return ActionDTO.Action.newBuilder()
                .setId(actionId)
                .setImportance(0)
                .setExecutable(true)
                .setExplanation(Explanation.newBuilder().build());
    }

    private static ActionSpec.Builder baseActionSpec(final long actionPlanId) {
        return ActionSpec.newBuilder()
                .setActionPlanId(actionPlanId)
                .setRecommendationTime(System.currentTimeMillis())
                .setActionMode(ActionMode.MANUAL)
                .setIsExecutable(true);
    }

    public static List<Setting> makeActionModeSetting(ActionMode mode) {
        return Collections.singletonList(Setting.newBuilder()
                .setSettingSpecName("move")
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue(mode.toString()).build())
                .build());
    }

}
