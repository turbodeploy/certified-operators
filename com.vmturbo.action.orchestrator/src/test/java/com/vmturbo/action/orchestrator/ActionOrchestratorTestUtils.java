package com.vmturbo.action.orchestrator;

import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.junit.Assert;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utility methods for Action Orchestrator tests.
 */
public class ActionOrchestratorTestUtils {

    private ActionOrchestratorTestUtils() {}

    public static final long TARGET_ID = 11;

    private static final AtomicLong idCounter = new AtomicLong();

    private static int entityType = 1;

    private static final int DEFAULT_ENTITY_TYPE = EntityType.VIRTUAL_MACHINE_VALUE;

    private static ActionTranslator actionTranslator = mock(ActionTranslator.class);
    private static ActionModeCalculator actionModeCalculator = new ActionModeCalculator(actionTranslator);

    @Nonnull
    public static Action createMoveAction(final long actionId, final long actionPlanId) {
        return new Action(createMoveRecommendation(actionId), actionPlanId, actionModeCalculator);
    }

    @Nonnull
    public static ActionDTO.Action createMoveRecommendation(final long actionId) {
        return createMoveRecommendation(actionId, idCounter.incrementAndGet(),
            idCounter.incrementAndGet(), entityType,
            idCounter.incrementAndGet(), entityType);
    }

    @Nonnull
    public static ActionDTO.Action createMoveRecommendation(final long actionId, final long targetId,
                                                            final long sourceId, final int sourceType,
                                                            final long destinationId, final int destType) {
        return baseAction(actionId)
                        .setInfo(
                            TestActionBuilder.makeMoveInfo(targetId, sourceId, sourceType,
                                destinationId, destType))
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
                    .setTarget(createActionEntity(targetId))))
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
            .setSupportingLevel(SupportLevel.SUPPORTED)
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

    public static Map<String, Setting> makeActionModeSetting(ActionMode mode) {
        return ImmutableMap.of("move", Setting.newBuilder()
                .setSettingSpecName("move")
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue(mode.toString()).build())
                .build());
    }

    public static ActionEntity createActionEntity(long id) {
        return ActionEntity.newBuilder()
                    .setId(id)
                     // set some fake type for now
                    .setType(DEFAULT_ENTITY_TYPE)
                    .build();
    }
}
