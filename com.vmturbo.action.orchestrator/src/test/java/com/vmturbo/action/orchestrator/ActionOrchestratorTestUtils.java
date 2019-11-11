package com.vmturbo.action.orchestrator;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.translation.ActionTranslator.TranslationExecutor;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utility methods for Action Orchestrator tests.
 */
public class ActionOrchestratorTestUtils {

    private ActionOrchestratorTestUtils() {}

    private static final long TARGET_ID = 11;

    private static final AtomicLong idCounter = new AtomicLong();

    private static final int DEFAULT_ENTITY_TYPE = EntityType.VIRTUAL_MACHINE_VALUE;

    private static ActionModeCalculator actionModeCalculator = new ActionModeCalculator();

    private static final SettingPolicyServiceMole settingPolicyServiceMole = new SettingPolicyServiceMole();

    private static final GrpcTestServer grpcTestServer = GrpcTestServer.newServer(settingPolicyServiceMole);

    static {
        try {
            grpcTestServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Nonnull
    public static ActionTranslator passthroughTranslator() {
        return Mockito.spy(new ActionTranslator(new TranslationExecutor() {
            @Override
            public <T extends ActionView> Stream<T> translate(@Nonnull final Stream<T> actionStream,
                                                              @Nonnull final EntitiesAndSettingsSnapshot snapshot) {
                return actionStream.peek(action -> action.getActionTranslation().setPassthroughTranslationSuccess());
            }
        }, grpcTestServer.getChannel()));
    }

    @Nonnull
    public static Action actionFromRecommendation(final ActionDTO.Action recommendation, final long actionPlanId) {
        return new Action(recommendation, actionPlanId, actionModeCalculator);
    }

    @Nonnull
    public static Action createMoveAction(final long actionId, final long actionPlanId) {
        return new Action(createMoveRecommendation(actionId), actionPlanId, actionModeCalculator);
    }

    @Nonnull
    public static ActionDTO.Action createMoveRecommendation(final long actionId) {
        int entityType = 1;
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

    public static void assertActionsEqual(@Nonnull final Action expected,
                                    @Nonnull final Action got) {
        Assert.assertEquals(expected.getId(), got.getId());
        Assert.assertEquals(expected.getRecommendation(), got.getRecommendation());
        Assert.assertEquals(expected.getState(), got.getState());
        Assert.assertEquals(expected.getActionPlanId(), got.getActionPlanId());
        if (expected.getCurrentExecutableStep().isPresent()) {
            Assert.assertTrue(got.getCurrentExecutableStep().isPresent());
            assertExecutionStepsEqual(expected.getCurrentExecutableStep().get(), got.getCurrentExecutableStep().get());
        } else {
            Assert.assertFalse(got.getCurrentExecutableStep().isPresent());
        }

        Assert.assertEquals(expected.getDecision(), got.getDecision());
    }

    private static void assertExecutionStepsEqual(@Nonnull final ExecutableStep expected,
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
            .setDeprecatedImportance(0)
            .setExecutable(true)
            .setExplanation(Explanation.newBuilder().build());
    }

    public static Map<String, Setting> makeActionModeSetting(ActionMode mode) {
        return ImmutableMap.of("move", Setting.newBuilder()
                .setSettingSpecName("move")
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue(mode.toString()).build())
                .build());
    }

    public static Optional<ActionPartialEntity> createTopologyEntityDTO(Long Oid, int entityType) {
        return Optional.of(ActionPartialEntity.newBuilder()
            .setOid(Oid)
            .setEntityType(entityType)
            .build());
    }

    public static void setEntityAndSourceAndDestination(EntitiesAndSettingsSnapshot entityCacheSnapshot,
                                                        Action action) {
        Long action1EntityId = action.getRecommendation().getInfo().getMove().getTarget().getId();
        ChangeProvider primaryChange = ActionDTOUtil.getPrimaryChangeProvider(
                action.getRecommendation());
        Long actionSourceId = primaryChange.getSource().getId();
        Long actionDestinationId = primaryChange.getDestination().getId();

        when(entityCacheSnapshot.getEntityFromOid(eq(action1EntityId)))
            .thenReturn((ActionOrchestratorTestUtils.createTopologyEntityDTO(action1EntityId,
                action.getRecommendation().getInfo().getMove().getTarget().getType())));
        when(entityCacheSnapshot.getEntityFromOid(eq(actionSourceId)))
            .thenReturn((ActionOrchestratorTestUtils.createTopologyEntityDTO(actionSourceId,
                primaryChange.getSource().getType())));
        when(entityCacheSnapshot.getEntityFromOid(eq(actionDestinationId)))
            .thenReturn((ActionOrchestratorTestUtils.createTopologyEntityDTO(actionDestinationId,
                primaryChange.getDestination().getType())));
    }

    public static void setEntityAndSourceAndDestination(EntitiesAndSettingsSnapshot entityCacheSnapshot,
                                                        ActionDTO.Action action) {
        Long action1EntityId = action.getInfo().getMove().getTarget().getId();
        ChangeProvider primaryChange = ActionDTOUtil.getPrimaryChangeProvider(action);
        Long actionSourceId = primaryChange.getSource().getId();
        Long actionDestinationId = primaryChange.getDestination().getId();

        when(entityCacheSnapshot.getEntityFromOid(eq(action1EntityId)))
            .thenReturn((ActionOrchestratorTestUtils.createTopologyEntityDTO(action1EntityId,
                action.getInfo().getMove().getTarget().getType())));
        when(entityCacheSnapshot.getEntityFromOid(eq(actionSourceId)))
            .thenReturn((ActionOrchestratorTestUtils.createTopologyEntityDTO(actionSourceId,
                primaryChange.getSource().getType())));
        when(entityCacheSnapshot.getEntityFromOid(eq(actionDestinationId)))
            .thenReturn((ActionOrchestratorTestUtils.createTopologyEntityDTO(actionDestinationId,
                primaryChange.getDestination().getType())));
    }

    public static ActionEntity createActionEntity(long id) {
        return ActionEntity.newBuilder()
                    .setId(id)
                     // set some fake type for now
                    .setType(DEFAULT_ENTITY_TYPE)
                    .build();
    }

    public static Explanation.ReasonCommodity createReasonCommodity(int baseType, String key) {
        CommodityType.Builder ct = TopologyDTO.CommodityType.newBuilder()
                .setType(baseType);
        if (key != null) {
            ct.setKey(key);
        }
        return Explanation.ReasonCommodity.newBuilder().setCommodityType(ct.build()).build();
    }

    /**
     * Mock {@link ActionView} instance.
     *
     * @param action Action that is backing {@code ActionView}.
     * @return Mocked {@code ActionView} instance.
     */
    @Nonnull
    public static ActionView mockActionView(ActionDTO.Action action) {
        final ActionView actionView = mock(ActionView.class);

        when(actionView.getRecommendation()).thenReturn(action);
        when(actionView.getTranslationResultOrOriginal()).thenReturn(action);

        return actionView;
    }
}
