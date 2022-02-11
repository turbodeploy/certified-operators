package com.vmturbo.action.orchestrator;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

import org.junit.Assert;
import org.mockito.Mockito;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.Action.SerializationState;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.ExecutableStep;
import com.vmturbo.action.orchestrator.action.TestActionBuilder;
import com.vmturbo.action.orchestrator.action.VisibilityLevel;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.topology.ActionGraphEntity;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.translation.ActionTranslator.TranslationExecutor;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo.CoreQuotaByBusinessAccount;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo.CoreQuotaByBusinessAccount.CoreQuotaByRegion;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo.CoreQuotaByBusinessAccount.CoreQuotaByRegion.CoreQuotaByFamily;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintType;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.Active;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowProperty;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.ResourcePath;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ActionSettingType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
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
        }, grpcTestServer.getChannel(), new ActionTopologyStore()));
    }

    @Nonnull
    public static Action actionFromRecommendation(final ActionDTO.Action recommendation, final long actionPlanId) {
        return new Action(recommendation, actionPlanId, actionModeCalculator,
                IdentityGenerator.next());
    }

    @Nonnull
    public static Action createMoveAction(final long actionId, final long actionPlanId) {
        return new Action(createMoveRecommendation(actionId), actionPlanId, actionModeCalculator,
                IdentityGenerator.next());
    }

    /**
     * Create provision action.
     *
     * @param actionId     Given action ID.
     * @param targetEntityId Target entity ID.
     * @return {@link Action} with provision info.
     */
    @Nonnull
    public static Action createProvisionAction(final long actionId, final long targetEntityId,
                                               final int targetEntityType) {
        return new Action(createProvisionRecommendation(actionId, targetEntityId, targetEntityType),
                2, actionModeCalculator, targetEntityType);
    }

    /**
     * Create atomicResize action.
     *
     * @param actionId       Given action ID.
     * @param targetEntityId Target entity OID.
     * @return {@link Action} with atomicResize info.
     */
    @Nonnull
    public static Action createAtomicResizeAction(final long actionId, final long targetEntityId) {
        return new Action(createAtomicResizeRecommendation(actionId, targetEntityId), 2, actionModeCalculator,
                IdentityGenerator.next());
    }

    /**
     * Create resize action.
     *
     * @param actionId         Given action ID.
     * @param targetEntityId   Given target entity OID.
     * @param targetEntityType Given target entity type.
     * @return Resize action.
     */
    @Nonnull
    public static Action createResizeAction(final long actionId, final long targetEntityId, final int targetEntityType) {
        return new Action(createResizeRecommendation(actionId, targetEntityId, 1, 1.0, 1.0, targetEntityType),
                2, actionModeCalculator, IdentityGenerator.next());
    }

    /**
     * Creates action from serialized state.
     *
     * @param serializationState action serialized state
     * @return action
     */
    @Nonnull
    public static Action createActionFromSerializedState(
            @Nonnull final SerializationState serializationState) {
        final Action action = new Action(serializationState, actionModeCalculator);
        action.getActionTranslation().setPassthroughTranslationSuccess();
        return action;
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
                        .setExplanation(Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
                            .addChangeProviderExplanation(ChangeProviderExplanation.getDefaultInstance()).build()).build())
                        .build();
    }

    /**
     * Create a Resize action.
     *
     * @param actionId         Given action ID.
     * @param targetId         Given target entity ID.
     * @param resizeCommodity  Resize commodity type.
     * @param oldCapacity      Commodity old capacity.
     * @param newCapacity      Commodity new capacity
     * @param targetEntityType Target entity type.
     * @return A new Resize action.
     */
    @Nonnull
    public static ActionDTO.Action createResizeRecommendation(final long actionId,
                                                              final long targetId,
                                                              final int resizeCommodity,
                                                              final double oldCapacity,
                                                              final double newCapacity,
                                                              final int targetEntityType) {
        return baseAction(actionId)
            .setInfo(ActionDTO.ActionInfo.newBuilder()
                .setResize(ActionDTO.Resize.newBuilder()
                    .setCommodityType(CommodityType.newBuilder().setType(resizeCommodity))
                    .setOldCapacity((float)oldCapacity)
                    .setNewCapacity((float)newCapacity)
                    .setTarget(createActionEntity(targetId, targetEntityType))))
            .build();
    }

    /**
     * Create a Resize action.
     *
     * @param actionId         Given action ID.
     * @param targetId         Given target entity ID.
     * @param resizeCommodity  Resize commodity type.
     * @param oldCapacity      Commodity old capacity.
     * @param newCapacity      Commodity new capacity
     * @return A new Resize action.
     */
    @Nonnull
    public static ActionDTO.Action createResizeRecommendation(final long actionId,
                                                              final long targetId,
                                                              @Nonnull final CommodityDTO.CommodityType resizeCommodity,
                                                              final double oldCapacity,
                                                              final double newCapacity) {
        return createResizeRecommendation(actionId, targetId, resizeCommodity.getNumber(), oldCapacity, newCapacity, DEFAULT_ENTITY_TYPE);
    }

    /**
     * Create a Resize action.
     *
     * @param actionId         Given action ID.
     * @param resizeCommodity  Resize commodity type.
     * @return A new Resize action.
     */
    @Nonnull
    public static ActionDTO.Action createResizeRecommendation(final long actionId,
                                                              @Nonnull final CommodityDTO.CommodityType resizeCommodity) {
        return createResizeRecommendation(actionId, TARGET_ID, resizeCommodity, 1.0, 2.0);
    }

    /**
     * Create a provision recommendation.
     *
     * @param actionId the action ID
     * @param targetId the target ID
     * @param entityType the entity type
     * @return a provision recommendation
     */
    @Nonnull
    public static ActionDTO.Action createProvisionRecommendation(final long actionId,
                                                                 final long targetId,
                                                                 final int entityType) {
        return baseAction(actionId)
                .setInfo(ActionInfo.newBuilder()
                        .setProvision(Provision.newBuilder()
                                .setEntityToClone(ActionEntity.newBuilder()
                                        .setId(targetId)
                                        .setType(entityType))
                                .setProvisionIndex(0)))
                .setExplanation(Explanation.newBuilder()
                        .setProvision(ProvisionExplanation.newBuilder()
                                .setProvisionBySupplyExplanation(ProvisionBySupplyExplanation.newBuilder()
                                        .setMostExpensiveCommodityInfo(ActionOrchestratorTestUtils
                                                .createReasonCommodity(CommodityDTO.CommodityType
                                                        .RESPONSE_TIME_VALUE, null)))))
                .build();
    }

    public static ActionDTO.Action createAtomicResizeRecommendation(final long actionId, final long targetEntityId) {
        return baseAction(actionId)
                .setInfo(TestActionBuilder.makeAtomicResizeInfo(targetEntityId,
                        idCounter.incrementAndGet(), Collections.singletonList(idCounter.incrementAndGet())))
                .setExplanation(Explanation.newBuilder().setMove(MoveExplanation.newBuilder()
                        .addChangeProviderExplanation(ChangeProviderExplanation.getDefaultInstance()).build()).build())
                .build();
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

    /**
     * Creates action mode with related workflow (i.g. PRE) settings.
     *
     * @param actionSettings action mode setting
     * @param mode action mode value
     * @param actionSettingType type of related workflow setting
     * @param workflowId workflow setting value
     * @return map of settings
     */
    public static Map<String, Setting> makeActionModeAndWorkflowSettings(
            @Nonnull ConfigurableActionSettings actionSettings, @Nonnull ActionMode mode,
            @Nonnull ActionSettingType actionSettingType, long workflowId) {
        final ImmutableMap.Builder<String, Setting> settingMap = new ImmutableMap.Builder<>();
        settingMap.put(actionSettings.name(), Setting.newBuilder()
                .setSettingSpecName(actionSettings.name())
                .setEnumSettingValue(
                        EnumSettingValue.newBuilder().setValue(mode.toString()).build())
                .build());
        final String preWorkflowSettingName =
                ActionSettingSpecs.getSubSettingFromActionModeSetting(actionSettings,
                        actionSettingType);
        settingMap.put(preWorkflowSettingName, Setting.newBuilder()
                .setSettingSpecName(preWorkflowSettingName)
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue(String.valueOf(workflowId))
                        .build())
                .build());
        return settingMap.build();
    }

    /**
     * Create actionMode and executionSchedule settings.
     *
     * @param mode action mode
     * @param executionScheduleIds collection of schedule ids
     * @return map of setting spec to setting
     */
    public static Map<String, Setting> makeActionModeAndExecutionScheduleSetting(
            @Nonnull ActionMode mode, @Nonnull Collection<Long> executionScheduleIds) {
        ImmutableMap.Builder<String, Setting> settingMap = new ImmutableMap.Builder<>();
        settingMap.put(ConfigurableActionSettings.Move.getSettingName(), Setting.newBuilder()
                .setSettingSpecName(ConfigurableActionSettings.Move.getSettingName())
                .setEnumSettingValue(
                        EnumSettingValue.newBuilder().setValue(mode.toString()).build())
                .build());
        if (!executionScheduleIds.isEmpty()) {
            final Setting executionScheduleSetting = Setting.newBuilder()
                .setSettingSpecName(
                    ActionSettingSpecs.getSubSettingFromActionModeSetting(
                        ConfigurableActionSettings.Move, ActionSettingType.SCHEDULE))
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                    .addAllOids(executionScheduleIds)
                    .build())
                .build();
            settingMap.put(ActionSettingSpecs.getSubSettingFromActionModeSetting(
                    ConfigurableActionSettings.Move, ActionSettingType.SCHEDULE),
                    executionScheduleSetting);
        }
        return settingMap.build();
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
                action.getRecommendation()).get();
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
        ChangeProvider primaryChange = ActionDTOUtil.getPrimaryChangeProvider(action).get();
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
        when(entityCacheSnapshot.getAcceptingUserForAction(anyLong())).thenReturn(Optional.empty());
    }

    public static ActionEntity createActionEntity(long id) {
        return createActionEntity(id, DEFAULT_ENTITY_TYPE);
    }

    /**
     * Create an action entity.
     *
     * @param id the ID of the action
     * @param entityType the entity type of the action
     * @return the action entity
     */
    public static ActionEntity createActionEntity(long id, int entityType) {
        return ActionEntity.newBuilder()
                .setId(id)
                .setType(entityType)
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
        when(actionView.getVisibilityLevel()).thenReturn(VisibilityLevel.ALWAYS_VISIBLE);

        return actionView;
    }

    /**
     * Build core quota action constraint info.
     *
     * @param businessAccountIds a list of business account ids
     * @param regionIds a list of region ids
     * @param families a list of families
     * @param value a value
     * @return an action constraint info
     */
    public static ActionConstraintInfo buildCoreQuotaActionConstraintInfo(
        @Nonnull final List<Long> businessAccountIds, @Nonnull final List<Long> regionIds,
        @Nonnull final List<String> families, final int value) {
        return ActionConstraintInfo.newBuilder()
            .setActionConstraintType(ActionConstraintType.CORE_QUOTA)
            .setCoreQuotaInfo(CoreQuotaInfo.newBuilder()
                .addAllCoreQuotaByBusinessAccount(businessAccountIds.stream().map(businessAccountId ->
                    CoreQuotaByBusinessAccount.newBuilder()
                        .setBusinessAccountId(businessAccountId)
                        .addAllCoreQuotaByRegion(regionIds.stream().map(regionId ->
                            CoreQuotaByRegion.newBuilder()
                                .setRegionId(regionId).setTotalCoreQuota(value)
                                .addAllCoreQuotaByFamily(families.stream().map(family ->
                                    CoreQuotaByFamily.newBuilder()
                                        .setFamily(family).setQuota(value).build())
                                    .collect(Collectors.toList())).build())
                            .collect(Collectors.toList())).build())
                    .collect(Collectors.toList()))).build();
    }

    /**
     * Creates active schedule.
     *
     * @param executionScheduleId the schedule it
     * @return the instance of schedule
     */
    @Nonnull
    public static Schedule createActiveSchedule(long executionScheduleId) {
        return Schedule.newBuilder()
                .setId(executionScheduleId)
                .setDisplayName("testSchedule")
                .setNextOccurrence(
                        Schedule.NextOccurrence.newBuilder().setStartTime(1589799600L).build())
                .setActive(Active.newBuilder().setRemainingActiveTimeMs(2000))
                .setStartTime(1589796000L)
                .setEndTime(1589824800)
                .setTimezoneId("America/Toronto")
                .build();
    }

    /**
     * Mock a {@link ActionGraphEntity}.
     *
     * @param entityId   Given entity ID.
     * @param entityType Given entity Type.
     * @return Mocked {@link ActionGraphEntity}.
     */
    @Nonnull
    public static ActionGraphEntity mockActionGraphEntity(long entityId, int entityType) {
        ActionGraphEntity entity = mock(ActionGraphEntity.class);
        when(entity.getOid()).thenReturn(entityId);
        when(entity.getEntityType()).thenReturn(entityType);
        return entity;
    }

    /**
     * Reads sample ActionAPIDTO.
     *
     * @return ActionApiDTO represented as a string
     */
    public static String readSampleActionApiDto() {
        final File file1 = ResourcePath.getTestResource(ActionOrchestratorTestUtils.class, "resizeActionApiDto.json")
                .toFile();
        try {
            return Files.asCharSource(file1, Charset.defaultCharset()).read();
        } catch (IOException ex) {
            fail("Failed to load the sample ActionApiDto resource");
            return "";
        }
    }

    /**
     * Get workflow property by name.
     *
     * @param workflowProperties the list of workflow properties
     * @param propertyName the name of the property
     * @return certain workflow if present otherwise Optional.empty().
     **/
    public static Optional<WorkflowProperty> getWorkflowProperty(
            final List<WorkflowProperty> workflowProperties, final String propertyName) {
        return workflowProperties.stream()
                .filter(prop -> prop.getName().equals(propertyName))
                .findFirst();
    }
}
