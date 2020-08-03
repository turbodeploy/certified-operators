package com.vmturbo.action.orchestrator.translation.batch.translator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionTranslation;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Performance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo.ActionComputeTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link CloudMoveBatchTranslator}.
 */
public class CloudMoveBatchTranslatorTest {

    private CloudMoveBatchTranslator translator;

    private Action cloudMoveAction;
    private ActionTranslation actionTranslation;

    /**
     * Sets up test fields.
     */
    @Before
    public void setUp() {
        translator = new CloudMoveBatchTranslator();

        final ActionDTO.Action cloudMoveActionDto = ActionDTO.Action.newBuilder()
            .setId(1)
            .setDeprecatedImportance(0)
            .setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(1)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .build())
                    .addChanges(ChangeProvider.newBuilder()
                        .setSource(ActionEntity.newBuilder()
                            .setId(2)
                            .setType(EntityType.COMPUTE_TIER_VALUE)
                            .build())
                        .setDestination(ActionEntity.newBuilder()
                            .setId(3)
                            .setType(EntityType.COMPUTE_TIER_VALUE)
                            .build())
                        .build())
                    .build())
                .build())
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setIsPrimaryChangeProviderExplanation(false)
                        .setPerformance(Performance.newBuilder().build())
                        .build())
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setIsPrimaryChangeProviderExplanation(true)
                        .setEfficiency(Efficiency.newBuilder().build())
                        .build())
                    .build())
                .build())
            .build();
        cloudMoveAction = mock(Action.class);
        when(cloudMoveAction.getRecommendation()).thenReturn(cloudMoveActionDto);
        actionTranslation = new ActionTranslation(cloudMoveActionDto);
        when(cloudMoveAction.getActionTranslation()).thenReturn(actionTranslation);
    }

    /**
     * Tests that {@link CloudMoveBatchTranslator#appliesTo(ActionView)} returns true for Cloud
     * Move action.
     */
    @Test
    public void testAppliesToForCloudMove() {
        assertTrue(translator.appliesTo(cloudMoveAction));
    }

    /**
     * Tests that {@link CloudMoveBatchTranslator#appliesTo(ActionView)} returns false for Cloud
     * non-Move action.
     */
    @Test
    public void testAppliesToForCloudNonMove() {
        final ActionDTO.Action cloudNonMoveActionDto = ActionDTO.Action.newBuilder()
            .setId(1)
            .setDeprecatedImportance(0)
            .setInfo(ActionInfo.newBuilder()
                .setDelete(Delete.newBuilder().build()))
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setIsPrimaryChangeProviderExplanation(false)
                        .setPerformance(Performance.newBuilder().build())
                        .build())
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setIsPrimaryChangeProviderExplanation(true)
                        .setEfficiency(Efficiency.newBuilder().build())
                        .build())
                    .build())
                .build())
            .build();
        final Action cloudNonMoveAction = mock(Action.class);
        when(cloudNonMoveAction.getRecommendation()).thenReturn(cloudNonMoveActionDto);

        assertFalse(translator.appliesTo(cloudNonMoveAction));
    }

    /**
     * Tests that {@link CloudMoveBatchTranslator#appliesTo(ActionView)} returns false for Cloud
     * Move action with missing source.
     */
    @Test
    public void testAppliesToForCloudMoveWithMissingSource() {
        final ActionDTO.Action cloudMoveActionWithMissingSourceDto = ActionDTO.Action.newBuilder()
            .setId(1)
            .setDeprecatedImportance(0)
            .setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(1)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .build())
                    .addChanges(ChangeProvider.newBuilder()
                        .setDestination(ActionEntity.newBuilder()
                            .setId(3)
                            .setType(EntityType.COMPUTE_TIER_VALUE)
                            .build())
                        .build())
                    .build())
                .build())
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setEfficiency(Efficiency.newBuilder().build())
                        .build())
                    .build())
                .build())
            .build();
        final Action cloudMoveActionWithMissingSource = mock(Action.class);
        when(cloudMoveActionWithMissingSource.getRecommendation()).thenReturn(cloudMoveActionWithMissingSourceDto);

        assertFalse(translator.appliesTo(cloudMoveActionWithMissingSource));
    }

    /**
     * Tests that {@link CloudMoveBatchTranslator#appliesTo(ActionView)} returns false for Hybrid
     * Move action.
     */
    @Test
    public void testAppliesToForHybridMove() {
        final ActionDTO.Action hybridMoveActionDto = ActionDTO.Action.newBuilder()
            .setId(1)
            .setDeprecatedImportance(0)
            .setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(1)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .build())
                    .addChanges(ChangeProvider.newBuilder()
                        .setSource(ActionEntity.newBuilder()
                            .setId(2)
                            .setType(EntityType.COMPUTE_TIER_VALUE)
                            .build())
                        .setDestination(ActionEntity.newBuilder()
                            .setId(3)
                            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                            .build())
                        .build())
                    .build())
                .build())
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setEfficiency(Efficiency.newBuilder().build())
                        .build())
                    .build())
                .build())
            .build();
        final Action hybridMoveAction = mock(Action.class);
        when(hybridMoveAction.getRecommendation()).thenReturn(hybridMoveActionDto);

        assertFalse(translator.appliesTo(hybridMoveAction));
    }

    /**
     * Tests that {@link CloudMoveBatchTranslator#appliesTo(ActionView)} returns false for On-Prem
     * Move action.
     */
    @Test
    public void testAppliesToForOnPremMove() {
        final ActionDTO.Action onPremMoveActionDto = ActionDTO.Action.newBuilder()
            .setId(1)
            .setDeprecatedImportance(0)
            .setInfo(ActionInfo.newBuilder()
                .setMove(Move.newBuilder()
                    .setTarget(ActionEntity.newBuilder()
                        .setId(1)
                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .build())
                    .addChanges(ChangeProvider.newBuilder()
                        .setSource(ActionEntity.newBuilder()
                            .setId(2)
                            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                            .build())
                        .setDestination(ActionEntity.newBuilder()
                            .setId(3)
                            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
                            .build())
                        .build())
                    .build())
                .build())
            .setExplanation(Explanation.newBuilder()
                .setMove(MoveExplanation.newBuilder()
                    .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                        .setEfficiency(Efficiency.newBuilder().build())
                        .build())
                    .build())
                .build())
            .build();
        final Action onPremMoveAction = mock(Action.class);
        when(onPremMoveAction.getRecommendation()).thenReturn(onPremMoveActionDto);

        assertFalse(translator.appliesTo(onPremMoveAction));
    }

    /**
     * Tests translation of Cloud Move action to Scale action.
     */
    @Test
    public void testTranslateToScale() {
        // Arrange
        final List<Action> actionsToTranslate = ImmutableList.of(cloudMoveAction);
        final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);

        // Act
        final List<Action> translatedActions = translator.translate(actionsToTranslate, snapshot)
            .collect(Collectors.toList());

        // Assert
        assertEquals(1, translatedActions.size());
        assertEquals(TranslationStatus.TRANSLATION_SUCCEEDED, actionTranslation.getTranslationStatus());
        final ActionDTO.Action translatedAction = actionTranslation.getTranslationResultOrOriginal();
        assertTrue(translatedAction.getInfo().hasScale());
        final Explanation explanation = translatedAction.getExplanation();
        assertTrue(explanation.hasScale());
        final ScaleExplanation scaleExplanation = explanation.getScale();
        assertEquals(2, scaleExplanation.getChangeProviderExplanationCount());
    }

    /**
     * Tests translation of Cloud Move action to Allocate action.
     */
    @Test
    public void testTranslateToAllocate() {
        // Arrange
        final ActionEntity vm = ActionEntity.newBuilder()
                .setId(1)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        final long computeTierId = 2;
        final ActionEntity computeTier = ActionEntity.newBuilder()
                .setId(computeTierId)
                .setType(EntityType.COMPUTE_TIER_VALUE)
                .build();
        final ActionDTO.Action allocateActionDto = ActionDTO.Action.newBuilder()
                .setId(1)
                .setDeprecatedImportance(0)
                .setInfo(ActionInfo.newBuilder()
                        .setMove(Move.newBuilder()
                                .setTarget(vm)
                                .addChanges(ChangeProvider.newBuilder()
                                        .setSource(computeTier)
                                        .setDestination(computeTier)
                                        .build())
                                .build())
                        .build())
                .setExplanation(Explanation.getDefaultInstance())
                .build();
        final Action allocateAction = mock(Action.class);
        when(allocateAction.getRecommendation()).thenReturn(allocateActionDto);
        final ActionTranslation actionTranslation = new ActionTranslation(allocateActionDto);
        when(allocateAction.getActionTranslation()).thenReturn(actionTranslation);
        final List<Action> actionsToTranslate = ImmutableList.of(allocateAction);
        final EntitiesAndSettingsSnapshot snapshot = mock(EntitiesAndSettingsSnapshot.class);
        final String instanceSizeFamily = "m4";
        final ActionPartialEntity computeTierPartial = ActionPartialEntity.newBuilder()
                .setOid(1L)
                .setTypeSpecificInfo(ActionEntityTypeSpecificInfo.newBuilder()
                        .setComputeTier(ActionComputeTierInfo.newBuilder()
                                .setFamily(instanceSizeFamily)))
                .build();
        final Optional<ActionPartialEntity> computeTierPartialOpt = Optional.of(computeTierPartial);
        when(snapshot.getEntityFromOid(computeTierId)).thenReturn(computeTierPartialOpt);

        // Act
        final List<Action> translatedActions = translator.translate(actionsToTranslate, snapshot)
                .collect(Collectors.toList());

        // Assert
        assertEquals(1, translatedActions.size());
        assertEquals(TranslationStatus.TRANSLATION_SUCCEEDED, actionTranslation.getTranslationStatus());
        final ActionDTO.Action translatedAction = actionTranslation.getTranslationResultOrOriginal();
        assertTrue(translatedAction.getInfo().hasAllocate());
        final Allocate allocate = translatedAction.getInfo().getAllocate();
        assertEquals(vm, allocate.getTarget());
        assertEquals(computeTier, allocate.getWorkloadTier());
        final Explanation explanation = translatedAction.getExplanation();
        assertTrue(explanation.hasAllocate());
        assertEquals(instanceSizeFamily, explanation.getAllocate().getInstanceSizeFamily());
    }
}
