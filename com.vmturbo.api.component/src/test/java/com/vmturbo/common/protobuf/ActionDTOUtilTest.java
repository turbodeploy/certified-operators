package com.vmturbo.common.protobuf;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Sets;

import com.vmturbo.api.component.external.api.util.ApiUtilsTest;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.InitialPlacement;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link ActionDTOUtil{.
 */
public class ActionDTOUtilTest {
    private static final long TARGET = 10L;
    private static final long SOURCE_1 = 100L;
    private static final long DEST_1 = 101L;
    private static final long SOURCE_2 = 200L;
    private static final long DEST_2 = 201L;
    private static final Action action = Action.newBuilder()
                    .setId(1L)
                    .setImportance(1.0)
                    .setInfo(ActionInfo.newBuilder()
                        .setMove(Move.newBuilder()
                            .setTarget(ApiUtilsTest.createActionEntity(TARGET))
                            .addChanges(ChangeProvider.newBuilder()
                                .setSource(ApiUtilsTest.createActionEntity(SOURCE_1))
                                .setDestination(ApiUtilsTest.createActionEntity(DEST_1))
                                .build())
                            .addChanges(ChangeProvider.newBuilder()
                                .setSource(ApiUtilsTest.createActionEntity(SOURCE_2))
                                .setDestination(ApiUtilsTest.createActionEntity(DEST_2))
                                .build())
                            .build())
                        .build())
                    .setExplanation(Explanation.newBuilder()
                        .setMove(MoveExplanation.newBuilder()
                            .addChangeProviderExplanation(ChangeProviderExplanation.newBuilder()
                                .setInitialPlacement(InitialPlacement.getDefaultInstance())
                                .build())
                            .build())
                        .build())
                    .build();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testMapNormalSeverity() throws Exception {
        // It must be BELOW the Normal value to map to Normal.
        assertEquals(Severity.NORMAL,
            ActionDTOUtil.mapImportanceToSeverity(ActionDTOUtil.NORMAL_SEVERITY_THRESHOLD - 1.0));
    }

    @Test
    public void testMapMinorSeverity() throws Exception {
        // This is slightly weird, but if importance maps exactly to the Normal value it is considered Minor.
        assertEquals(Severity.MINOR,
            ActionDTOUtil.mapImportanceToSeverity(ActionDTOUtil.NORMAL_SEVERITY_THRESHOLD));
        assertEquals(Severity.MINOR,
            ActionDTOUtil.mapImportanceToSeverity(ActionDTOUtil.MINOR_SEVERITY_THRESHOLD - 1.0));
    }

    @Test
    public void testMapMajorSeverity() throws Exception {
        assertEquals(Severity.MAJOR,
            ActionDTOUtil.mapImportanceToSeverity(ActionDTOUtil.MINOR_SEVERITY_THRESHOLD));
        assertEquals(Severity.MAJOR,
            ActionDTOUtil.mapImportanceToSeverity(ActionDTOUtil.MAJOR_SEVERITY_THRESHOLD - 1.0));
    }

    @Test
    public void testMapCriticalSeverity() throws Exception {
        assertEquals(Severity.CRITICAL,
            ActionDTOUtil.mapImportanceToSeverity(ActionDTOUtil.MAJOR_SEVERITY_THRESHOLD));
        assertEquals(Severity.CRITICAL,
            ActionDTOUtil.mapImportanceToSeverity(ActionDTOUtil.MAJOR_SEVERITY_THRESHOLD + 1.0));
    }

    @Test
    public void testMapIllegalImportance() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        ActionDTOUtil.mapImportanceToSeverity(Double.NaN);
    }

    @Test
    public void testMapMoveToStart() {
        assertEquals(ActionType.START, ActionDTOUtil.getActionInfoActionType(action));
    }

    /**
     * Verify that the involved entities of a (compound) Move action are computed correctly.
     *
     * @throws UnsupportedActionException is not supposed to happen
     */
    @Test
    public void testInvolvedEntities() throws UnsupportedActionException {
        Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntities(action);
        assertEquals(Sets.newHashSet(TARGET, SOURCE_1, DEST_1, SOURCE_2, DEST_2), involvedEntities);
    }

    /**
     * Verify that the severity entity of a Move action is the source host, when one of the
     * changes involves a host, and the first source provider otherwise.
     *
     * @throws UnsupportedActionException is not supposed to happen
     */
    @Ignore
    @Test
    public void testGetSeverityEntityMove() throws UnsupportedActionException {
        // When SOURCE_1 is a PM and SOURCE_2 is STORAGE, severity entity is SOURCE_1
        assertEquals(SOURCE_1, ActionDTOUtil.getSeverityEntity(action));
        // When SOURCE_2 is a PM and SOURCE_1 is STORAGE, severity entity is SOURCE_2
        assertEquals(SOURCE_2, ActionDTOUtil.getSeverityEntity(action));
        // When all sources are STORAGEs, severity entity is SOURCE_1
        assertEquals(SOURCE_1, ActionDTOUtil.getSeverityEntity(action));
    }

    /**
     * Construct a types map where the specified oids are PMs and the rest are STORAGEs.
     *
     * @param pmOids oids to have type PM
     * @return the entity types map
     * @throws UnsupportedActionException is not supposed to happen
     */
    private static Map<Long, EntityType> entityTypes(Long... pmOids) throws UnsupportedActionException {
        List<Long> pmOidsList = Arrays.asList(pmOids);
        return ActionDTOUtil.getInvolvedEntities(action).stream()
                        .collect(Collectors.toMap(Function.identity(),
                            oid -> pmOidsList.contains(oid)
                                ? EntityType.PHYSICAL_MACHINE
                                : EntityType.STORAGE));
    }
}
