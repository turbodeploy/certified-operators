package com.vmturbo.action.orchestrator.action;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.action.orchestrator.action.ActionEvent.BeginExecutionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.commons.idgen.IdentityGenerator;

/**
 * Unit test for {@link Action}s.
 */
public class ActionTest {

    final long actionPlanId = 1;
    private ActionDTO.Action recommendation;
    private Action action;

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);

        recommendation = move(11, 22, 33).build();
        action = new Action(recommendation, actionPlanId);
    }

    @Test
    public void testGetRecommendation() throws Exception {
        assertEquals(recommendation, action.getRecommendation());
    }

    @Test
    public void testGetActionPlanId() throws Exception {
        assertEquals(actionPlanId, action.getActionPlanId());
    }

    @Test
    public void testInitialState() throws Exception {
        assertEquals(ActionState.READY, action.getState());
    }

    @Test
    public void testIsReady() throws Exception {
        assertTrue(action.isReady());
        action.receive(new ActionEvent.NotRecommendedEvent(5));
        assertFalse(action.isReady());
    }

    @Test
    public void testGetId() throws Exception {
        assertEquals(recommendation.getId(), action.getId());
    }

    @Test
    public void testExecutionCreatesExecutionStep() throws Exception {
        final long targetId = 7;
        action.receive(new ManualAcceptanceEvent(0L, targetId));
        Assert.assertTrue(action.getExecutableStep().isPresent());
        Assert.assertEquals(targetId, action.getExecutableStep().get().getTargetId());
    }

    @Test
    public void testBeginExecutionEventStartsExecute() throws Exception {
        final long targetId = 7;
        action.receive(new ManualAcceptanceEvent(0L, targetId));
        action.receive(new BeginExecutionEvent());
    }

    @Test
    public void testDetermineExecutabilityReady() {
        assertTrue(action.determineExecutability());
    }

    @Test
    public void testDetermineExecutabilityInProgress() {
        action.receive(new ManualAcceptanceEvent(0L, 24L));

        assertFalse(action.determineExecutability());
    }

    @Test
    public void testDetermineExecutabilityNotExecutable() {
        final ActionDTO.Action recommendation = action.getRecommendation().toBuilder()
            .setExecutable(false).build();
        final Action notExecutable = new Action(recommendation, 1);

        assertFalse(notExecutable.determineExecutability());
    }

    private ActionDTO.Action.Builder move(long targetId, long sourceId, long destinationId) {
        return ActionDTO.Action.newBuilder()
            .setId(IdentityGenerator.next())
            .setImportance(0)
            .setExecutable(true)
            .setInfo(ActionInfo.newBuilder().setMove(Move.newBuilder()
                    .setTargetId(targetId)
                    .setSourceId(sourceId)
                    .setDestinationId(destinationId)
            )).setExplanation(Explanation.newBuilder().build());
    }
}