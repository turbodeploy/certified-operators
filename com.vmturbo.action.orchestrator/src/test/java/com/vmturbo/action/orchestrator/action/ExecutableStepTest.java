package com.vmturbo.action.orchestrator.action;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep.Status;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExecutableStepTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    final ExecutableStep step = new ExecutableStep(0);

    @Test
    public void testConstruction() throws Exception {
        assertEquals(0, step.getTargetId());
        assertEquals(Status.QUEUED, step.getStatus());
        assertTrue(step.getEnqueueTime().isPresent());

        assertFalse(step.getStartTime().isPresent());
        assertFalse(step.getCompletionTime().isPresent());
    }

    @Test
    public void testExecute() throws Exception {
        step.execute();

        assertEquals(Status.IN_PROGRESS, step.getStatus());
        assertTrue(step.getStartTime().isPresent());

        assertFalse(step.getCompletionTime().isPresent());
    }

    @Test
    public void testSuccess() throws Exception {
        step.execute();
        step.success();

        assertEquals(Status.SUCCESS, step.getStatus());
        assertTrue(step.getCompletionTime().isPresent());
        assertEquals(ExecutableStep.SUCCESS_DESCRIPTION, step.getProgressDescription().get());
    }

    @Test
    public void testFail() throws Exception {
        step.execute();
        step.fail();
        step.addError("Foo");

        assertEquals(Status.FAILED, step.getStatus());
        assertTrue(step.getCompletionTime().isPresent());
        assertEquals("Foo", step.getErrors().get(0));
        assertEquals(ExecutableStep.FAILURE_DESCRIPTION, step.getProgressDescription().get());
    }

    @Test
    public void testUpdateProgress() throws Exception {
        step.execute();
        assertEquals(0, (int)step.getProgressPercentage().get());
        assertEquals(ExecutableStep.INITIAL_EXECUTION_DESCRIPTION, step.getProgressDescription().get());

        step.updateProgress(50, "Foo");
        assertEquals(50, (int)step.getProgressPercentage().get());
        assertEquals("Foo", step.getProgressDescription().get());
    }

    @Test
    public void testUpdateProgressPercentageTooLow() throws Exception {
        step.execute();

        expectedException.expect(IllegalArgumentException.class);
        step.updateProgress(-40, "Foo");
    }

    @Test
    public void testUpdateProgressPercentageTooHigh() throws Exception {
        step.execute();

        expectedException.expect(IllegalArgumentException.class);
        step.updateProgress(4120, "Foo");
    }

    @Test
    public void testUpdateProgressWhenComplete() throws Exception {
        step.execute();
        step.success();

        expectedException.expect(IllegalStateException.class);
        step.updateProgress(12, "Foo");
    }

    @Test
    public void testUpdateProgressWhenQueued() throws Exception {
        expectedException.expect(IllegalStateException.class);
        step.updateProgress(55, "Foo");
    }
}