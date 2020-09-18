package com.vmturbo.components.common.pipeline;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineContextMemberException;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineException;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestMemberDefinitions;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestPassthroughStage;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestPipeline;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestPipelineContext;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestStage;
import com.vmturbo.components.common.pipeline.Stage.ContextMemberSummary;
import com.vmturbo.components.common.pipeline.Stage.FromContext;

/**
 * Unit tests for the {@link Pipeline}.
 *
 * <p/>These tests use a test implementation of a {@link Pipeline} - {@link TestPipeline}.
 */
public class PipelineTest {

    private final TestPipelineContext context = new TestPipelineContext();

    /**
     * Expected exception.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Test running a multi-stage pipeline.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testTwoStagePipeline() throws Exception {
        final TestStage stage1 = spy(new TestStage());
        final TestPassthroughStage stage2 = spy(new TestPassthroughStage());
        final TestPipeline<Long, Long> simplePipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                        .addStage(stage1)
                        .finalStage(stage2));


        assertThat(stage1.getContext(), is(context));
        assertThat(stage2.getContext(), is(context));
        assertThat(simplePipeline.run(10L), is(10L));

        // Assert that they were changed in order.
        final InOrder order = inOrder(stage1, stage2);
        order.verify(stage1).execute(any());
        order.verify(stage2).execute(any());
    }

    /**
     * Test that a stage that throws an exception crashes the pipeline.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test(expected = PipelineException.class)
    public void testPipelineException() throws Exception {
        final TestStage stage1 = spy(new TestStage());
        when(stage1.execute(eq(10L))).thenThrow(new PipelineStageException("ERROR"));

        final TestPipeline<Long, Long> simplePipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .finalStage(stage1));
        simplePipeline.run(10L);
    }

    /**
     * Test that a required passthrough stage that throws an exception crashes the pipeline.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test(expected = PipelineException.class)
    public void testPipelineRequiredPassthroughStageException() throws Exception {
        final TestPassthroughStage stage = spy(new TestPassthroughStage());
        when(stage.execute(eq(10L))).thenThrow(new PipelineStageException("ERROR"));
        when(stage.required()).thenReturn(true);

        final TestPipeline<Long, Long> simplePipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .finalStage(stage));
        simplePipeline.run(10L);
    }

    /**
     * Test that a required passthrough stage that throws a RUNTIME exception crashes the pipeline,
     * and that the exception is wrapped inside a {@link PipelineException}.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test(expected = PipelineException.class)
    public void testPipelineRequiredPassthroughStageRuntimeException() throws Exception {
        final TestPassthroughStage stage = spy(new TestPassthroughStage());
        when(stage.execute(eq(10L))).thenThrow(new RuntimeException("ERROR"));
        when(stage.required()).thenReturn(true);
        final TestPipeline<Long, Long> simplePipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .finalStage(stage));
        simplePipeline.run(10L);
    }

    /**
     * Test that a passthrough stage configured for retries attempts to re-run if it encounters
     * an error.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPipelinePassthroughStageRetry() throws Exception {
        final TestPassthroughStage stage = spy(new TestPassthroughStage());
        final RuntimeException err = new RuntimeException("ERROR");
        final MutableInt invocationCount = new MutableInt(0);
        doAnswer(invocation -> {
            if (invocationCount.intValue() == 0) {
                invocationCount.increment();
                throw err;
            } else {
                return invocation.callRealMethod();
            }
        }).when(stage).passthrough(eq(10L));

        doReturn(Optional.of(1L)).when(stage).getRetryIntervalMs(anyInt(), any());

        final TestPipeline<Long, Long> simplePipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .finalStage(stage));

        // Shouldn't throw an exception.
        simplePipeline.run(10L);
        // Should have been invoked twice.
        verify(stage, times(2)).passthrough(10L);
    }

    /**
     * Test that a NON-required passthrough stage does not crash the pipeline if it terminates with
     * an error.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPipelineNotRequiredStageException() throws Exception {
        final TestPassthroughStage stage = spy(new TestPassthroughStage());
        doThrow(new PipelineStageException("ERROR")).when(stage).passthrough(eq(10L));
        when(stage.required()).thenReturn(false);

        final TestPipeline<Long, Long> simplePipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .finalStage(stage));
        assertThat(simplePipeline.run(10L), is(10L));
    }

    /**
     * Test that a NON-required passthrough stage does not crash the pipeline if it terminates with
     * a RUNTIME exception.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPipelineNotRequiredStageRuntimeException() throws Exception {
        final TestPassthroughStage stage = spy(new TestPassthroughStage());
        doThrow(new RuntimeException("ERROR")).when(stage).passthrough(eq(10L));
        when(stage.required()).thenReturn(false);

        final TestPipeline<Long, Long> simplePipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .finalStage(stage));
        assertThat(simplePipeline.run(10L), is(10L));
    }

    /**
     * Basic membership test.
     *
     * @throws PipelineContextMemberException on bad.
     */
    @Test
    public void testPipelineContextMembership() throws PipelineContextMemberException {
        final TestPipelineContext context = new TestPipelineContext();

        assertThat(context.hasMember(TestMemberDefinitions.FOO), is(false));
        context.addMember(TestMemberDefinitions.FOO, "foo");
        assertThat(context.getMember(TestMemberDefinitions.FOO), is("foo"));
        assertThat(context.hasMember(TestMemberDefinitions.FOO), is(true));

        context.dropMember(TestMemberDefinitions.FOO);
        assertThat(context.hasMember(TestMemberDefinitions.FOO), is(false));
    }

    /**
     * Test a valid set of dependencies does not throw an exception.
     */
    @Test
    public void testValidPipelineDependencies() {
        final TestPassthroughStage stage1 = new TestPassthroughStage();
        final TestPassthroughStage stage2 = new TestPassthroughStage();
        final TestPassthroughStage stage3 = new TestPassthroughStage();

        stage1.providesToContext(TestMemberDefinitions.FOO, "foo");
        stage1.providesToContext(TestMemberDefinitions.BAR, 1);

        stage2.providesToContext(TestMemberDefinitions.BAZ, Arrays.asList(1L, 2L));
        stage2.requiresFromContext(TestMemberDefinitions.FOO);

        stage3.requiresFromContext(TestMemberDefinitions.BAZ);
        stage3.requiresFromContext(TestMemberDefinitions.BAR);

        final TestPipeline<Long, Long> abc = new TestPipeline<>(
            PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
            .addStage(stage1)
            .addStage(stage2)
            .finalStage(stage3));
    }

    /**
     * Test that providing a starting dependency allows a downstream stage to require it
     * while still passing validation.
     */
    @Test
    public void testStartingDependencies() {
        final TestPassthroughStage stage = new TestPassthroughStage();
        stage.requiresFromContext(TestMemberDefinitions.FOO);
        stage.providesToContext(TestMemberDefinitions.BAR, 1);

        final TestPipeline<Long, Long> pipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
            .initialContextMember(TestMemberDefinitions.FOO, () -> "foo")
            .initialContextMember(TestMemberDefinitions.BAZ, () -> Arrays.asList(1L, 2L))
            .finalStage(stage));
    }

    /**
     * Test that an exception is thrown when a stage says it will provide a ContextMember
     * that the pipeline ALSO says it will provide.
     */
    @Test
    public void testStartingDependenciesOverlapWithProvides() {
        expectedException.expect(PipelineContextMemberException.class);
        expectedException.expectMessage("Pipeline ContextMember of type foo[String] "
            + "provided by stage TestPassthroughStage is already provided by earlier "
            + "stage INITIAL. Only one stage in the pipeline may provide a particular "
            + "ContextMember to the pipeline context.");

        final TestPassthroughStage stage = new TestPassthroughStage();
        stage.providesToContext(TestMemberDefinitions.FOO, "foo");

        new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
            .initialContextMember(TestMemberDefinitions.FOO, () -> "foo")
            .finalStage(stage));
    }

    /**
     * Test that a stage requiring an unprovided dependency throws an exception.
     * Here the final stage requires a BAZ but no one provides it.
     */
    @Test
    public void testMissingRequirement() {
        expectedException.expect(PipelineContextMemberException.class);
        expectedException.expectMessage("No earlier stage provides required pipeline "
            + "dependencies (baz[List]) without default for stage TestPassthroughStage.");

        final TestPassthroughStage stage1 = new TestPassthroughStage();
        final TestPassthroughStage stage2 = new TestPassthroughStage();
        final TestPassthroughStage stage3 = new TestPassthroughStage();

        stage1.providesToContext(TestMemberDefinitions.FOO, "foo");
        stage1.providesToContext(TestMemberDefinitions.BAR, 1);

        stage2.requiresFromContext(TestMemberDefinitions.FOO);

        stage3.requiresFromContext(TestMemberDefinitions.BAZ);
        stage3.requiresFromContext(TestMemberDefinitions.BAR);

        new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
            .addStage(stage1)
            .addStage(stage2)
            .finalStage(stage3));
    }

    /**
     * Test that a stage requiring an unprovided dependency does not throw an exception when there is a default.
     * Here the final stage requires a QUUX and no one provides it but there is a default.
     */
    @Test
    public void testMissingRequirementWithDefault() {
        final TestPassthroughStage stage1 = new TestPassthroughStage();
        final TestPassthroughStage stage2 = new TestPassthroughStage();
        final TestPassthroughStage stage3 = new TestPassthroughStage();

        stage1.providesToContext(TestMemberDefinitions.FOO, "foo");
        stage1.providesToContext(TestMemberDefinitions.BAR, 1);

        stage2.requiresFromContext(TestMemberDefinitions.FOO);

        stage3.requiresFromContext(TestMemberDefinitions.QUUX);
        stage3.requiresFromContext(TestMemberDefinitions.BAR);

        final TestPipeline<Long, Long> pipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
            .addStage(stage1)
            .addStage(stage2)
            .finalStage(stage3));

        final String description = pipeline.tabularDescription("TestPipeline");
        // The quux member should be highlighted with an asterisk because it uses a default
        assertThat(description, containsString("*quux"));
        // It should also be dropped without an asterisk.
        assertThat(description, containsString("|quux"));

        // The other members should not have an asterisk
        assertThat(description, not(containsString("*foo")));
        assertThat(description, not(containsString("*bar")));
    }

    /**
     * Test that multiple stages providing the same requirement throws an exception.
     * Here two stages provide a BAR.
     */
    @Test
    public void testDuplicateRequirementProviders() {
        expectedException.expect(PipelineContextMemberException.class);
        expectedException.expectMessage("Pipeline ContextMember of type bar[Integer] "
            + "provided by stage TestPassthroughStage is already provided by earlier stage "
            + "TestPassthroughStage. Only one stage in the pipeline may provide a particular "
            + "ContextMember to the pipeline context.");

        final TestPassthroughStage stage1 = new TestPassthroughStage();
        final TestPassthroughStage stage2 = new TestPassthroughStage();
        final TestPassthroughStage stage3 = new TestPassthroughStage();

        stage1.providesToContext(TestMemberDefinitions.FOO, "foo");
        stage1.providesToContext(TestMemberDefinitions.BAR, 1);

        stage2.providesToContext(TestMemberDefinitions.BAR, 2);
        stage2.requiresFromContext(TestMemberDefinitions.FOO);

        stage3.requiresFromContext(TestMemberDefinitions.QUUX);
        stage3.requiresFromContext(TestMemberDefinitions.BAR);

        new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
            .addStage(stage1)
            .addStage(stage2)
            .finalStage(stage3));
    }

    /**
     * The pipeline should call providers and consumers when stages execute.
     *
     * @throws Exception on error.
     */
    @Test
    public void testProvidesConsumesCalledOnStageExecution() throws Exception {
        final TestPipelineContext spyContext = Mockito.spy(context);

        final TestPassthroughStage stage1 = new TestPassthroughStage();
        final TestPassthroughStage stage2 = new TestPassthroughStage();
        final TestPassthroughStage stage3 = new TestPassthroughStage();

        stage1.providesToContext(TestMemberDefinitions.FOO, "foo");
        stage1.providesToContext(TestMemberDefinitions.BAR, 1);

        stage2.providesToContext(TestMemberDefinitions.BAZ, Arrays.asList(1L, 2L));
        stage2.requiresFromContext(TestMemberDefinitions.FOO);

        final FromContext<List<Long>> baz = stage3.requiresFromContext(TestMemberDefinitions.BAZ);
        final FromContext<Integer> bar = stage3.requiresFromContext(TestMemberDefinitions.BAR);

        final TestPipeline<Long, Long> pipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(spyContext)
            .addStage(stage1)
            .addStage(stage2)
            .finalStage(stage3));

        pipeline.run(456L);
        final InOrder inOrder = inOrder(spyContext);
        inOrder.verify(spyContext).addMember(eq(TestMemberDefinitions.FOO), eq("foo"));
        inOrder.verify(spyContext).addMember(eq(TestMemberDefinitions.BAR), eq(1));
        inOrder.verify(spyContext).getMember(eq(TestMemberDefinitions.FOO));
        inOrder.verify(spyContext).addMember(eq(TestMemberDefinitions.BAZ), eq(Arrays.asList(1L, 2L)));
        inOrder.verify(spyContext).getMember(eq(TestMemberDefinitions.BAZ));
        inOrder.verify(spyContext).getMember(eq(TestMemberDefinitions.BAR));
        assertEquals(1, (int)bar.get());
        assertEquals(Arrays.asList(1L, 2L), baz.get());
    }

    /**
     * Test that the pipeline status generates the correct message with summary.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testStatusMessageWithSummary() throws Exception {
        final Stage<Long, Long, TestPipelineContext> stage = new TestPassthroughStage();
        stage.providesToContext(TestMemberDefinitions.BAZ, (Supplier<List<Long>>)() -> Arrays.asList(1L, 2L));
        stage.requiresFromContext(TestMemberDefinitions.QUUX);
        new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
            .finalStage(stage));
        stage.execute(1L);

        final Status status = Status.success("My message");
        status.setContextMemberSummary(new ContextMemberSummary(stage, context));
        assertThat(status.getMessageWithSummary(), either(
            is("My message\n"
                + "PIPELINE_CONTEXT_MEMBERS:\n"
                + "\tPROVIDED=(baz[size=2])\n"
                + "\tREQUIRED=(quux[one object])\n"
                + "\tDROPPED=(quux, baz)\n")).or(
            is("My message\n"
                + "PIPELINE_CONTEXT_MEMBERS:\n"
                + "\tPROVIDED=(baz[size=2])\n"
                + "\tREQUIRED=(quux[one object])\n"
                + "\tDROPPED=(baz, quux)\n")));
    }
}
