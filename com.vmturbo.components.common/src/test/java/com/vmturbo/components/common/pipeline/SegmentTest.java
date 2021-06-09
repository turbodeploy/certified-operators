package com.vmturbo.components.common.pipeline;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.annotation.Nonnull;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineContextMemberException;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineException;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.NamedTestPassthroughStage;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestMemberDefinitions;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestPassthroughStage;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestPipeline;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestPipelineContext;
import com.vmturbo.components.common.pipeline.SegmentStage.SegmentDefinition;
import com.vmturbo.components.common.pipeline.SegmentStage.SegmentDefinitionBuilder;

/**
 * Tests for {@link SegmentStage}.
 */
public class SegmentTest {

    private final TestPipelineContext context = new TestPipelineContext();

    /**
     * A stage for the {@link TestPipeline}.
     */
    public static class TestSegmentStage extends SegmentStage<Long, Long, Long, Long, TestPipelineContext> {
        /**
         * Constructor.
         *
         * @param segmentDefinition The segment definition.
         */
        public TestSegmentStage(@Nonnull SegmentDefinition<Long, Long, TestPipelineContext> segmentDefinition) {
            super(segmentDefinition);
        }

        @Nonnull
        @Override
        protected Long setupExecution(@Nonnull Long input) {
            return input;
        }

        @Nonnull
        @Override
        protected StageResult<Long> completeExecution(@Nonnull StageResult<Long> segmentResult) {
            return segmentResult;
        }

        @Override
        protected void finalizeExecution(boolean executionCompleted) {
            // do nothing
        }
    }

    /**
     * Test that a pipeline including nested segments generates the correct summary.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testStatusMessageWithSummary() throws Exception {
        final Stage<Long, Long, TestPipelineContext> nestedSegmentStage = segmentStage();
        final Stage<Long, Long, TestPipelineContext> segmentStage = segmentStage(nestedSegmentStage);
        final Stage<Long, Long, TestPipelineContext> otherStage = new TestPassthroughStage();
        final TestPipeline<Long, Long> pipeline = new TestPipeline<>(
            PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .addStage(segmentStage)
                .finalStage(otherStage));
        pipeline.run(1L);

        // We use the pattern [\s\S]* to match any character including new lines. The more typical .*
        // will fail because there are newlines that fail to match.
        assertThat(pipeline.getSummary().toString(), matchesPattern(
            "[\\s\\S]*======== Stage Breakdown ========\n"
                + "TestSegmentStage --- SUCCEEDED in .*\n"
                + "    Status: COMPLETED\n"
                + "\n"
                + "    -------\n"
                + "    TestSegmentStage --- SUCCEEDED in .*\n"
                + "        Status: COMPLETED\n"
                + "\n"
                + "        -------\n"
                + "        TestPassthroughStage --- SUCCEEDED in .*s\n"
                + "        -------\n"
                + "        TestPassthroughStage --- SUCCEEDED in .*s\n"
                + "    -------\n"
                + "    TestPassthroughStage --- SUCCEEDED in .*s\n"
                + "    -------\n"
                + "    TestPassthroughStage --- SUCCEEDED in .*s\n"
                + "-------\n"
                + "TestPassthroughStage --- SUCCEEDED in .*s\n"
                + "=================================[\\s\\S]*"));
    }

    /**
     * Test that a valid context member setup involving interior pipeline segments
     * does not throw any exceptions.
     */
    @Test
    public void testValidContextMembersForSegment() {
        final Stage<Long, Long, TestPipelineContext> providesFoo = new TestPassthroughStage();
        final Stage<Long, Long, TestPipelineContext> requiresFoo = new TestPassthroughStage();
        final Stage<Long, Long, TestPipelineContext> requiresBar = new TestPassthroughStage();
        final Stage<Long, Long, TestPipelineContext> segmentStage = segmentStage(requiresFoo);

        providesFoo.providesToContext(TestMemberDefinitions.FOO, "foo");
        requiresFoo.requiresFromContext(TestMemberDefinitions.FOO);
        requiresFoo.providesToContext(TestMemberDefinitions.BAR, 2);
        // No one provides BAR so this should trigger a PipelineContextMemberException.
        requiresBar.requiresFromContext(TestMemberDefinitions.BAR);

        new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
            .addStage(providesFoo)
            .addStage(segmentStage)
            .finalStage(requiresBar));
    }

    /**
     * Test that a valid context member setup involving nested
     * interior pipeline segments does not throw any exceptions.
     */
    @Test
    public void testValidContextMembersForNestedSegment() {
        final Stage<Long, Long, TestPipelineContext> providesFoo = new TestPassthroughStage();
        final Stage<Long, Long, TestPipelineContext> requiresFoo = new TestPassthroughStage();
        final Stage<Long, Long, TestPipelineContext> requiresBar = new TestPassthroughStage();

        final Stage<Long, Long, TestPipelineContext> nestedSegmentStage = segmentStage(requiresFoo);
        final Stage<Long, Long, TestPipelineContext> segmentStage = segmentStage(providesFoo, nestedSegmentStage);
        new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .addStage(segmentStage)
                .finalStage(requiresBar));

    }

    /**
     * Test that a pipeline segment that has an unfulfilled context member requirement
     * will throw an exception.
     */
    @Test(expected = PipelineContextMemberException.class)
    public void testMissingRequirementForSegment() {
        final Stage<Long, Long, TestPipelineContext> requiresFoo = new TestPassthroughStage();
        final Stage<Long, Long, TestPipelineContext> requiresBar = new TestPassthroughStage();
        final Stage<Long, Long, TestPipelineContext> segmentStage = segmentStage(requiresFoo, requiresBar);

        segmentStage.providesToContext(TestMemberDefinitions.FOO, "foo");
        requiresFoo.requiresFromContext(TestMemberDefinitions.FOO);
        // No one provides BAR so this should trigger a PipelineContextMemberException.
        requiresBar.requiresFromContext(TestMemberDefinitions.BAR);

        new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .finalStage(segmentStage));
    }

    /**
     * Test that a pipeline segment that provides a context member already provided
     * by an earlier stage will throw an exception.
     */
    @Test(expected = PipelineContextMemberException.class)
    public void testDuplicateProviderForSegment() {
        final Stage<Long, Long, TestPipelineContext> alsoProvidesFoo = new TestPassthroughStage();
        final Stage<Long, Long, TestPipelineContext> segmentStage = segmentStage(alsoProvidesFoo);

        segmentStage.providesToContext(TestMemberDefinitions.FOO, "foo");
        alsoProvidesFoo.providesToContext(TestMemberDefinitions.FOO, "other-foo");

        new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
            .finalStage(segmentStage));
    }

    /**
     * Ensure that a PipelineStageException exception in the segment kills the pipeline.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testPipelineExceptionInSegment() throws Exception {
        final Stage<Long, Long, TestPipelineContext> blowup = Mockito.spy(new NamedTestPassthroughStage("blowup"));
        final Stage<Long, Long, TestPipelineContext> nestedSegmentStage = SegmentDefinition
            .finalStage(blowup)
            .asStage("nestedStage");

        final Stage<Long, Long, TestPipelineContext> neverRun = Mockito.spy(new NamedTestPassthroughStage("never run"));
        final Stage<Long, Long, TestPipelineContext> segmentStage = segmentStage(nestedSegmentStage, neverRun);
        final Stage<Long, Long, TestPipelineContext> otherStage = Mockito.spy(new NamedTestPassthroughStage("OtherStage"));
        final TestPipeline<Long, Long> pipeline = new TestPipeline<>(
            PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .addStage(segmentStage)
                .finalStage(otherStage));

        when(blowup.executeStage(anyLong())).thenThrow(new PipelineStageException("We blew up!"));
        try {
            pipeline.run(1L);
            fail();
        } catch (PipelineException e) {
            // We should never execute the stages after the blowup stage blows up
            verify(neverRun, never()).executeStage(anyLong());
            verify(otherStage, never()).executeStage(anyLong());
        }

        assertThat(pipeline.getSummary().toString(), containsString("We blew up!"));
    }

    /**
     * Ensure that a RuntimeException in the segment kills the pipeline.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testRuntimeExceptionInSegment() throws Exception {
        final Stage<Long, Long, TestPipelineContext> blowup = Mockito.spy(new NamedTestPassthroughStage("blowup"));
        final Stage<Long, Long, TestPipelineContext> nestedSegmentStage = SegmentDefinition
            .finalStage(blowup)
            .asStage("nestedStage");

        final Stage<Long, Long, TestPipelineContext> neverRun = Mockito.spy(new NamedTestPassthroughStage("never run"));
        final Stage<Long, Long, TestPipelineContext> segmentStage = segmentStage(nestedSegmentStage, neverRun);
        final Stage<Long, Long, TestPipelineContext> otherStage = Mockito.spy(new NamedTestPassthroughStage("OtherStage"));
        final TestPipeline<Long, Long> pipeline = new TestPipeline<>(
            PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .addStage(segmentStage)
                .finalStage(otherStage));

        when(blowup.executeStage(anyLong())).thenThrow(new RuntimeException("runtime exception!"));
        try {
            pipeline.run(1L);
            fail();
        } catch (PipelineException e) {
            // We should never execute the stages after the blowup stage blows up
            verify(neverRun, never()).executeStage(anyLong());
            verify(otherStage, never()).executeStage(anyLong());
        }

        assertThat(pipeline.getSummary().toString(), containsString("runtime exception!"));
    }

    /**
     * Test that the {@link SegmentStage#finalizeExecution(boolean)} method is run when the segment
     * executes successfully.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testFinalizeExecutionRunOnSuccessfulSegment() throws Exception {
        final Stage<Long, Long, TestPipelineContext> foo = new NamedTestPassthroughStage("foo");
        final SegmentStage<Long, Long, Long, Long, TestPipelineContext> nestedSegmentStage = Mockito.spy(SegmentDefinition
            .finalStage(foo)
            .asStage("nestedStage"));

        final SegmentStage<Long, Long, Long, Long, TestPipelineContext> segmentStage = Mockito.spy(segmentStage(nestedSegmentStage));
        final Stage<Long, Long, TestPipelineContext> otherStage = new NamedTestPassthroughStage("OtherStage");
        final TestPipeline<Long, Long> pipeline = new TestPipeline<>(
            PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .addStage(segmentStage)
                .finalStage(otherStage));

        pipeline.run(1L);
        verify(nestedSegmentStage).finalizeExecution(eq(true));
        verify(segmentStage).finalizeExecution(eq(true));
    }

    /**
     * Test that the {@link SegmentStage#finalizeExecution(boolean)} method is run when the segment
     * throws an exception and fails.
     *
     * @throws Exception on exception.
     */
    @Test
    public void testFinalizeExecutionRunOnException() throws Exception {
        final Stage<Long, Long, TestPipelineContext> blowup = Mockito.spy(new NamedTestPassthroughStage("blowup"));
        final SegmentStage<Long, Long, Long, Long, TestPipelineContext> nestedSegmentStage = Mockito.spy(SegmentDefinition
            .finalStage(blowup)
            .asStage("nestedStage"));

        final Stage<Long, Long, TestPipelineContext> neverRun = new NamedTestPassthroughStage("never run");
        final SegmentStage<Long, Long, Long, Long, TestPipelineContext> segmentStage = Mockito.spy(
            segmentStage(nestedSegmentStage, neverRun));
        final Stage<Long, Long, TestPipelineContext> otherStage = new NamedTestPassthroughStage("OtherStage");
        final TestPipeline<Long, Long> pipeline = new TestPipeline<>(
            PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .addStage(segmentStage)
                .finalStage(otherStage));

        when(blowup.executeStage(anyLong())).thenThrow(new PipelineStageException("We blew up!"));

        try {
            pipeline.run(1L);
            fail();
        } catch (PipelineException e) {
            // We should never execute the stages after the blowup stage blows up
            verify(nestedSegmentStage).finalizeExecution(eq(false));
            verify(segmentStage).finalizeExecution(eq(false));
        }
    }

    @SafeVarargs
    private final SegmentStage<Long, Long, Long, Long, TestPipelineContext> segmentStage(
        final Stage<Long, Long, TestPipelineContext>... otherStages) {
        final Stage<Long, Long, TestPipelineContext> stage = new TestPassthroughStage();

        final SegmentDefinitionBuilder<Long, Long, TestPipelineContext> segmentDefinitionBuilder = SegmentDefinition.newBuilder();
        for (Stage<Long, Long, TestPipelineContext> other : otherStages) {
            segmentDefinitionBuilder.addStage(other);
        }

        segmentDefinitionBuilder.addStage(stage);

        return new TestSegmentStage(segmentDefinitionBuilder.finalStage(stage));
    }
}
