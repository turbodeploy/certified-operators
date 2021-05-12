package com.vmturbo.components.common.pipeline;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;

import java.util.Arrays;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.NamedTestPassthroughStage;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestMemberDefinitions;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestPassthroughStage;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestPipeline;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestPipelineContext;
import com.vmturbo.components.common.pipeline.SegmentStage.SegmentDefinition;

/**
 * Tests for {@link PipelineTabularDescription}.
 */
public class PipelineTabularDescriptionTest {

    private final TestPipelineContext context = new TestPipelineContext();

    /**
     * Test tabular description.
     */
    @Test
    public void testTabularDescription() {
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

        final String description = abc.tabularDescription("TestPipeline");
        assertThat(description, containsString("TestPipeline"));
        assertThat(description, containsString("Long"));
        assertThat(description, containsString(TestMemberDefinitions.FOO.getName()));
        assertThat(description, containsString(TestMemberDefinitions.BAR.getName()));
        assertThat(description, containsString(TestMemberDefinitions.BAZ.getName()));
    }

    /**
     * Test tabular description for initial context members.
     */
    @Test
    public void testTabularDescriptionForInitialContextMembers() {
        final TestPassthroughStage stage = new TestPassthroughStage();
        stage.requiresFromContext(TestMemberDefinitions.FOO);
        stage.providesToContext(TestMemberDefinitions.BAR, 1);

        final TestPipeline<Long, Long> pipeline = new TestPipeline<>(PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
            .initialContextMember(TestMemberDefinitions.FOO, () -> "foo")
            .initialContextMember(TestMemberDefinitions.BAZ, () -> Arrays.asList(1L, 2L))
            .finalStage(stage));
        final String description = pipeline.tabularDescription("TestPipeline");
        // Note that even though it was initially provided, BAZ should not appear anywhere
        // because it was not required
        assertThat(description, containsString("foo"));
        assertThat(description, not(containsString("baz")));
    }

    /**
     * Test tabular description for pipeline segments.
     */
    @Test
    public void testTabularDescriptionForSegments() {
        final TestPassthroughStage stage1 = new TestPassthroughStage();
        final TestPassthroughStage stage2 = new TestPassthroughStage();
        final TestPassthroughStage stage3 = new TestPassthroughStage();

        stage1.providesToContext(TestMemberDefinitions.FOO, "foo");

        stage2.providesToContext(TestMemberDefinitions.BAZ, Arrays.asList(1L, 2L));
        stage2.requiresFromContext(TestMemberDefinitions.FOO);

        stage3.requiresFromContext(TestMemberDefinitions.BAZ);
        stage3.requiresFromContext(TestMemberDefinitions.BAR);

        final NamedTestPassthroughStage segmentFirstStage = new NamedTestPassthroughStage("SegmentFirstStage");
        segmentFirstStage.requiresFromContext(TestMemberDefinitions.FOO);
        segmentFirstStage.providesToContext(TestMemberDefinitions.BAR, 1);

        final TestPipeline<Long, Long> abc = new TestPipeline<>(
            PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .addStage(stage1)
                .addStage(stage2)
                .addStage(SegmentDefinition
                    .addStage(segmentFirstStage)
                    .finalStage(new NamedTestPassthroughStage("SegmentFinalStage"))
                    .asStage("MySegment"))
                .finalStage(stage3));

        final String description = abc.tabularDescription("TestPipeline");
        assertThat(description, containsString("TestPipeline"));
        assertThat(description, containsString("Long"));
        assertThat(description, containsString("Start MySegment"));
        assertThat(description, containsString("Finish MySegment"));
        assertThat(description, matchesPattern("[\\s\\S]*->SegmentFirstStage.*Long.*Long.*bar.*foo.*foo[\\s\\S]*"));
        assertThat(description, containsString("->SegmentFinalStage"));
        assertThat(description, containsString(TestMemberDefinitions.FOO.getName()));
        assertThat(description, containsString(TestMemberDefinitions.BAR.getName()));
        assertThat(description, containsString(TestMemberDefinitions.BAZ.getName()));
    }

    /**
     * Test tabular description for nested pipeline segments.
     */
    @Test
    public void testTabularDescriptionForNestedSegments() {
        final TestPassthroughStage stage1 = new TestPassthroughStage();
        final TestPassthroughStage stage2 = new TestPassthroughStage();
        final TestPassthroughStage stage3 = new TestPassthroughStage();

        stage1.providesToContext(TestMemberDefinitions.FOO, "foo");
        stage2.providesToContext(TestMemberDefinitions.BAZ, Arrays.asList(1L, 2L));
        stage2.requiresFromContext(TestMemberDefinitions.FOO);
        stage3.requiresFromContext(TestMemberDefinitions.BAZ);
        stage3.requiresFromContext(TestMemberDefinitions.BAR);

        final StringToLongStage segmentFirstStage = new StringToLongStage("SegmentFirstStage");
        segmentFirstStage.requiresFromContext(TestMemberDefinitions.FOO);
        segmentFirstStage.providesToContext(TestMemberDefinitions.BAR, 1);

        final TestPipeline<Long, Long> abc = new TestPipeline<>(
            PipelineDefinition.<Long, Long, TestPipelineContext>newBuilder(context)
                .addStage(stage1)
                .addStage(stage2)
                .addStage(new TransposeSegment(SegmentDefinition
                    .addStage(segmentFirstStage)
                    .addStage(SegmentDefinition
                        .finalStage(new NamedTestPassthroughStage("NestedSegmentStage"))
                        .asStage("NestedSegment"))
                    .finalStage(new LongToStringStage("SegmentFinalStage"))))
                .finalStage(stage3));

        final String description = abc.tabularDescription("TestPipeline");
        assertThat(description, containsString("TestPipeline"));
        assertThat(description, containsString("Long"));
        assertThat(description, containsString("Start TransposeSegment"));
        assertThat(description, containsString("Finish TransposeSegment"));
        assertThat(description, matchesPattern("[\\s\\S]*->SegmentFirstStage.*String.*Long.*bar.*foo.*foo[\\s\\S]*"));
        assertThat(description, containsString("->Start NestedSegment"));
        assertThat(description, containsString("--->NestedSegmentStage"));
        assertThat(description, containsString("->Finish NestedSegment"));
        assertThat(description, containsString("->SegmentFinalStage"));
        assertThat(description, containsString(TestMemberDefinitions.FOO.getName()));
        assertThat(description, containsString(TestMemberDefinitions.BAR.getName()));
        assertThat(description, containsString(TestMemberDefinitions.BAZ.getName()));
    }

    /**
     * A helper class.
     */
    private static class TransposeSegment extends SegmentStage<Long, String, String, Long, TestPipelineContext> {
        /**
         * Create a new TransposeSegment.
         *
         * @param segmentDefinition The definition.
         */
        private TransposeSegment(@Nonnull SegmentDefinition<String, String, TestPipelineContext> segmentDefinition) {
            super(segmentDefinition);
        }

        @Nonnull
        @Override
        protected String setupExecution(@Nonnull Long input) {
            return "foo";
        }

        @Nonnull
        @Override
        protected StageResult<Long> completeExecution(@Nonnull StageResult<String> segmentResult) {
            return segmentResult.transpose(1L);
        }

        @Override
        protected void finalizeExecution(boolean executionCompleted) {
            // nothing to do
        }
    }

    /**
     * A stage that takes a Long and returns a String.
     */
    private static class LongToStringStage extends Stage<Long, String, TestPipelineContext> {
        private final String name;

        private LongToStringStage(@Nonnull final String name) {
            this.name = Objects.requireNonNull(name);
        }

        @Nonnull
        @Override
        public String getName() {
            return name;
        }

        @Nonnull
        @Override
        protected StageResult<String> executeStage(@Nonnull Long input) {
            return StageResult.withResult("foo").andStatus(Status.success());
        }
    }

    /**
     * A stage that takes a String and returns a Long.
     */
    private static class StringToLongStage extends Stage<String, Long, TestPipelineContext> {
        private final String name;

        private StringToLongStage(@Nonnull final String name) {
            this.name = Objects.requireNonNull(name);
        }

        @Nonnull
        @Override
        public String getName() {
            return name;
        }

        @Nonnull
        @Override
        protected StageResult<Long> executeStage(@Nonnull String input) {
            return StageResult.withResult(1L).andStatus(Status.success());
        }
    }
}
