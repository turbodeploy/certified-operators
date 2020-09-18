package com.vmturbo.components.common.pipeline;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import java.util.Arrays;

import org.junit.Test;

import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestMemberDefinitions;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestPassthroughStage;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestPipeline;
import com.vmturbo.components.common.pipeline.PipelineTestUtilities.TestPipelineContext;

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
}
