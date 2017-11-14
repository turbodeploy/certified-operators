package com.vmturbo.topology.processor.topology.pipeline;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PassthroughStage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.Stage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.TopologyPipelineException;

public class TopologyPipelineTest {
    private final TopologyPipelineContext context = mock(TopologyPipelineContext.class);

    @Before
    public void setup() {
        when(context.getTopologyTypeName()).thenReturn("test");
        when(context.getTopologyInfo()).thenReturn(TopologyInfo.newBuilder()
                .setTopologyContextId(7L)
                .setTopologyId(1L)
                .build());
    }

    @Test
    public void testTwoStagePipeline() throws Exception {
        final TestStage stage1 = spy(new TestStage());
        final TestPassthroughStage stage2 = spy(new TestPassthroughStage());
        final TopologyPipeline<Long,Long> simplePipeline =
            TopologyPipeline.<Long, Long>newBuilder(context)
                .addStage(stage1)
                .addStage(stage2)
                .build();


        assertThat(stage1.getContext(), is(context));
        assertThat(stage2.getContext(), is(context));
        assertThat(simplePipeline.run(10L), is(10L));

        // Assert that they were changed in order.
        final InOrder order = inOrder(stage1, stage2);
        order.verify(stage1).execute(any());
        order.verify(stage2).execute(any());
    }

    @Test(expected = TopologyPipelineException.class)
    public void testPipelineException() throws Exception {
        final TestStage stage1 = spy(new TestStage());
        when(stage1.execute(eq(10L))).thenThrow(new PipelineStageException("ERROR"));

        final TopologyPipeline<Long,Long> simplePipeline =
            TopologyPipeline.<Long, Long>newBuilder(context)
                .addStage(stage1)
                .build();
        simplePipeline.run(10L);
    }

    @Test(expected = TopologyPipelineException.class)
    public void testPipelineRequiredPassthroughStageException() throws Exception {
        final TestPassthroughStage stage = spy(new TestPassthroughStage());
        when(stage.execute(eq(10L))).thenThrow(new PipelineStageException("ERROR"));
        when(stage.required()).thenReturn(true);

        final TopologyPipeline<Long,Long> simplePipeline =
                TopologyPipeline.<Long, Long>newBuilder(context)
                        .addStage(stage)
                        .build();
        simplePipeline.run(10L);
    }

    @Test(expected = TopologyPipelineException.class)
    public void testPipelineRequiredPassthroughStageRuntimeException() throws Exception {
        final TestPassthroughStage stage = spy(new TestPassthroughStage());
        when(stage.execute(eq(10L))).thenThrow(new RuntimeException("ERROR"));
        when(stage.required()).thenReturn(true);

        final TopologyPipeline<Long,Long> simplePipeline =
                TopologyPipeline.<Long, Long>newBuilder(context)
                        .addStage(stage)
                        .build();
        simplePipeline.run(10L);
    }

    @Test
    public void testPipelineNotRequiredStageException() throws Exception {
        final TestPassthroughStage stage = spy(new TestPassthroughStage());
        doThrow(new PipelineStageException("ERROR")).when(stage).passthrough(eq(10L));
        when(stage.required()).thenReturn(false);

        final TopologyPipeline<Long,Long> simplePipeline =
                TopologyPipeline.<Long, Long>newBuilder(context)
                        .addStage(stage)
                        .build();
        assertThat(simplePipeline.run(10L), is(10L));
    }

    @Test
    public void testPipelineNotRequiredStageRuntimeException() throws Exception {
        final TestPassthroughStage stage = spy(new TestPassthroughStage());
        doThrow(new RuntimeException("ERROR")).when(stage).passthrough(eq(10L));
        when(stage.required()).thenReturn(false);

        final TopologyPipeline<Long,Long> simplePipeline =
                TopologyPipeline.<Long, Long>newBuilder(context)
                        .addStage(stage)
                        .build();
        assertThat(simplePipeline.run(10L), is(10L));
    }

    public static class TestStage extends Stage<Long, Long> {
        @Nonnull
        @Override
        public Long execute(@Nonnull final Long input)
                throws PipelineStageException, InterruptedException {
            return input;
        }
    }

    public static class TestPassthroughStage extends PassthroughStage<Long> {
        @Override
        public void passthrough(final Long input) throws PipelineStageException {
            // Don't do anything.
        }
    }
}
