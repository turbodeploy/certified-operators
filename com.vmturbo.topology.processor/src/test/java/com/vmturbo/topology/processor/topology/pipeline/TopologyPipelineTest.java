package com.vmturbo.topology.processor.topology.pipeline;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.topology.pipeline.CachedTopology.CachedTopologyResult;
import com.vmturbo.topology.processor.topology.pipeline.Stages.CacheWritingConstructTopologyFromStitchingContextStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.CachingConstructTopologyFromStitchingContextStage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PassthroughStage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.Stage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.StageResult;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.Status;
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

        final TopologyPipeline<Long,Long> simplePipeline =
            TopologyPipeline.<Long, Long>newBuilder(context)
                .addStage(stage)
                .build();

        // Shouldn't throw an exception.
        simplePipeline.run(10L);
        // Should have been invoked twice.
        verify(stage, times(2)).passthrough(10L);
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

    @Test
    public void testCachingStage() throws Exception {
        final CachedTopology cachedTopo = mock(CachedTopology.class);
        TopologyEntity.Builder builder = mock(TopologyEntity.Builder.class);
        final Map<Long, TopologyEntity.Builder> cachedMap = Maps.newHashMap();
        cachedMap.put(1L, builder);
        final CachedTopologyResult cachedResult = new CachedTopologyResult(null, cachedMap);
        when(cachedTopo.getTopology()).thenReturn(cachedResult);
        when(builder.snapshot()).thenReturn(builder);
        final EntityStore entityStore = mock(EntityStore.class);
        final TopologyPipeline<EntityStore, Map<Long, TopologyEntity.Builder>> pipeline =
            TopologyPipeline.<EntityStore, Map<Long, TopologyEntity.Builder>>newBuilder(context)
                .addStage(new CachingConstructTopologyFromStitchingContextStage(cachedTopo))
                .build();
        assertThat(pipeline.run(entityStore), is(cachedMap));
    }

    @Test
    public void testCacheWritingStage() throws Exception {
        final CachedTopology cachedTopo = mock(CachedTopology.class);
        final TopologyEntity.Builder builder = mock(TopologyEntity.Builder.class);
        final Map<Long, TopologyEntity.Builder> cachedMap = Maps.newHashMap();
        cachedMap.put(1L, builder);
        final StitchingContext stitchingContext = mock(StitchingContext.class);
        when(stitchingContext.constructTopology()).thenReturn(cachedMap);
        final TopologyPipeline<StitchingContext, Map<Long, TopologyEntity.Builder>> pipeline =
            TopologyPipeline.<StitchingContext, Map<Long, TopologyEntity.Builder>>newBuilder(context)
                .addStage(new CacheWritingConstructTopologyFromStitchingContextStage(cachedTopo))
                .build();
        pipeline.run(stitchingContext);
        verify(cachedTopo).updateTopology(cachedMap);
    }

    public static class TestStage extends Stage<Long, Long> {
        @Nonnull
        @Override
        public StageResult<Long> execute(@Nonnull final Long input)
                throws PipelineStageException, InterruptedException {
            return StageResult.withResult(input)
                .andStatus(Status.success());
        }
    }

    public static class TestPassthroughStage extends PassthroughStage<Long> {
        @Override
        public Status passthrough(final Long input) throws PipelineStageException {
            // Don't do anything.
            return Status.success();
        }
    }
}
