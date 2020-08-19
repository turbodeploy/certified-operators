package com.vmturbo.topology.processor.topology.pipeline;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.topology.pipeline.CachedTopology.CachedTopologyResult;
import com.vmturbo.topology.processor.topology.pipeline.Stages.CacheWritingConstructTopologyFromStitchingContextStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.CachingConstructTopologyFromStitchingContextStage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PassthroughStage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.Stage;

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
            new TopologyPipeline<>(PipelineDefinition.<EntityStore, Map<Long, TopologyEntity.Builder>, TopologyPipelineContext>newBuilder(context)
                .finalStage(new CachingConstructTopologyFromStitchingContextStage(cachedTopo)));
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
            new TopologyPipeline<>(PipelineDefinition.<StitchingContext, Map<Long, TopologyEntity.Builder>, TopologyPipelineContext>newBuilder(context)
                .finalStage(new CacheWritingConstructTopologyFromStitchingContextStage(cachedTopo)));
        pipeline.run(stitchingContext);
        verify(cachedTopo).updateTopology(cachedMap);
    }

    public static class TestStage extends Stage<Long, Long> {
        @NotNull
        @Nonnull
        @Override
        public StageResult<Long> execute(@NotNull @Nonnull final Long input)
                throws PipelineStageException, InterruptedException {
            return StageResult.withResult(input)
                .andStatus(Status.success());
        }
    }

    public static class TestPassthroughStage extends PassthroughStage<Long> {
        @NotNull
        @Override
        public Status passthrough(final Long input) throws PipelineStageException {
            // Don't do anything.
            return Status.success();
        }
    }
}
