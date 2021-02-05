package com.vmturbo.group.pipeline;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.group.group.GroupDAO;
import com.vmturbo.group.group.GroupEnvironmentTypeResolver;
import com.vmturbo.group.pipeline.Stages.StoreSupplementaryGroupInfoStage;
import com.vmturbo.group.pipeline.Stages.UpdateGroupMembershipCacheStage;
import com.vmturbo.group.service.CachingMemberCalculator;

/**
 * Factory for {@link GroupInfoUpdatePipeline}s.
 */
public class GroupInfoUpdatePipelineFactory {

    private static final Logger logger = LogManager.getLogger();

    private final CachingMemberCalculator memberCache;

    private final SearchServiceBlockingStub searchServiceRpc;

    private final GroupEnvironmentTypeResolver groupEnvironmentTypeResolver;

    private final GroupDAO groupDAO;

    /**
     * Constructor.
     * @param memberCache group membership cache.
     * @param searchServiceRpc gRPC service for requests to repository.
     * @param groupEnvironmentTypeResolver utility class to get group environment.
     * @param groupDAO for queries to group db.
     */
    public GroupInfoUpdatePipelineFactory(@Nonnull CachingMemberCalculator memberCache,
            @Nonnull final SearchServiceBlockingStub searchServiceRpc,
            @Nonnull final GroupEnvironmentTypeResolver groupEnvironmentTypeResolver,
            @Nonnull final GroupDAO groupDAO) {
        this.memberCache = memberCache;
        this.searchServiceRpc = searchServiceRpc;
        this.groupEnvironmentTypeResolver = groupEnvironmentTypeResolver;
        this.groupDAO = groupDAO;
    }

    /**
     * Creates and returns a new {@link GroupInfoUpdatePipeline} object.
     *
     * @param topologyId the topology id associated with the most recent topology, for reporting
     *                   purposes.
     * @return the new pipeline.
     */
    public GroupInfoUpdatePipeline newPipeline(long topologyId) {
        final GroupInfoUpdatePipelineContext context =
                new GroupInfoUpdatePipelineContext(topologyId);
        return new GroupInfoUpdatePipeline(PipelineDefinition
                .<GroupInfoUpdatePipelineInput, LongSet, GroupInfoUpdatePipelineContext>newBuilder(context)
                .addStage(new UpdateGroupMembershipCacheStage(memberCache))
                .finalStage(new StoreSupplementaryGroupInfoStage(memberCache, searchServiceRpc,
                        groupEnvironmentTypeResolver, groupDAO)));
    }
}
