package com.vmturbo.group.pipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Multimap;

import io.grpc.StatusRuntimeException;

import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.StopWatch;

import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.common.CloudTypeEnum;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithOnlyEnvironmentTypeAndTargets;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.components.common.pipeline.Pipeline.PassthroughStage;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.components.common.pipeline.Stage;
import com.vmturbo.group.db.tables.pojos.GroupSupplementaryInfo;
import com.vmturbo.group.group.GroupEnvironment;
import com.vmturbo.group.group.GroupEnvironmentTypeResolver;
import com.vmturbo.group.group.GroupSeverityCalculator;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.service.CachingMemberCalculator;
import com.vmturbo.group.service.CachingMemberCalculator.RegroupingResult;
import com.vmturbo.group.service.StoreOperationException;

/**
 * A wrapper class for implementations of the various stages of a {@link GroupInfoUpdatePipeline}.
 * Since the stages are very few, it makes some sense to keep them in one place for now.
 */
public class Stages {
    private static final Logger logger = LogManager.getLogger();

    /**
     * This stage updates the group membership cache.
     * This should run before any other stage that does calculations that depend on the members of
     * the group, in order to benefit from the quick member retrieval.
     */
    public static class UpdateGroupMembershipCacheStage extends
            Stage<GroupInfoUpdatePipelineInput, LongSet, GroupInfoUpdatePipelineContext> {

        private final CachingMemberCalculator memberCache;

        /**
         * Constructor for the stage.
         *
         * @param memberCache group membership cache.
         */
        public UpdateGroupMembershipCacheStage(@Nonnull CachingMemberCalculator memberCache) {
            this.memberCache = memberCache;
        }

        @Nonnull
        @Override
        public StageResult<LongSet> executeStage(GroupInfoUpdatePipelineInput input) {
            // Update the group membership cache
            final RegroupingResult result = memberCache.regroup();
            if (!result.isSuccessfull()) {
                return StageResult.withResult((LongSet)LongSets.EMPTY_SET)
                        .andStatus(Status.failed("Regrouping failed."));
            }
            return StageResult.withResult(result.getResolvedGroupsIds()).andStatus(Status.success(
                    "Regrouping result:\n"
                    + "  " + result.getResolvedGroupsIds().size() + " groups\n"
                    + "  " + result.getTotalMemberCount() + " total members"
                    + " (" + result.getDistinctEntitiesCount() + " distinct entities)\n"
                    + "  Cached members memory: " + result.getMemory()
            ));
        }
    }

    /**
     * This stage stores supplementary group info (such as environment/cloud type) in the database.
     * It must be run after the group membership cache has been updated, in order to use it to
     * efficiently calculate the various info that derive from members.
     */
    public static class StoreSupplementaryGroupInfoStage extends
            PassthroughStage<LongSet, GroupInfoUpdatePipelineContext> {

        private final CachingMemberCalculator memberCache;

        private final SearchServiceBlockingStub searchServiceRpc;

        private final GroupEnvironmentTypeResolver groupEnvironmentTypeResolver;

        private final GroupSeverityCalculator severityCalculator;

        private final IGroupStore groupStore;

        /**
         * Constructor for the stage.
         *
         * @param memberCache group membership cache.
         * @param searchServiceRpc gRPC service for requests to repository.
         * @param groupEnvironmentTypeResolver utility class to get group environment.
         * @param severityCalculator calculates severity for groups.
         * @param groupStore used to store the updated info to the database.
         */
        public StoreSupplementaryGroupInfoStage(@Nonnull CachingMemberCalculator memberCache,
                @Nonnull final SearchServiceBlockingStub searchServiceRpc,
                @Nonnull final GroupEnvironmentTypeResolver groupEnvironmentTypeResolver,
                @Nonnull final GroupSeverityCalculator severityCalculator,
                @Nonnull final IGroupStore groupStore) {
            this.memberCache = memberCache;
            this.searchServiceRpc = searchServiceRpc;
            this.groupEnvironmentTypeResolver = groupEnvironmentTypeResolver;
            this.severityCalculator = severityCalculator;
            this.groupStore = groupStore;
        }

        @Nonnull
        @Override
        public Status passthrough(LongSet input) {
            final StopWatch stopWatch = new StopWatch("storeSupplementaryGroupInfo");
            // fetch entity info from repository
            Map<Long, EntityWithOnlyEnvironmentTypeAndTargets> entitiesWithEnvironmentMap =
                    fetchEntitiesWithEnvironment();
            if (entitiesWithEnvironmentMap == null) {
                return Status.failed("Request for entities failed. Group supplementary info cannot "
                        + "be updated.");
            }
            Multimap<Long, Long> discoveredGroups = groupStore.getDiscoveredGroupsWithTargets();
            Map<EnvironmentTypeEnum.EnvironmentType, Long> groupCountsByEnvironmentType =
                    new EnumMap<>(EnvironmentTypeEnum.EnvironmentType.class);
            Map<CloudTypeEnum.CloudType, Long> groupCountsByCloudType =
                    new EnumMap<>(CloudTypeEnum.CloudType.class);
            Map<Severity, Long> groupCountsBySeverity = new EnumMap<>(Severity.class);
            // iterate over all groups and calculate supplementary info
            Collection<GroupSupplementaryInfo> groupsToInsert = new ArrayList<>();
            stopWatch.start("calculateAndIngestDataToDatabase");
            try {
                for (long groupId : input) {
                    // get group's members
                    Set<Long> groupEntities = memberCache.getGroupMembers(groupStore,
                            Collections.singleton(groupId), true);
                    // calculate environment type based on members' environment type
                    GroupEnvironment groupEnvironment =
                            groupEnvironmentTypeResolver.getEnvironmentAndCloudTypeForGroup(groupId,
                                    groupEntities.stream()
                                            .map(entitiesWithEnvironmentMap::get)
                                            .filter(Objects::nonNull)
                                            .collect(Collectors.toSet()),
                                    discoveredGroups);
                    // calculate severity based on members' severity
                    Severity groupSeverity = severityCalculator.calculateSeverity(groupEntities);
                    // increment counts
                    // try to insert 1. if a value is already there, add 1 to it
                    groupCountsByEnvironmentType.merge(
                            groupEnvironment.getEnvironmentType(), 1L, Long::sum);
                    groupCountsByCloudType.merge(groupEnvironment.getCloudType(), 1L, Long::sum);
                    groupCountsBySeverity.merge(groupSeverity, 1L, Long::sum);
                    boolean isEmpty = groupEntities.size() == 0;
                    groupsToInsert.add(new GroupSupplementaryInfo(groupId, isEmpty,
                            groupEnvironment.getEnvironmentType().getNumber(),
                            groupEnvironment.getCloudType().getNumber(),
                            groupSeverity.getNumber()));
                }
                // update database records in a batch
                groupStore.updateBulkGroupSupplementaryInfo(groupsToInsert);
            } catch (StoreOperationException e) {
                return Status.failed("Exception caught: " + e.getMessage());
            } finally {
                stopWatch.stop();
                logger.info("Group supplementary info update took "
                        + stopWatch.getLastTaskTimeMillis() + " ms.");
            }
            return Status.success(successSummary(groupCountsByEnvironmentType,
                    groupCountsByCloudType, groupCountsBySeverity));
        }

        /**
         * Utility method that fetches environment related information (environment type &
         * discovering targets) for all the entities in current topology.
         *
         * @return a map from entity uuid to the entity's environment related information, or null
         *         if the request to repository failed.
         */
        private Map<Long, EntityWithOnlyEnvironmentTypeAndTargets> fetchEntitiesWithEnvironment() {
            final StopWatch stopWatch = new StopWatch("repositoryCallForEntitiesEnvironment");
            SearchEntitiesRequest request = SearchEntitiesRequest.newBuilder()
                    .setSearch(SearchQuery.getDefaultInstance())
                    .setReturnType(Type.WITH_ONLY_ENVIRONMENT_TYPE_AND_TARGETS)
                    .build();
            List<PartialEntity> entitiesWithEnvironment = new ArrayList<>();
            stopWatch.start("GetEntitiesWithEnvironment");
            try {
                searchServiceRpc.searchEntitiesStream(request)
                        .forEachRemaining(e -> entitiesWithEnvironment.addAll(e.getEntitiesList()));
            // When there's no real time topology in repository, we get a status runtime exception.
            } catch (StatusRuntimeException e) {
                return null;
            } finally {
                stopWatch.stop();
                logger.debug("Repository call for info about environment type of entities took "
                        + stopWatch.getLastTaskTimeMillis() + " ms. "
                        + entitiesWithEnvironment.size() + " entities fetched.");
            }
            // store the entities in a hashmap for direct access
            Map<Long, EntityWithOnlyEnvironmentTypeAndTargets> entitiesWithEnvironmentMap =
                    new HashMap<>();
            entitiesWithEnvironment.forEach(e -> entitiesWithEnvironmentMap.put(
                    e.getWithOnlyEnvironmentTypeAndTargets().getOid(),
                    e.getWithOnlyEnvironmentTypeAndTargets()
            ));
            return entitiesWithEnvironmentMap;
        }

        /**
         * Creates a small summary of the stage, reporting group counts by environment & cloud type
         * and severity.
         *
         * @param groupCountsByEnvironmentType group counts broken down by environment type.
         * @param groupCountsByCloudType group counts broken down by cloud type.
         * @param groupCountsBySeverity group counts broken down by severity.
         * @return the constructed string message.
         */
        private String successSummary(
                Map<EnvironmentTypeEnum.EnvironmentType, Long> groupCountsByEnvironmentType,
                Map<CloudTypeEnum.CloudType, Long> groupCountsByCloudType,
                Map<Severity, Long> groupCountsBySeverity) {
            StringBuilder result = new StringBuilder();
            result.append("Group counts by category:\n")
                    .append("  Environment type:\n");
            groupCountsByEnvironmentType.forEach((environmentType, count) -> {
                result.append("    ").append(environmentType).append(" : ").append(count).append("\n");
            });
            result.append("  Cloud type:\n");
            groupCountsByCloudType.forEach((cloudType, count) -> {
                result.append("    ").append(cloudType).append(" : ").append(count).append("\n");
            });
            result.append("  Severity:\n");
            groupCountsBySeverity.forEach((severity, count) -> {
                result.append("    ").append(severity).append(" : ").append(count).append("\n");
            });
            return result.toString();
        }
    }
}
