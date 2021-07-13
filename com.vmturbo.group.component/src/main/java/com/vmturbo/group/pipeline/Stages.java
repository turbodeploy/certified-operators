package com.vmturbo.group.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import io.grpc.StatusRuntimeException;

import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSets;

import org.apache.logging.log4j.Level;
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
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.group.db.tables.pojos.GroupSupplementaryInfo;
import com.vmturbo.group.group.GroupEnvironment;
import com.vmturbo.group.group.GroupEnvironmentTypeResolver;
import com.vmturbo.group.group.GroupSeverityCalculator;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.service.CachingMemberCalculator;
import com.vmturbo.group.service.CachingMemberCalculator.RegroupingResult;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.group.service.TransactionProvider;

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

        private final TransactionProvider transactionProvider;

        private final int batchSize;

        private final ExecutorService executorService;

        /**
         * Constructor for the stage.
         *
         * @param memberCache group membership cache.
         * @param searchServiceRpc gRPC service for requests to repository.
         * @param groupEnvironmentTypeResolver utility class to get group environment.
         * @param severityCalculator calculates severity for groups.
         * @param groupStore used to store the updated info to the database.
         * @param transactionProvider used to update group supplementary info in a transaction.
         * @param executorService thread pool for calculations & ingestion.
         * @param batchSize the maximum number of groups to be processed by each thread during group
         *                  supplementary info ingestion.
         */
        public StoreSupplementaryGroupInfoStage(@Nonnull CachingMemberCalculator memberCache,
                @Nonnull final SearchServiceBlockingStub searchServiceRpc,
                @Nonnull final GroupEnvironmentTypeResolver groupEnvironmentTypeResolver,
                @Nonnull final GroupSeverityCalculator severityCalculator,
                @Nonnull final IGroupStore groupStore,
                @Nonnull final TransactionProvider transactionProvider,
                @Nonnull final ExecutorService executorService,
                final int batchSize) {
            this.memberCache = memberCache;
            this.searchServiceRpc = searchServiceRpc;
            this.groupEnvironmentTypeResolver = groupEnvironmentTypeResolver;
            this.severityCalculator = severityCalculator;
            this.groupStore = groupStore;
            this.transactionProvider = transactionProvider;
            this.executorService = executorService;
            this.batchSize = batchSize;
        }

        @Nonnull
        @Override
        public Status passthrough(LongSet input) throws InterruptedException {
            final StopWatch stopWatch =
                    new StopWatch("Calculate and store group supplementary info");
            // fetch entity info from repository
            Map<Long, EntityWithOnlyEnvironmentTypeAndTargets> entitiesWithEnvironmentMap =
                    fetchEntitiesWithEnvironment();
            if (entitiesWithEnvironmentMap == null) {
                return Status.failed("Request for entities failed. Group supplementary info cannot "
                        + "be updated.");
            }
            Multimap<Long, Long> discoveredGroups = groupStore.getDiscoveredGroupsWithTargets();
            stopWatch.start("Group Supplementary Info calculation & ingestion");
            // split the groups into batches and assign each batch to a different thread.
            int numberOfFailedIngestions = 0;
            List<Long> inputList = new ArrayList<>(input);
            List<List<Long>> workloads = Lists.partition(inputList, this.batchSize);
            logger.trace("Splitting {} groups into {} batches.", input::size, workloads::size);
            List<Future<SupplementaryInfoIngestionResult>> futures =
                    new ArrayList<>(workloads.size());
            for (List<Long> workload : workloads) {
                futures.add(executorService.submit(() -> calculateAndIngestSupplementaryInfo(
                        workload, entitiesWithEnvironmentMap, discoveredGroups)));
            }
            SupplementaryInfoIngestionResult totalResult = new SupplementaryInfoIngestionResult();
            for (Future<SupplementaryInfoIngestionResult> future : futures) {
                try {
                    SupplementaryInfoIngestionResult result = future.get();
                    if (result != null) {
                        totalResult.mergeResult(result);
                    } else {
                        numberOfFailedIngestions++;
                    }
                } catch (ExecutionException e) {
                    logger.warn("Retrieving the group supplementary info ingestion results from"
                            + " a worker thread failed. Error: " + e.getCause());
                    numberOfFailedIngestions++;
                }
            }
            stopWatch.stop();
            logger.info(stopWatch);
            if (numberOfFailedIngestions == workloads.size()) {
                return Status.failed("Group supplementary info ingestion failed.");
            } else {
                if (numberOfFailedIngestions > 0) {
                    logger.warn("Group supplementary info ingestion succeeded only partially. {}"
                            + " out of {} batches failed.", numberOfFailedIngestions,
                            workloads.size());
                }
                return Status.success(totalResult.toString());
            }
        }

        /**
         * Method executed by worker threads. Calculates and ingests supplementary info for the
         * given list of groups.
         *
         * @param groups the list of groups to calculate and ingest supplementary info for.
         * @param entitiesWithEnvironmentMap a map containing information about the environment type
         *                                   of all entities.
         * @param discoveredGroups a map that contains discovered groups.
         * @return a {@link SupplementaryInfoIngestionResult} object that contains the result of the
         *         worker's execution.
         * @throws InterruptedException if the thread gets interrupted
         */
        private SupplementaryInfoIngestionResult calculateAndIngestSupplementaryInfo(
                final List<Long> groups,
                final Map<Long, EntityWithOnlyEnvironmentTypeAndTargets> entitiesWithEnvironmentMap,
                final Multimap<Long, Long> discoveredGroups) throws InterruptedException {
            final SupplementaryInfoIngestionResult result = new SupplementaryInfoIngestionResult();
            final Map<Long, GroupSupplementaryInfo> groupsToInsert = new HashMap<>();
            final MultiStageTimer timer = new MultiStageTimer(logger);
            timer.start("Group supplementary info calculation");
            // iterate over all groups and calculate supplementary info
            for (long groupId : groups) {
                // get group's members
                final Set<Long> groupEntities;
                try {
                    groupEntities = memberCache.getGroupMembers(groupStore,
                            Collections.singleton(groupId), true);
                } catch (RuntimeException | StoreOperationException e) {
                    logger.error("Skipped supplementary info calculation for group with "
                            + "uuid {} due to failure to retrieve its entities. "
                            + "Error: ", groupId, e);
                    continue;
                }
                // calculate environment type based on members' environment type
                GroupEnvironment groupEnvironment =
                        groupEnvironmentTypeResolver.getEnvironmentAndCloudTypeForGroup(groupStore,
                                groupId,
                                groupEntities.stream()
                                        .map(entitiesWithEnvironmentMap::get)
                                        .filter(Objects::nonNull)
                                        .collect(Collectors.toSet()),
                                discoveredGroups);
                // calculate severity based on members' severity
                Severity groupSeverity = severityCalculator.calculateSeverity(groupEntities);
                // increment counts
                result.addEnvironmentType(groupEnvironment.getEnvironmentType());
                result.addCloudType(groupEnvironment.getCloudType());
                result.addSeverity(groupSeverity);
                boolean isEmpty = groupEntities.isEmpty();
                groupsToInsert.put(groupId, new GroupSupplementaryInfo(groupId, isEmpty,
                        groupEnvironment.getEnvironmentType().getNumber(),
                        groupEnvironment.getCloudType().getNumber(),
                        groupSeverity.getNumber()));
            }
            timer.start("Group supplementary info ingestion");
            // update database records in a batch
            try {
                transactionProvider.transaction(stores -> {
                    stores.getGroupStore().updateBulkGroupSupplementaryInfo(groupsToInsert);
                    return true;
                });
            } catch (StoreOperationException e) {
                logger.error("Group supplementary info ingestion failed for a batch.", e);
                logger.trace("List of groups in failed batch: {}", () -> groups);
                return null;
            } finally {
                timer.stop();
            }
            timer.log(Level.DEBUG, "Group supplementary info calculation & ingestion worker "
                    + "results: ", Detail.STAGE_SUMMARY);
            return result;
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
         * Class that holds the results of the execution of a single worker thread that calculates
         * and ingests supplementary info for a given list of groups.
         * It contains group counts (broken down by environment type, cloud type and severity).
         */
        private static class SupplementaryInfoIngestionResult {
            private final Map<EnvironmentTypeEnum.EnvironmentType, Long> groupCountsByEnvironmentType;
            private final Map<CloudTypeEnum.CloudType, Long> groupCountsByCloudType;
            private final Map<Severity, Long> groupCountsBySeverity;

            SupplementaryInfoIngestionResult() {
                groupCountsByEnvironmentType =
                        new EnumMap<>(EnvironmentTypeEnum.EnvironmentType.class);
                groupCountsByCloudType =  new EnumMap<>(CloudTypeEnum.CloudType.class);
                groupCountsBySeverity = new EnumMap<>(Severity.class);
            }

            void addEnvironmentType(EnvironmentTypeEnum.EnvironmentType environmentType) {
                groupCountsByEnvironmentType.merge(environmentType, 1L, Long::sum);
            }

            void addCloudType(CloudTypeEnum.CloudType cloudType) {
                groupCountsByCloudType.merge(cloudType, 1L, Long::sum);
            }

            void addSeverity(Severity severity) {
                groupCountsBySeverity.merge(severity, 1L, Long::sum);
            }

            void mergeResult(SupplementaryInfoIngestionResult input) {
                input.groupCountsByEnvironmentType.forEach((envType, count) ->
                        this.groupCountsByEnvironmentType.merge(envType, count, Long::sum));
                input.groupCountsByCloudType.forEach((cloudType, count) ->
                        this.groupCountsByCloudType.merge(cloudType, count, Long::sum));
                input.groupCountsBySeverity.forEach((severity, count) ->
                        this.groupCountsBySeverity.merge(severity, count, Long::sum));
            }

            /**
             * Creates a small summary of the stage, reporting group counts by environment & cloud
             * type and severity.
             *
             * @return the constructed string message.
             */
            @Override
            public String toString() {
                StringBuilder result = new StringBuilder();
                result.append("Group counts by category:").append(System.lineSeparator());
                addInfo(groupCountsByEnvironmentType, "Environment Type", result);
                addInfo(groupCountsByCloudType, "Cloud Type", result);
                addInfo(groupCountsBySeverity, "Severity", result);
                return result.toString();
            }

            private static <T> void addInfo(Map<T, Long> map, String title, StringBuilder sb) {
                sb.append("  ");
                sb.append(title);
                sb.append(":");
                sb.append(System.lineSeparator());
                map.forEach((type, count) -> sb.append("    ").append(type).append(" : ")
                        .append(count).append(System.lineSeparator()));
            }
        }
    }
}
