package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.mapper.StatsMapper.toStatSnapshotApiDTO;
import static com.vmturbo.api.component.external.api.mapper.StatsMapper.toStatsSnapshotApiDtoList;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IStatsService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.api.utils.EncodingUtil;
import com.vmturbo.api.utils.StatsUtils;
import com.vmturbo.api.utils.UrlsHelp;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;

/**
 * Service implementation of Stats
 **/
public class StatsService implements IStatsService {

    private static Logger logger = LogManager.getLogger(StatsService.class);

    private final StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub statsServiceRpc;

    private final PlanServiceBlockingStub planRpcService;

    private final StatPeriodApiInputDTO DEFAULT_STAT_API_INPUT_DTO = new StatPeriodApiInputDTO();

    private final Clock clock;

    private final RepositoryApi repositoryApi;

    private final GroupExpander groupExpander;

    private final GroupServiceBlockingStub groupServiceRpc;

    private final TargetsService targetsService;

    // "headroomVMs" is a constant specified in a query input used for requesting headroom stats
    private final String HEADROOM_VMS = "headroomVMs";

    StatsService(@Nonnull final StatsHistoryServiceBlockingStub statsServiceRpc,
                 @Nonnull final PlanServiceBlockingStub planRpcService,
                 @Nonnull final RepositoryApi repositoryApi,
                 @Nonnull final GroupExpander groupExpander,
                 @Nonnull final Clock clock,
                 @Nonnull final TargetsService targetsService,
                 @Nonnull final GroupServiceBlockingStub groupServiceRpc) {
        this.statsServiceRpc = Objects.requireNonNull(statsServiceRpc);
        this.planRpcService = planRpcService;
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.clock = Objects.requireNonNull(clock);
        this.groupExpander = groupExpander;
        this.targetsService = Objects.requireNonNull(targetsService);
        this.groupServiceRpc = Objects.requireNonNull(groupServiceRpc);
    }

    /**
     * Create a simple response object containing the HATEOS links for the /stats service.
     *
     * @return a simple {@link BaseApiDTO} decorated with the HATEOS links for the /stats service.
     * @throws Exception if there's a problem constructing a URL
     */
    @Override
    public BaseApiDTO getStats() throws Exception {
        BaseApiDTO dto = new BaseApiDTO();

        UrlsHelp.setStatsHelp(dto);

        return dto;
    }

    /**
     * Return stats for an Entity (ServiceEntity or Group) and a uuencoded
     * {@link StatPeriodApiInputDTO} query parameter which modifies the stats search. A group is
     * expanded and the stats averaged over the group contents.
     *
     * @param uuid unique ID of the Entity for which the stats should be gathered
     * @param encodedQuery a uuencoded structure for the {@link StatPeriodApiInputDTO} to modify the
     *                     stats search
     * @return a List of {@link StatSnapshotApiDTO} responses containing the time-based stats
     * snapshots.
     * @throws Exception if there's an error enconding / deconding the string
     */
    @Override
    public List<StatSnapshotApiDTO> getStatsByEntityUuid(String uuid, String encodedQuery)
            throws Exception {

        StatPeriodApiInputDTO inputDto;
        if (encodedQuery != null && !encodedQuery.isEmpty()) {
            String jsonObject = EncodingUtil.decode(encodedQuery);
            ObjectMapper jsonMapper = new ObjectMapper();
            inputDto = jsonMapper.readValue(jsonObject, StatPeriodApiInputDTO.class);
        } else {
            inputDto = DEFAULT_STAT_API_INPUT_DTO;
        }

        return getStatsByEntityQuery(uuid, inputDto);
    }

    /**
     * Return stats for a {@link ServiceEntityApiDTO} given the UUID and an input
     * {@link StatPeriodApiInputDTO} object which modifies the stats search.
     *
     * Note that the ServiceEntity may be a group. In that case, we expand the given UUID into
     * a list of ServiceEntity UUID's, and the results are averaged over the ServiceEntities
     * in the expanded list.
     *
     * The "scope" field of the "inputDto" is ignored.
     *
     * @param uuid the UUID of either a single ServiceEntity or a group.
     * @param inputDto the parameters to further refine this search.
     * @return a list of {@link StatSnapshotApiDTO}s one for each ServiceEntity in the expanded list
     * @throws Exception if there is an error fetching information from either the
     * SearchService or the StatsService
     */
    @Override
    public List<StatSnapshotApiDTO> getStatsByEntityQuery(String uuid, StatPeriodApiInputDTO inputDto)
            throws Exception {

        logger.debug("fetch stats for {} requestInfo: {}", uuid, inputDto);

        // choose LinkedList to make appending more efficient. This list will only be read once.
        final List<StatSnapshotApiDTO> stats = Lists.newLinkedList();
        // If the endDate is in the future, read from the projected stats
        final long clockTimeNow = clock.millis();

        // If uuid belongs to a cluster and the request is for getting VM headroom data,
        // get the stats from the cluster stats tables.
        // Currently, it is the only use case that reads stats data from the cluster table
        // and it is handled as a special case.  If more use cases need to get cluster level
        // statistics in the future, the conditions for calling getClusterStats will need to change.
        List<StatApiInputDTO> statsFilters = inputDto.getStatistics();
        List<String> inputQueryFilters = statsFilters == null ? Lists.newArrayList() :
                inputDto.getStatistics().stream().map(StatApiInputDTO::getName).collect(Collectors.toList());
        if (isClusterUuid(uuid) && inputQueryFilters.contains(HEADROOM_VMS)) {
            // uuid belongs to a cluster. Call Stats service to retrieve cluster related stats.
            ClusterStatsRequest clusterStatsRequest = StatsMapper.toClusterStatsRequest(uuid, inputDto);
            Iterator<StatSnapshot> statSnapshotIterator = statsServiceRpc.getClusterStats(clusterStatsRequest);
            while (statSnapshotIterator.hasNext()) {
                stats.add(StatsMapper.toStatSnapshotApiDTO(statSnapshotIterator.next()));
            }
        } else {
            // determine the list of entity OIDs to query for this operation
            final Set<Long> entityStatsOids = groupExpander.expandUuid(uuid);
            // if empty expansion and not "Market", must be an empty group; quick return
            if (entityStatsOids.isEmpty() && !UuidMapper.isRealtimeMarket(uuid)) {
                return Collections.emptyList();
            }

            if (inputDto.getEndDate() != null
                    && DateTimeUtil.parseTime(inputDto.getEndDate()) > clockTimeNow) {
                ProjectedStatsResponse response =
                        statsServiceRpc.getProjectedStats(StatsMapper.toProjectedStatsRequest(entityStatsOids,
                                inputDto));
                // create a StatSnapshotApiDTO from the ProjectedStatsResponse
                final StatSnapshotApiDTO projectedStatSnapshot = toStatSnapshotApiDTO(
                        response.getSnapshot());
                // set the time of the snapshot to "future" using the "endDate" of the request
                projectedStatSnapshot.setDate(DateTimeUtil.toString(Long.valueOf(inputDto.getEndDate())));
                // add to the list of stats to return
                stats.add(projectedStatSnapshot);
            }
            // if the startDate is in the past, read from the history (and combine with projected, if any)
            if (inputDto.getStartDate() == null || DateTimeUtil.parseTime(inputDto.getStartDate()) <
                    clockTimeNow) {

                final EntityStatsRequest request = StatsMapper.toEntityStatsRequest(entityStatsOids,
                        inputDto);
                final Iterable<StatSnapshot> statsIterator = () ->
                        statsServiceRpc.getAveragedEntityStats(request);

                // convert the stats snapshots to the desired ApiDTO and return them.
                stats.addAll(StreamSupport.stream(statsIterator.spliterator(), false)
                        .map(StatsMapper::toStatSnapshotApiDTO)
                        .collect(Collectors.toList()));
            }
        }

        List<TargetApiDTO> targets = null;
        try {
            targets = targetsService.getTargets(null);
        } catch (RuntimeException e) {
            logger.error("Unable to get targets list due to error: {}." +
                    " Not using targets list for stat filtering.", e.getMessage());
        }

        // filter out those commodities listed in BLACK_LISTED_STATS in StatsUtils
        return StatsUtils.filterStats(stats, targets);
    }

    /**
     * Check if the uuid belongs to a cluster.
     *
     * @param uuid UUID of an entity
     * @return true if it is a cluster, false otherwise
     */
    private boolean isClusterUuid(String uuid) {
        try {
            GetGroupResponse response = groupServiceRpc.getGroup(GroupID.newBuilder()
                    .setId(Long.parseLong(uuid))
                    .build());
            Type type = response.getGroup().getType();
            if (type.equals(Type.CLUSTER)) {
                return true;
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cluster uuid is invalid: " + uuid);
        }
        return false;
    }

    /**
     * Return stats for multiple entities by expanding the scopes field
     * of the {@link StatScopesApiInputDTO}.
     *
     * TODO: this conversion does not (yet) handle the "realtimeMarketReference" field of the
     * EntityStatsApiDTO.
     *
     * @param inputDto contains the query arguments; the 'scopes' property indicates a
     *                 list of items to query - might be Group, Cluster, or ServiceEntity.
     * @return a list of {@link EntityStatsApiDTO} objects representing the entities in the search
     * with the commodities values filled in
     */
    @Override
    public List<EntityStatsApiDTO> getStatsByUuidsQuery(StatScopesApiInputDTO inputDto)
            throws Exception {

        // check to see if this is a plan stats request
        Optional<List<EntityStatsApiDTO>> planUuidStats = getPlanUuidStats(inputDto);
        if (planUuidStats.isPresent()) {
            return planUuidStats.get();
        }

        final Set<Long> expandedUuids;
        final Map<Long, EntityStatsApiDTO> entityStatsMap = new HashMap<>();

        // check to see if this a full-market request (must be for a subset of entity types)
        Optional<List<ServiceEntityApiDTO>> fullMarketEntities = getFullMarketEntitiesSubset(inputDto);
        if (fullMarketEntities.isPresent()) {
            // subset of the full market entities that match a 'relatedType' returned
            fullMarketEntities.get().forEach(serviceEntity-> {
                final EntityStatsApiDTO entityStatsApiDTO = populateEntityStatsApiDTO(serviceEntity);
                entityStatsMap.put(Long.valueOf(serviceEntity.getUuid()), entityStatsApiDTO);
            });
            expandedUuids = entityStatsMap.keySet();
        } else {
            // Expand scopes list to determine the list of entity OIDs to query for this operation
            final Set<String> seedUuids = Sets.newHashSet(inputDto.getScopes());
            expandedUuids = groupExpander.expandUuids(
                    seedUuids);
            // if not a global scope, then expanded OIDs are expected
            if (UuidMapper.hasLimitedScope(seedUuids) && expandedUuids.isEmpty()) {
                // empty expanded list; return an empty stats list
                return Lists.newArrayList();
            }

            // create a map of OID -> empty EntityStatsApiDTO for the Service Entity OIDs given;
            // evaluate the Optional for each ServiceEntityApiDTO returned, and throw an exception if
            // the corresponding oid is not found
            for (Map.Entry<Long, Optional<ServiceEntityApiDTO>> entry :
                    repositoryApi.getServiceEntitiesById(
                            ServiceEntitiesRequest.newBuilder(expandedUuids).build()).entrySet()) {
                ServiceEntityApiDTO serviceEntity = entry.getValue().orElseThrow(()
                        -> new UnknownObjectException(
                        "ServiceEntity Not Found for oid: " + entry.getKey()));
                final EntityStatsApiDTO entityStatsApiDTO = populateEntityStatsApiDTO(serviceEntity);
                entityStatsMap.put(entry.getKey(), entityStatsApiDTO);
            }
        }

        // is the startDate in the past?
        final long clockTimeNow = clock.millis();
        if (inputDto.getPeriod().getStartDate() == null
                || DateTimeUtil.parseTime(inputDto.getPeriod().getStartDate()) < clockTimeNow) {
            // fetch the historical stats for the given entities using the given search spec
            Iterator<EntityStats> historicalStatsIterator = statsServiceRpc.getEntityStats(
                    StatsMapper.toEntityStatsRequest(entityStatsMap.keySet(), inputDto.getPeriod()));
            while (historicalStatsIterator.hasNext()) {
                EntityStats entityStats = historicalStatsIterator.next();
                final long entityOid = entityStats.getOid();
                if (!entityStatsMap.containsKey(entityOid)) {
                    throw new UnknownObjectException("Cannot find entity definition for:  "
                            + entityOid);
                }
                entityStatsMap.get(entityOid).getStats()
                        .addAll(toStatsSnapshotApiDtoList(entityStats));

            }
        }
        // is the endDate in the future?
        final String endDateParam = inputDto.getPeriod().getEndDate();
        if (endDateParam != null
                && DateTimeUtil.parseTime(endDateParam) > clockTimeNow) {
            // fetch the projected stats for each of the given entities
            Iterator<EntityStats> projectedStatsIterator = statsServiceRpc.getProjectedEntityStats(
                    StatsMapper.toProjectedStatsRequest(expandedUuids, inputDto.getPeriod()));

            while (projectedStatsIterator.hasNext()) {
                EntityStats projectedEntityStats = projectedStatsIterator.next();
                // if any snapshots were returned, then accumulate the stats index by entity Oid
                if (projectedEntityStats.getStatSnapshotsCount() > 0) {
                    // we expect either zero or one snapshot for each entity
                    if (projectedEntityStats.getStatSnapshotsCount() > 1) {
                        // this indicates a bug in History Component
                        logger.error("Too many entity stats ({}) for: {} -> {}; taking the first.",
                                projectedEntityStats.getStatSnapshotsCount(),
                                expandedUuids,
                                projectedEntityStats.getStatSnapshotsList());
                    }
                    long entityOid = projectedEntityStats.getOid();

                    // create a StatSnapshotApiDTO from the ProjectedStatsResponse
                    final StatSnapshotApiDTO projectedSnapshotDTO = toStatSnapshotApiDTO(
                            projectedEntityStats.getStatSnapshotsList().iterator().next());
                    // set the time of the snapshot to "future" using the "endDate" of the request
                    projectedSnapshotDTO.setDate(DateTimeUtil.toString(Long.valueOf(endDateParam)));
                    // add the projected stats for this entity to any historical stats fetched above
                    entityStatsMap.get(entityOid).getStats().add(projectedSnapshotDTO);
                }
            }
        }
        return Lists.newArrayList(entityStatsMap.values());
    }

    /**
     * Given a {@link ServiceEntityApiDTO} populate the entity-based fields
     * of an {@link EntityStatsApiDTO}.
     *
     * @param serviceEntity The {@link ServiceEntityApiDTO} from which to get the entity-based fields
     * @return a new {@link EntityStatsApiDTO} with the entity-based fields populated from the given
     * {@link ServiceEntityApiDTO}
     */
    private EntityStatsApiDTO populateEntityStatsApiDTO(ServiceEntityApiDTO serviceEntity) {
        final EntityStatsApiDTO entityStatsApiDTO = new EntityStatsApiDTO();
        entityStatsApiDTO.setUuid(serviceEntity.getUuid());
        entityStatsApiDTO.setClassName(serviceEntity.getClassName());
        entityStatsApiDTO.setDisplayName(serviceEntity.getDisplayName());
        entityStatsApiDTO.setStats(new ArrayList<>());
        return entityStatsApiDTO;
    }

    /**
     * If the uuid given in the 'scopes' value of the StatScopesApiInputDTO refers to a plan,
     * then return an Optional containing the list of EntityStatsApiDTO for the plan.
     *
     * If the 'inputDto.period.startDate is before "now", then the stats returned will include stats
     * for the Plan Source Topology. If the 'inputDto.period.startDate is after "now", then  the
     * stats returned will include stats for the Plan Projected Topology.
     *
     * Return Optional.empty() if this request is not for a plan.
     *
     * @param inputDto the specification of the stats query
     * @return if a plan scope, then return an optional containing a List of the EntityStatsApiDTO's
     * for the plan; if not a plan, return Optional/empty();
     */
    private Optional<List<EntityStatsApiDTO>> getPlanUuidStats(StatScopesApiInputDTO inputDto) {
        // plan stats request must be the only uuid in the scopes list
        if (inputDto.getScopes().size() != 1) {
            return Optional.empty();
        }
        // check for a plan uuid
        String scopeUuid = inputDto.getScopes().iterator().next();
        long scopeOid;
        try {
            scopeOid = Long.valueOf(scopeUuid);
        } catch (NumberFormatException e) {
            // not a number, so cannot be a plan UUID
            return Optional.empty();
        }

        // fetch plan from plan orchestrator
        PlanDTO.OptionalPlanInstance planInstanceOptional =
                planRpcService.getPlan(PlanDTO.PlanId.newBuilder()
                        .setPlanId(scopeOid)
                        .build());
        if (!planInstanceOptional.hasPlanInstance()) {
            return Optional.empty();
        }

        // fetch stats for a plan from plan orchestrator
        // TODO - OM-28024 - for now just return an empty list
        return Optional.of(Collections.emptyList());
    }

    /**
     * Fetch the subset of the full market where the entity matches the given 'relatedType'.
     * Uses the Search service of the Repository API.
     *
     * If this is not a full market search, return Optional.empty().
     *
     * If this is a full market search and 'relatedType' is not specified throw an
     * {@link IllegalArgumentException}.
     *
     * @param inputDto the specification of the search to perform, specifically the 'scopes' list and
     *                 the 'relatedType'
     * @return an Optional List of ServiceEntityApiDTO's if this is a full market search; otherwise
     * return Optional.empty()
     * @throws Exception if there is an error fetching results from Repository
     */
    private @Nonnull Optional<List<ServiceEntityApiDTO>> getFullMarketEntitiesSubset(
            @Nonnull StatScopesApiInputDTO inputDto) throws Exception {
        // Market stats request must be the only uuid in the scopes list
        if (inputDto.getScopes().size() != 1 ||
                !inputDto.getScopes().iterator().next().equals(UuidMapper.UI_REAL_TIME_MARKET_STR)) {
            return Optional.empty();
        }
        String relatedType = inputDto.getRelatedType();
        // 'relatedType' is required for full market entity stats
        if (StringUtils.isEmpty(relatedType)) {
            throw new IllegalArgumentException("Cannot request individual stats for full " +
                    "Market without specifying 'relatedType'");
        }
        // Fetch the SE's of this type
        List<ServiceEntityApiDTO> matchingServiceEntities = Lists.newArrayList(
                repositoryApi.getSearchResults(null, Collections.singletonList(relatedType),
                        UuidMapper.UI_REAL_TIME_MARKET_STR, null, null));

        return Optional.of(matchingServiceEntities);
    }
}
