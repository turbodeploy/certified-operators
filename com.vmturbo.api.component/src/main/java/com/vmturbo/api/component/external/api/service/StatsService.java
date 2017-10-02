package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.mapper.StatsMapper.toEntityStatsApiDTO;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.StatsMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.ServiceEntityApiDTO;
import com.vmturbo.api.dto.input.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.input.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IStatsService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.api.utils.EncodingUtil;
import com.vmturbo.api.utils.StatsUtils;
import com.vmturbo.api.utils.UrlsHelp;
import com.vmturbo.common.protobuf.group.ClusterServiceGrpc.ClusterServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClusterResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;

/**
 * Service implementation of Stats
 **/
public class StatsService implements IStatsService {

    private static Logger logger = LogManager.getLogger(StatsService.class);

    private final GroupServiceBlockingStub groupServiceRpc;

    private final ClusterServiceBlockingStub clusterServiceRpc;

    private final StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub statsServiceRpc;

    private final StatPeriodApiInputDTO DEFAULT_STAT_API_INPUT_DTO = new StatPeriodApiInputDTO();

    private final Clock clock;

    private final UuidMapper uuidMapper;

    private final RepositoryApi repositoryApi;

    StatsService(@Nonnull final StatsHistoryServiceBlockingStub statsServiceRpc,
                 @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub,
                 @Nonnull final ClusterServiceBlockingStub clusterServiceBlockingStub,
                 RepositoryApi repositoryApi, @Nonnull final UuidMapper uuidMapper,
                 @Nonnull final Clock clock) {
        this.statsServiceRpc = Objects.requireNonNull(statsServiceRpc);
        this.groupServiceRpc = Objects.requireNonNull(groupServiceBlockingStub);
        this.clusterServiceRpc = Objects.requireNonNull(clusterServiceBlockingStub);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.clock = Objects.requireNonNull(clock);
        this.uuidMapper = uuidMapper;
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

        final ApiId apiId = uuidMapper.fromUuid(uuid);
        Optional<List<Long>> expandedGroupOids = Optional.empty();
        boolean isGroup = false;
        boolean isCluster = false;
        if (!apiId.isRealtimeMarket()) {
            // If the input is a group or cluster, get its members and issue a stats request for those.
            try {
                final GetMembersResponse membersResp = groupServiceRpc.getMembers(
                    GetMembersRequest.newBuilder()
                        .setId(apiId.oid())
                        .build());
                isGroup = true;
                expandedGroupOids = Optional.of(membersResp.getMemberIdList());
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                    final GetClusterResponse resp = clusterServiceRpc.getCluster(
                        GetClusterRequest.newBuilder()
                            .setClusterId(apiId.oid())
                            .build());
                    if (resp.hasCluster()) {
                        expandedGroupOids = Optional.of(resp.getCluster().getInfo()
                                .getMembers().getStaticMemberOidsList());
                        isCluster = true;
                    }
                } else {
                    throw e;
                }
            }
        }

        // determine the list of entity OIDs to query for this operation
        final List<Long> entityStatsOids;
        if (isGroup || isCluster) {
            // Since groups don't have special stats, a query for group stats is a query
            // for the members of the group.
            entityStatsOids = expandedGroupOids.orElse(Collections.emptyList());
        } else {
            entityStatsOids = Collections.singletonList(apiId.oid());
        }

        final List<StatSnapshotApiDTO> stats = Lists.newArrayList();
        // If the user asks for stats with an end date in the future, assume they want
        // stats ONLY from the projected topology.
        // TODO (roman, June 26, 2017) OM-20628: Verify what the right behaviour is w.r.t. splitting
        // incoming time ranges into queries for projected stats vs. "real" historical stats.
        final long clockTimeNow = clock.millis();
        if (inputDto.getEndDate() != null && DateTimeUtil.parseTime(inputDto.getEndDate()) > clockTimeNow) {
            ProjectedStatsResponse response =
                statsServiceRpc.getProjectedStats(StatsMapper.toProjectedStatsRequest(
                            expandedGroupOids.orElseGet(() -> {
                                if (apiId.isRealtimeMarket()) {
                                    return Collections.emptyList();
                                } else {
                                    return Collections.singletonList(apiId.oid());
                                }
                            }), inputDto));
            // create a StatSnapshotApiDTO from the ProjectedStatsResponse
            final StatSnapshotApiDTO projectedStatSnapshot = StatsMapper.toStatSnapshotApiDTO(
                    response.getSnapshot());
            // set the time of the snapshot to "future" using the "endDate" of the request
            projectedStatSnapshot.setDate(DateTimeUtil.toString(Long.valueOf(inputDto.getEndDate())));
            // add to the list of stats to return
            stats.add(projectedStatSnapshot);
        }
        if (inputDto.getStartDate() == null || DateTimeUtil.parseTime(inputDto.getStartDate()) <
                clockTimeNow){
            // Clusters have special stats associated with them, and therefore have their own
            // RPC call. We add those (if any) to the soup
            // TODO: OM-21711: Mark

            final EntityStatsRequest request = StatsMapper.toEntityStatsRequest(entityStatsOids,
                    inputDto);
            final Iterable<Stats.StatSnapshot> statsIterator = () -> statsServiceRpc.getAveragedEntityStats(request);

            // convert the stats snapshots to the desired ApiDTO and return them.
            stats.addAll(StreamSupport.stream(statsIterator.spliterator(), false)
                    .map(StatsMapper::toStatSnapshotApiDTO)
                    .collect(Collectors.toList()));
        }
        // filter out those commodities listed in BLACK_LISTED_STATS in StatsUtils
        // TODO: Pass in list of targets here.
        return StatsUtils.filterStats(stats, null);
    }


    /**
     * Return stats for multiple entities by expanding the scopes field
     * of the {@link StatScopesApiInputDTO}.
     *
     * @param inputDto contains the query arguments; the 'scopes' property indicates a
     *                 list of items to query - might be Group, Cluster, or ServiceEntity.
     * @return a list of {@link EntityStatsApiDTO} objects representing the entities in the search
     * with the commodities values filled in
     */
    @Override
    public List<EntityStatsApiDTO> getStatsByUuidsQuery(StatScopesApiInputDTO inputDto)
            throws Exception {

        // TODO: handle scope IDs for groups and clusters, not just SE's = in OM-22990
        Set<Long> oidSet = inputDto.getScopes().stream()
                .map(Long::valueOf)
                .collect(Collectors.toSet());

        // fetch the full definitions for the Service Entity OIDs given; evaluate the Optional
        //        for each ServiceEntityApiDTO returned, and throw an exception if oid is not found
        Map<Long, ServiceEntityApiDTO> entityInfoMap = new HashMap<>();
        for (Map.Entry<Long, Optional<ServiceEntityApiDTO>> entry :
                repositoryApi.getServiceEntitiesById(oidSet, false).entrySet()) {
            entityInfoMap.put(entry.getKey(), entry.getValue()
                            .orElseThrow(() -> new UnknownObjectException(
                                    "ServiceEntity Not Found for oid: " + entry.getKey())));
        }

        // fetch the stats for the given entities using the given search spec
        Iterable<EntityStats> iterable = () -> statsServiceRpc.getEntityStats(
                StatsMapper.toEntityStatsRequest(Lists.newArrayList(oidSet),
                inputDto.getPeriod()));

        return StreamSupport.stream(iterable.spliterator(), false)
                .map(entityStats -> {
                    EntityStatsApiDTO entityStatsApiDTO = toEntityStatsApiDTO(entityStats);
                    ServiceEntityApiDTO seInfo = entityInfoMap.get(entityStats.getOid());
                    entityStatsApiDTO.setDisplayName(seInfo.getDisplayName());
                    entityStatsApiDTO.setClassName(seInfo.getClassName());
                    return entityStatsApiDTO;
                })
                .collect(Collectors.toList());
    }
}
