package com.vmturbo.api.component.external.api.service;


import static com.vmturbo.components.common.utils.StringConstants.CLUSTER;
import static com.vmturbo.components.common.utils.StringConstants.GROUP;
import static com.vmturbo.components.common.utils.StringConstants.STORAGE_CLUSTER;
import static com.vmturbo.components.common.utils.StringConstants.VIRTUAL_MACHINE_CLUSTER;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.plan.orchestrator.api.PlanUtils;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.supplychain.SupplyChainStatsApiInputDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.enums.EntitiesCountCriteria;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.ISupplyChainsService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.FilteredActionResponse;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.components.common.utils.StringConstants;

public class SupplyChainsService implements ISupplyChainsService {
    private static final Logger logger = LogManager.getLogger();

    private static final Set<String> GROUP_TYPES = Sets.newHashSet(GROUP, CLUSTER, STORAGE_CLUSTER,
            VIRTUAL_MACHINE_CLUSTER);

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;
    private final long realtimeTopologyContextId;
    private final GroupExpander groupExpander;
    private final PlanServiceBlockingStub planRpcService;
    private final UserSessionContext userSessionContext;
    private final ActionSpecMapper actionSpecMapper;
    private final ActionsServiceBlockingStub actionOrchestratorRpc;

    // criteria in this list require fetching the health summary along with the supplychain
    private static final Collection<EntitiesCountCriteria> SUPPLY_CHAIN_HEALTH_REQUIRED =
            ImmutableList.of(
                    EntitiesCountCriteria.severity
            );

    SupplyChainsService(@Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                        @Nonnull final PlanServiceBlockingStub planRpcService,
                        @Nonnull final ActionSpecMapper actionSpecMapper,
                        @Nonnull final ActionsServiceBlockingStub actionOrchestratorRpcService,
                        final long realtimeTopologyContextId,
                        @Nonnull final GroupExpander groupExpander,
                        @Nonnull final UserSessionContext userSessionContext) {
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.planRpcService = planRpcService;
        this.actionSpecMapper = Objects.requireNonNull(actionSpecMapper);
        this.actionOrchestratorRpc = Objects.requireNonNull(actionOrchestratorRpcService);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
    }

    @Override
    public SupplychainApiDTO getSupplyChainByUuids(List<String> uuids,
                                                   List<String> entityTypes,
                                                   List<EntityState> entityStates,
                                                   EnvironmentType environmentType,
                                                   EntityDetailType entityDetailType,
                                                   Boolean includeHealthSummary) throws Exception {
        if (uuids.isEmpty()) {
            throw new RuntimeException("UUIDs list is empty");
        }

        // request the supply chain for the items, including expanding groups and clusters
        final SupplychainApiDTOFetcherBuilder fetcherBuilder =
            supplyChainFetcherFactory.newApiDtoFetcher()
                .entityTypes(entityTypes)
                .apiEnvironmentType(environmentType)
                .entityDetailType(entityDetailType)
                .includeHealthSummary(includeHealthSummary);

        //if the request is for a plan supply chain, the "seed uuid" should instead be used as the topology context ID.
        Optional<PlanInstance> possiblePlan = getPlanIfRequestIsPlan(uuids);
        if (possiblePlan.isPresent()) {
            PlanInstance plan = possiblePlan.get();
            // if we have a user id in the context, we may prevent access to the plan supply chain
            // if the user is either not an admin user or does not own the plan
            if (!PlanUtils.canCurrentUserAccessPlan(plan)) {
                throw new UserAccessException("User does not have access to plan.");
            } else {
                // turn off the access scope filter on the supply chain -- a user can see all entities
                // in a plan they have access to.
                fetcherBuilder.enforceUserScope(false);
            }

            fetcherBuilder.topologyContextId(Long.valueOf(uuids.iterator().next()));
            if (isPlanScoped(plan)) {
                Set<Long> planSeedOids = getSeedIdsForPlan(possiblePlan.get());
                for (Long oid : planSeedOids) {
                    fetcherBuilder.addSeedUuid(String.valueOf(oid));
                }
                if (planSeedOids.size() == 0) {
                    logger.warn("Scoped plan {} did not have any entities in scope.", plan.getPlanId());
                }
            }
        } else {
            fetcherBuilder.topologyContextId(realtimeTopologyContextId).addSeedUuids(uuids);
        }

        return fetcherBuilder.fetch();
    }

    /**
     * check if a PlanInstance is scoped. It's scoped if the plan instance contains a scenario with
     * a scope definition inside.
     * @param planInstance the PlanInstance to check
     * @return true if the plan is scoped, false otherwise
     */
    private boolean isPlanScoped(PlanInstance planInstance) {
        // does this plan have a scope?
        return (planInstance.hasScenario()
                && planInstance.getScenario().hasScenarioInfo()
                && planInstance.getScenario().getScenarioInfo().hasScope());
        }

    /**
     * Given a plan instance, extract the set of seed entities based on the plan scope. If a plan
     * is not scoped, this will be an empty set.
     *
     * @param planInstance the PlanInstance to check the scope of.
     * @return The set of unique seed entities based on the plan scope. Will be empty for an
     * unscoped plan.
     */
    private Set<Long> getSeedIdsForPlan(PlanInstance planInstance) {
        // does this plan have a scope?
        if (!isPlanScoped(planInstance)) {
            // nope, no scope
            return Collections.emptySet();
        }
        Set<Long> seedEntities = new HashSet(); // seed entities to return
        PlanScope scope = planInstance.getScenario().getScenarioInfo().getScope();
        for (PlanScopeEntry scopeEntry : scope.getScopeEntriesList()) {
            // if it's an entity, add it right to the seed set. Otherwise queue it for
            // group resolution.
            if (GROUP_TYPES.contains(scopeEntry.getClassName())) {
                // needs expansion
                groupExpander.expandUuid(String.valueOf(scopeEntry.getScopeObjectOid()))
                        .forEach(seedEntities::add);
            } else {
                // this is an entity -- add it right to the seedEntities
                seedEntities.add(scopeEntry.getScopeObjectOid());
            }
        }

        return seedEntities;
    }

    /**
     * Attempt to retrieve a PlanInstance if the supply chain request refers specifically to a plan.
     *
     * ASSUMPTIONS: A supply chain request refers to a plan if:
     *      - the seed UUID list has exactly one element
     *      - the uuid is a string of numerals
     *      - there exists a plan with that uuid
     *
     * These assumptions may change if the UI/API does.
     *
     * @param uuids the supplied seed UUID list
     * @return an Optional PlanInstance, which will be provided if the uuids did, in fact, represent
     * a plan request. It will be empty if this is not identified as a plan request.
     */
    private Optional<PlanInstance> getPlanIfRequestIsPlan(List<String> uuids) {
        if (uuids.size() != 1) {
            return Optional.empty();
        }
        final long prospectivePlanId;
        try {
            prospectivePlanId = Long.valueOf(uuids.iterator().next());
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
        final OptionalPlanInstance possiblePlan =
            planRpcService.getPlan(PlanId.newBuilder().setPlanId(prospectivePlanId).build());
        return possiblePlan.hasPlanInstance() ? Optional.of(possiblePlan.getPlanInstance()) : Optional.empty();
    }

    /**
     * Return the stats for a supplychain; expand the supplychain to SE's and use those for
     * the Stats query; if requested by the 'groupBy' field, aggregate counts for a
     * list of criteria.
     *
     * @param supplyChainStatsApiInputDTO a description of the supplychain seed uuids
     *                                    and the stats query to execute over that supplychain
     * @return the list of stats request for each snapshot time in the result
     * @throws Exception if the supplychain fetch() operation fails
     */
    @Override
    public List<StatSnapshotApiDTO> getSupplyChainStats(
            final SupplyChainStatsApiInputDTO supplyChainStatsApiInputDTO) throws Exception {

        final List<String> uuids = supplyChainStatsApiInputDTO.getUuids();
        final List<String> types = supplyChainStatsApiInputDTO.getTypes() != null ?
                supplyChainStatsApiInputDTO.getTypes() : Lists.newArrayList();
        final List<EntityState> states = supplyChainStatsApiInputDTO.getStates();

        final EnvironmentType environmentType = supplyChainStatsApiInputDTO.getEnvironmentType();

        if (CollectionUtils.isEmpty(uuids)) {
            // full topology - not implemented
            throw ApiUtils.notImplementedInXL();
        }
        // grab the 'groupBy' criteria list, if any
        final List<EntitiesCountCriteria> criteriaToGroupBy =
                ListUtils.emptyIfNull(supplyChainStatsApiInputDTO.getGroupBy());
        // fetch the supplychain for the list of seeds; includes group and cluster expansion
        // and if criteria only has severity, it doesn't need to query severity stats for each entity,
        // it just need to make one query to get the total severity count stats.
        final boolean onlyGroupBySeverity = (criteriaToGroupBy.size() == 1 &&
                criteriaToGroupBy.get(0) == EntitiesCountCriteria.severity);
        final SupplychainApiDTOFetcherBuilder supplyChainFetcher = this.supplyChainFetcherFactory
                .newApiDtoFetcher()
                .topologyContextId(realtimeTopologyContextId)
                .addSeedUuids(uuids)
                .entityDetailType(onlyGroupBySeverity ? null : EntityDetailType.entity)
                .includeHealthSummary(isHealthSummaryNeeded(criteriaToGroupBy));

        if (types != null) {
            supplyChainFetcher.entityTypes(types);
        }
        if (environmentType != null) {
            supplyChainFetcher.apiEnvironmentType(environmentType);
        }
        final SupplychainApiDTO supplyChainResponse = supplyChainFetcher.fetch();
        applyStatesFilter(supplyChainResponse, states);
        // count Service Entities with each unique set of filter/value for all the filters
        // analyze the counts
        final List<StatApiDTO> stats = Lists.newArrayList();
        final Map<FilterSet, Long> entityCountMap = Maps.newHashMap();
        if (onlyGroupBySeverity) {
            supplyChainResponse.getSeMap().values().stream()
                    .flatMap(supplyChainDTO -> supplyChainDTO.getHealthSummary().entrySet().stream())
                    .forEach(severityEntrySet ->
                            generateFilterSetForSeverity(severityEntrySet, entityCountMap));
        } else if (!criteriaToGroupBy.isEmpty()) {
            // Getting supply chain actions only if groupBy contains riskSubCategory
            final List<ActionApiDTO> supplyChainActions =
                (criteriaToGroupBy.contains(EntitiesCountCriteria.riskSubCategory))
                    ? getSupplyChainActions(supplyChainResponse) : null;
            supplyChainResponse.getSeMap().forEach((entityType, supplychainDTO) -> {
                // get the filter sets for all entities of this type
                List<FilterSet> filtersForEntities = calculateFilters(supplychainDTO,
                    criteriaToGroupBy,supplyChainActions);
                // tabulate the filter set for each entity type
                filtersForEntities.forEach(filterSet ->
                    // increment the count of Filters that match this one
                    entityCountMap.put(filterSet,
                        entityCountMap.getOrDefault(filterSet, 0L) + 1));
            });


        } else {
            // If we're not grouping by anything, just add a single stat for
            // all the entities in the supply chain query.
            StatApiDTO stat = new StatApiDTO();
            stat.setName(StringConstants.ENTITIES);
            stat.setValue((float)supplyChainResponse.getSeMap().values().stream()
                .mapToInt(SupplychainEntryDTO::getEntitiesCount)
                .sum());
            stats.add(stat);
        }

        entityCountMap.forEach((filterSet, count) -> {
            StatApiDTO stat = new StatApiDTO();
            filterSet.forEach(filter -> stat.addFilter(filter.getType(),
                    filter.getValue()));
            stat.setName(StringConstants.ENTITIES);
            stat.setValue((float)count);
            stats.add(stat);
        });

        // create the return value
        StatSnapshotApiDTO snapshot = new StatSnapshotApiDTO();
        snapshot.setDate(DateTimeUtil.getNow());
        snapshot.setStatistics(stats);

        // the answer is always a single-element list
        return Lists.newArrayList(snapshot);
    }

    /**
     * Filters the given Supply Chain entities with the required states. Entities with other states
     * will be discarded.
     *
     * @param supplychainApiDTO {@link SupplychainApiDTO} object
     * @param states a list of {@link EntityState}
     */
    private static void applyStatesFilter(@Nonnull SupplychainApiDTO supplychainApiDTO,
                                          @Nullable List<EntityState> states){
        if(states != null && !states.isEmpty()) {
            // Since I'm modifying the supplychainApiDTO, I need to use Iterator otherwise
            // ConcurrentModificationException will be thrown.
            for (Iterator<Entry<String, SupplychainEntryDTO>>
                 sc = supplychainApiDTO.getSeMap().entrySet().iterator(); sc.hasNext();) {
                Entry<String, SupplychainEntryDTO> scEntry = sc.next();
                for (Iterator<Entry<String, ServiceEntityApiDTO>>
                     entity = scEntry.getValue().getInstances().entrySet().iterator();
                     entity.hasNext();) {
                    Entry<String, ServiceEntityApiDTO> entityDTO = entity.next();
                    try {
                        if (!states.contains(EntityState.valueOf(entityDTO.getValue().getState()))) {
                            entity.remove();
                        }
                    } catch (IllegalArgumentException e) {
                        // The entity's state is not one of EntityState enums.
                        entity.remove();
                    }
                }
            }
        }
    }

    /**
     * Return a list of {@link FilterSet}s, one for each Service Entity in the given
     * {@link SupplychainEntryDTO}.
     * Each FilterSet contains an {@link StatFilterApiDTO} for each element in the
     * 'criteriaToGroupBy' input, where the StatFilterApiDTO value is derived from the corresponding
     * Service Entity.
     * <p>
     * For example, if 'criteriaToGroupBy' is [entityType, severity], then the answer will be
     * a list of CriterionSets, one for each {@link ServiceEntityApiDTO} in the input
     * {@link SupplychainEntryDTO}. Each FilterSet will have two {@link StatFilterApiDTO}
     * elements (called 'filters' here):  {entityType: se.getType()}, {severity: se.getSeverity()}.
     *
     * @param supplychainEntryDTO the information about entities of this type
     * @param criteriaToGroupBy what {@link EntitiesCountCriteria}(s) to group by,
     *                          e.g. 'entityType,severity'
     * @param supplyChainActions a List of {@link ActionApiDTO} representing the supply chain
     *                           actions. This param will be set only in case 'criteriaToGroupBy'
     *                           contains 'riskSubCategory'
     * @return a list of criterion sets for this entity type, with one criterion set element for
     * each entity in the type
     */
    private List<FilterSet> calculateFilters(@Nonnull SupplychainEntryDTO supplychainEntryDTO,
                                             @Nonnull List<EntitiesCountCriteria> criteriaToGroupBy,
                                             List<ActionApiDTO> supplyChainActions){

        List<FilterSet> filterSetsForAllEntities = Lists.newArrayList();

        supplychainEntryDTO.getInstances().values().forEach(entityApiDTO -> {
            FilterSet filtersForEntity = new FilterSet();
            for (EntitiesCountCriteria filter : criteriaToGroupBy) {
                StatFilterApiDTO resultFilter = null;
                switch (filter) {
                    case entityType:
                        resultFilter = buildStatFilter(filter.name(),
                            entityApiDTO.getClassName());
                        break;
                    case state:
                        resultFilter = buildStatFilter(filter.name(),
                            entityApiDTO.getState());
                        break;
                    case severity:
                        resultFilter = buildStatFilter(filter.name(),
                            entityApiDTO.getSeverity());
                        break;
                    case riskSubCategory:
                        List<ActionApiDTO> filteredActions =  supplyChainActions.stream()
                            .filter(actionApiDTO ->
                                entityApiDTO.getUuid().equals(actionApiDTO.getTarget().getUuid()))
                            .collect(Collectors.toList());
                        for (ActionApiDTO action : filteredActions) {
                            resultFilter = buildStatFilter(filter.name(),
                                action.getRisk().getSubCategory());
                            filtersForEntity.addFilter(resultFilter);
                        }
                        break;
                    case template:
                        throw ApiUtils.notImplementedInXL();
                    default:
                        throw new RuntimeException("Unexpected filter criterion: " + filter);
                }
                // Skipping riskSubCategory since adding to filtersForEntity is already done in its
                // switch case.
                if(!EntitiesCountCriteria.riskSubCategory.equals(filter)){
                    filtersForEntity.addFilter(resultFilter);
                }

            }
            // filtersForEntity can be null in case groupBy riskSubCategory was required and we
            // didn't find actions related to the requested entities.
            if(filtersForEntity != null) {
                filterSetsForAllEntities.add(filtersForEntity);
            }

        });

        return filterSetsForAllEntities;
    }

    /**
     * Create {@link FilterSet} based on severity total count summary. And add it into entity count
     * map.
     *
     * @param severityEntrySet a Map entry which key is severity type and value is count.
     * @param entityCountMap a Map which key is {@link FilterSet}, value is entity count.
     */
    private void generateFilterSetForSeverity(
            @Nonnull Map.Entry<String, Integer> severityEntrySet,
            @Nonnull Map<FilterSet, Long> entityCountMap) {
        // need to convert severity type to uppercase, otherwise UI will not parse it.
        final String severityType = severityEntrySet.getKey().toUpperCase();
        final int severityCount = severityEntrySet.getValue();
        final FilterSet filterSet = new FilterSet();
        filterSet.addFilter(buildStatFilter(EntitiesCountCriteria.severity.name(), severityType));
        entityCountMap.put(filterSet,
                entityCountMap.getOrDefault(filterSet, 0L) + severityCount);
    }

    /**
     * Determine whether or not the health summaries are needed by the
     * supplychain stats request. Not all "groupBy" criteria require the entity itself.
     *
     * Currently the only EntitiesCountCriteria that requires health summaries is 'severity'.
     *
     * @param criteriaToGroupBy what are the criteria in the request to be "group-ed by"
     * @return true iff any of the criteria in the given list require the health summaries
     */
    private boolean isHealthSummaryNeeded(@Nonnull List<EntitiesCountCriteria> criteriaToGroupBy) {
        return CollectionUtils.containsAny(criteriaToGroupBy, SUPPLY_CHAIN_HEALTH_REQUIRED);
    }


    private StatFilterApiDTO buildStatFilter(@Nonnull String filterType,
                                             @Nonnull String filterValue) {
        StatFilterApiDTO statFilter = new StatFilterApiDTO();
        statFilter.setType(filterType);
        statFilter.setValue(filterValue);
        return statFilter;
    }

    /**
     * Gets the entities that are members of the given Supply Chain.
     *
     * @param supplychainApiDTO {@link SupplychainApiDTO} object
     * @return A set of entities uuids
     */
    private Set<Long> getSupplyChainEntitiesUuids(@Nonnull SupplychainApiDTO supplychainApiDTO){
        Set<Long> uuidSet = new HashSet<>();
        supplychainApiDTO.getSeMap().forEach((entityType, supplychainEntryDTO) -> {
                supplychainEntryDTO.getInstances().values().forEach(entityApiDTO -> {
                    // Getting actions for ACTIVE entities only. This is to comply with our
                    // requirements
                    if(EntityState.ACTIVE.name().equals(entityApiDTO.getState())) {
                        uuidSet.add(Long.parseLong(entityApiDTO.getUuid()));
                    }
                });
        });
        return uuidSet;
    }

    /**
     * Gets the entities actions that are members of the given Supply Chain.
     *
     * @param supplychainApiDTO {@link SupplychainApiDTO} object
     * @return A list of {@link ActionApiDTO} with the actions details
     * @throws UnknownObjectException,UnsupportedActionException,ExecutionException,InterruptedException
     * When the actions involve an unrecognized entity uuid
     */
    private List<ActionApiDTO> getSupplyChainActions(@Nonnull SupplychainApiDTO supplychainApiDTO)
        throws UnknownObjectException, UnsupportedActionException, ExecutionException,
        InterruptedException{

        // Getting the supply chain members uuids to get the actions related to them
        Set<Long> supplyChainEntitiesUuids = getSupplyChainEntitiesUuids(supplychainApiDTO);
        // Passing null as inputDto to get all actions for the given entities.
        final ActionQueryFilter filter = actionSpecMapper.createActionFilter(
            null, Optional.of(supplyChainEntitiesUuids));
        final FilteredActionResponse response = actionOrchestratorRpc.getAllActions(
            FilteredActionRequest.newBuilder()
                .setTopologyContextId(realtimeTopologyContextId)
                .setFilter(filter)
                .build());
        final List<ActionApiDTO> actionsList = actionSpecMapper.mapActionSpecsToActionApiDTOs(
            response.getActionsList().stream()
                .map(ActionOrchestratorAction::getActionSpec)
                .collect(Collectors.toList()), realtimeTopologyContextId);
        return actionsList;
    }

    /**
     * This class is used to define a key to be used in the "groupBy" operation. The key
     * is a set of {@link StatFilterApiDTO}, and since StatFilterApiDTO does not define
     * "equals()" we cannot rely on Set::equals and must redefine that here.
     */
    @VisibleForTesting
    static class FilterSet {

        Set<StatFilterApiDTO> filters = Sets.newHashSet();

        /**
         * Add a {@link StatFilterApiDTO} to the filters in this set.
         *
         * @param newFilter a new {@link StatFilterApiDTO} to add
         */
        public void addFilter(StatFilterApiDTO newFilter) {
            filters.add(newFilter);
        }

        /**
         * Apply the given action to each {@link StatFilterApiDTO}.
         *
         * @param action the {@link Consumer} to apply
         */
        public void forEach(Consumer<StatFilterApiDTO> action) {
            filters.forEach(action);
        }

        /**
         * Equality for a FilterSet is defined as a match over the type and value of each
         * StatApiDTO included. This is required since {@link StatFilterApiDTO} does not
         * implement "equals()".
         *
         * @param o the other FilterSet to compare against
         * @return true iff the two sets of StatFilterApiDTO are equal when comparing type and value
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FilterSet that = (FilterSet) o;

            // make sure the sizes match and that all items in this set are in 'that' set
            return filters.size() == that.filters.size() && filters.stream()
                    .allMatch(filters -> that.filters
                            .stream()
                            .anyMatch(thatFilter ->
                                    filters.getType().equals(thatFilter.getType()) &&
                                            filters.getValue().equals(thatFilter.getValue())));

        }

        /**
         * Hashcode is the sum of the hashcodes of the types & values of the elements.
         * We need to compute this here since StatFilterApiDTO doesn't implement 'equals()' in
         * a smart way.
         *
         * @return a hash calculated by summing the hash of each type and value
         */
        @Override
        public int hashCode() {
            return filters.stream()
                    .map(filter -> filter.getType().hashCode() + filter.getValue().hashCode())
                    .reduce(0, (total, addend) -> total + addend);
        }
    }
}
