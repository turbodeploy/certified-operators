package com.vmturbo.api.component.external.api.service;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.springframework.util.CollectionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.dto.supplychain.SupplychainEntryDTO;
import com.vmturbo.api.dto.supplychain.SupplyChainStatsApiInputDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.enums.EntitiesCountCriteria;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.SupplyChainDetailType;
import com.vmturbo.api.serviceinterfaces.ISupplyChainsService;
import com.vmturbo.api.utils.DateTimeUtil;

public class SupplyChainsService implements ISupplyChainsService {

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;
    private final long liveTopologyContextId;
    private final GroupExpander groupExpander;

    // criteria in this list require fetching the health summary along with the supplychain
    private static final Collection<EntitiesCountCriteria> SUPPLY_CHAIN_HEALTH_REQUIRED =
            ImmutableList.of(
                    EntitiesCountCriteria.severity
            );

    SupplyChainsService(@Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                        final long liveTopologyContextId, GroupExpander groupExpander) {
        this.liveTopologyContextId = liveTopologyContextId;
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.groupExpander = groupExpander;
    }

    @Override
    public SupplychainApiDTO getSupplyChainByUuids(List<String> uuids,
                                                   List<String> entityTypes,
                                                   EnvironmentType environmentType,
                                                   SupplyChainDetailType supplyChainDetailType,
                                                   Boolean includeHealthSummary) throws Exception {
        if (uuids.isEmpty()) {
            throw new RuntimeException("UUIDs list is empty");
        }

        // request the supply chain for the items, including expanding groups and clusters
        return supplyChainFetcherFactory.newApiDtoFetcher()
                .topologyContextId(liveTopologyContextId)
                .addSeedUuids(uuids)
                .entityTypes(entityTypes)
                .environmentType(environmentType)
                .supplyChainDetailType(supplyChainDetailType)
                .includeHealthSummary(includeHealthSummary)
                .fetch();
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
        // TODO: implement "states" filter - OM-25089

        final EnvironmentType environmentType = supplyChainStatsApiInputDTO.getEnvironmentType();

        if (CollectionUtils.isEmpty(uuids)) {
            // full topology - not implemented
            throw ApiUtils.notImplementedInXL();
        }
        // grab the 'groupBy' criteria list, if any
        final List<EntitiesCountCriteria> criteriaToGroupBy = supplyChainStatsApiInputDTO.getGroupBy();

        // fetch the supplychain for the list of seeds; includes group and cluster expansion
        final SupplychainApiDTOFetcherBuilder supplyChainFetcher = this.supplyChainFetcherFactory
                .newApiDtoFetcher()
                .topologyContextId(liveTopologyContextId)
                .addSeedUuids(uuids)
                .entityTypes(supplyChainStatsApiInputDTO.getTypes())
                .supplyChainDetailType(SupplyChainDetailType.entity)
                .includeHealthSummary(isHealthSummaryNeeded(criteriaToGroupBy));
        if (types != null) {
            supplyChainFetcher.entityTypes(types);
        }
        if (environmentType != null) {
            supplyChainFetcher.environmentType(environmentType);
        }
        final SupplychainApiDTO supplyChainResponse = supplyChainFetcher.fetch();

        // count Service Entities with each unique set of filter/value for all the filters
        Map<FilterSet, Long> entityCountMap = Maps.newHashMap();
        if (!criteriaToGroupBy.isEmpty()) {
            supplyChainResponse.getSeMap().forEach((entityType, supplychainDTO) -> {
                // get the filter sets for all entities of this type
                List<FilterSet> filtersForEntities = calculateFilters(supplychainDTO,
                        criteriaToGroupBy);
                // tabulate the filter set for each entity type
                filtersForEntities.forEach(filterSet ->
                        // increment the count of Filters that match this one
                        entityCountMap.put(filterSet,
                                entityCountMap.getOrDefault(filterSet, 0L) + 1));
            });
        }

        // analyze the counts
        List<StatApiDTO> stats = Lists.newArrayList();
        entityCountMap.forEach((filterSet, count) -> {
            StatApiDTO stat = new StatApiDTO();
            filterSet.forEach(filter -> stat.addFilter(filter.getType(),
                    filter.getValue()));
            stat.setName("entities");
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
     * @return a list of criterion sets for this entity type, with one criterion set element for
     * each entity in the type
     */
    private List<FilterSet> calculateFilters(@Nonnull SupplychainEntryDTO supplychainEntryDTO,
                                             @Nonnull List<EntitiesCountCriteria> criteriaToGroupBy) {

        List<FilterSet> filterSetsForAllEntities = Lists.newArrayList();

        supplychainEntryDTO.getInstances().values().forEach(entityApiDTO -> {
            FilterSet filtersForEntity = new FilterSet();
            for (EntitiesCountCriteria filter : criteriaToGroupBy) {
                StatFilterApiDTO resultFilter;
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
                    case template:
                        throw ApiUtils.notImplementedInXL();
                    default:
                        throw new RuntimeException("Unexpected filter criterion: " + filter);
                }
                filtersForEntity.addFilter(resultFilter);
            }
            filterSetsForAllEntities.add(filtersForEntity);
        });

        return filterSetsForAllEntities;
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
