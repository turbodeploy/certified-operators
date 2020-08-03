package com.vmturbo.reserved.instance.coverage.allocator.filter;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.utils.ReservedInstanceHelper;

/**
 * Provides an initial filter of a {@link CoverageTopology}, based on topology filters and current
 * coverage of each {@link ReservedInstanceBought} and {@link TopologyEntityDTO} instance. This filter
 * is applicable to all CSP types since it's addressing only provider-agnostic topology filters and
 * current RI coverage/utilization. Subsequent filters may be provider-specific (e.g. filtering non-platform
 * flexible RIs in Azure since this is invalid for that CSP) and are generally applied as part of
 * {@link com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext} creation.
 */
public class FirstPassCoverageFilter {

    /**
     * A set of {@link EntityType} values that are coverable by an instance of {@link ReservedInstanceBought}.
     * Currently, only {@link EntityType#VIRTUAL_MACHINE_VALUE} is supported.
     */
    public static final Set<Integer> SUPPORTED_COVERABLE_ENTITY_TYPES = ImmutableSet.of(
            EntityType.VIRTUAL_MACHINE_VALUE);

    @Nonnull
    private final CoverageTopology coverageTopology;
    @Nonnull
    private final Optional<AccountFilter> accountFilter;
    @Nonnull
    private final Optional<EntityFilter> entityFilter;
    @Nonnull
    private final ReservedInstanceCoverageJournal coverageJournal;

    /**
     * Construct a new instance of {@link FirstPassCoverageFilter}
     * @param coverageTopology The coverage topology to filter
     * @param accountFilter An account filter to apply to entities within the cloud topology. If null
     *                      is passed, no account filter will be applied
     * @param entityFilter An entity filter to apply to entities within the cloud topology. If null is
     *                     passed, no entity filter will be applied
     * @param coverageJournal The coverage journal, containing current coverages of the entities and RIs
     *                        contained within {@code coverageTopology}
     */
    public FirstPassCoverageFilter(@Nonnull CoverageTopology coverageTopology,
                                   @Nullable AccountFilter accountFilter,
                                   @Nullable EntityFilter entityFilter,
                                   @Nonnull ReservedInstanceCoverageJournal coverageJournal) {

        this.coverageTopology = Objects.requireNonNull(coverageTopology);
        this.accountFilter = Optional.ofNullable(accountFilter);
        this.entityFilter = Optional.ofNullable(entityFilter);
        this.coverageJournal = Objects.requireNonNull(coverageJournal);
    }

    /**
     * Filters {@link TopologyEntityDTO} instances, extracted from the {@link CoverageTopology}.
     * The following filters are applied:
     *
     * <p>
     * <ol>
     *     <li>Entities are filtered by {@link #SUPPORTED_COVERABLE_ENTITY_TYPES}.
     *     <li>An {@link EntityFilter} is applied, if configured.
     *     <li>An {@link AccountFilter} is applied, if configured.
     *     <li>Any fully covered entities are removed, based on coverage from
     *     {@link ReservedInstanceCoverageJournal}.
     * </ol>
     *
     * @return A filtered {@link Stream} of {@link TopologyEntityDTO} instances
     */
    public Stream<TopologyEntityDTO> getCoverableEntities() {

        final Predicate<TopologyEntityDTO> entityDTOFilter = entityFilter
                .map(ef -> {
                    final Set<Long> entityIdSet = new HashSet(ef.getEntityIdList());
                    return (Predicate<TopologyEntityDTO>)(entity) ->
                            entityIdSet.contains(entity.getOid()); })
                .orElse(Predicates.alwaysTrue());

        final Predicate<TopologyEntityDTO> entityAccountFilter = accountFilter
                .map(af -> {
                    final Set<Long> accountIdSet = new HashSet<>(af.getAccountIdList());
                    return (Predicate<TopologyEntityDTO>)(entity) ->
                            coverageTopology.getOwner(entity.getOid())
                                    .map(owner -> accountIdSet.contains(owner.getOid()))
                                    .orElse(false); })
                .orElse(Predicates.alwaysTrue());

        return coverageTopology.getAllEntitiesOfType(SUPPORTED_COVERABLE_ENTITY_TYPES)
                .stream()
                .filter(entityDTOFilter)
                .filter(entityAccountFilter)
                .filter(entity -> !coverageJournal.isEntityAtCapacity(entity.getOid()));
    }

    /**
     * Filters {@link ReservedInstanceBought} instances, extracted from the {@link CoverageTopology},
     * based on whether coverage capacity is available (based on the {@link ReservedInstanceCoverageJournal})
     * and on the expiration date of the {@link com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec}
     * associated with the {@link ReservedInstanceBought}.
     *
     * <p>
     * Note: The {@link AccountFilter} is not applied to {@link ReservedInstanceBought} instance. It
     * is assumed the instances will have been previously filtered since the RI store provides filtering,
     * whereas the accepted {@link com.vmturbo.cost.calculation.integration.CloudTopology} (underlying the
     * {@link CoverageTopology} instance is immutable and therefore more difficult to filter.
     *
     * @return A filtered {@link Stream} of {@link ReservedInstanceBought} instances
     */
    public Stream<ReservedInstanceBought> getReservedInstances() {
        return coverageTopology.getAllReservedInstances().values().stream()
                .filter(ri -> !coverageJournal.isReservedInstanceAtCapacity(ri.getId()))
                .filter(ri -> coverageTopology.getSpecForReservedInstance(ri.getId())
                        .map(riSpec -> !ReservedInstanceHelper.isExpired(ri, riSpec))
                        .orElse(false));
    }
}
