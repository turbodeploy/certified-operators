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

import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregate;
import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentStatus;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageJournal;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

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

    @Nullable
    private final AccountFilter accountFilter;
    @Nullable
    private final EntityFilter entityFilter;
    @Nonnull
    private final CloudCommitmentCoverageJournal coverageJournal;

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
                                   @Nonnull CloudCommitmentCoverageJournal coverageJournal) {

        this.coverageTopology = Objects.requireNonNull(coverageTopology);
        this.accountFilter = accountFilter;
        this.entityFilter = entityFilter;
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
     *     {@link CloudCommitmentCoverageJournal}.
     * </ol>
     *
     * @return A filtered {@link Stream} of {@link TopologyEntityDTO} instances
     */
    public Stream<Long> getCoverageEntities() {

        final Predicate<Long> entityDTOFilter = Optional.ofNullable(entityFilter)
                .map(ef -> {
                    final Set<Long> entityIdSet = new HashSet(ef.getEntityIdList());
                    return (Predicate<Long>)(entityOid) ->
                            entityIdSet.contains(entityOid);
                }).orElse(Predicates.alwaysTrue());

        final Predicate<Long> entityAccountFilter = Optional.ofNullable(accountFilter)
                .map(af -> {
                    final Set<Long> accountIdSet = new HashSet<>(af.getAccountIdList());
                    return (Predicate<Long>)(entityOid) ->
                            coverageTopology.getAggregationInfo(entityOid)
                                    .map(aggregationInfo -> accountIdSet.contains(aggregationInfo.accountOid()))
                                    .orElse(false);
                }).orElse(Predicates.alwaysTrue());

        return SUPPORTED_COVERABLE_ENTITY_TYPES.stream()
                .map(coverageTopology::getEntitiesOfType)
                .flatMap(Set::stream)
                .filter(entityDTOFilter)
                .filter(entityAccountFilter)
                .filter(entityOid -> coverageTopology.getEntityInfo(entityOid)
                        .map(entityInfo -> entityInfo.entityState() == EntityState.POWERED_ON)
                        .orElse(false));
    }

    /**
     * Filters cloud commitment instances, extracted from the {@link CoverageTopology},
     * based on whether coverage capacity is available (based on the {@link CloudCommitmentCoverageJournal})
     * and on the expiration date of the commitment.
     *
     * <p>
     * Note: The {@link AccountFilter} is not applied to cloud commitments.
     *
     * @return A filtered {@link Stream} of {@link ReservedInstanceBought} instances
     */
    public Stream<CloudCommitmentAggregate> getCloudCommitments() {
        return coverageTopology.getAllCloudCommitmentAggregates().stream()
                .filter(commitmentAggregate -> commitmentAggregate.aggregationInfo().status() == CloudCommitmentStatus.CLOUD_COMMITMENT_STATUS_ACTIVE)
                .filter(commitmentAggregate ->
                        !coverageJournal.isCommitmentAtCapacity(commitmentAggregate.aggregateId()));
    }
}
