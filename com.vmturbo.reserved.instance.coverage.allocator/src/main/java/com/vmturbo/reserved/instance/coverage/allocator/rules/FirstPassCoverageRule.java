package com.vmturbo.reserved.instance.coverage.allocator.rules;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.vmturbo.cloud.common.commitment.CommitmentAmountUtils;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageJournal;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext;

/**
 * The first pass coverage rule fills in any partial coverage assignments present within the initial
 * coverage input. Partial coverages will be prioritized from smallest entity type to largest. Entities
 * with the same type will be prioritized according to the allocated coverage amount (greatest first).
 *
 * <p>For example, take the following allocations of an RI (with a capacity of 16):
 * <ul>
 *     <li> VM_A: 2 allocated (Capacity|4)
 *     <li> VM_B: 1 allocated (Capcity|4)
 *     <li> VM_C: 7 allocated (Capacity|16)
 * </ul>2
 * This filter would return a sorted entity set of VM_A, VM_B, VM_C
 */
public class FirstPassCoverageRule implements CoverageRule {

    private static final String RULE_TAG = "First Pass Coverage Rule";

    private final CloudProviderCoverageContext coverageContext;
    private final CloudCommitmentCoverageJournal coverageJournal;

    private FirstPassCoverageRule(@Nonnull CloudProviderCoverageContext coverageContext,
                                  @Nonnull CloudCommitmentCoverageJournal coverageJournal) {
        this.coverageContext = Objects.requireNonNull(coverageContext);
        this.coverageJournal = Objects.requireNonNull(coverageJournal);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<CoverageGroup> coverageGroups() {

        final Map<Long, Map<Long, CloudCommitmentAmount>> commitmentAllocationTable =
                coverageJournal.getCoverages().columnMap();

        return commitmentAllocationTable.entrySet().stream()
                .flatMap(commitmentEntry -> streamCoverageGroupsForCommitment(
                        commitmentEntry.getKey(), commitmentEntry.getValue()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean createsDisjointGroups() {
        return false;
    }

    @Override
    public String ruleTag() {
        return RULE_TAG;
    }

    private Stream<CoverageGroup> streamCoverageGroupsForCommitment(long commitmentOid,
                                                                    @Nonnull Map<Long, CloudCommitmentAmount> entityCoverageMap) {

        // First group the covered entities by the coverage type
        final SetMultimap<CloudCommitmentCoverageTypeInfo, Long> coverageTypeEntityMap = entityCoverageMap.entrySet()
                .stream()
                .flatMap(entityEntry -> CommitmentAmountUtils.extractTypeInfo(entityEntry.getValue())
                        .stream()
                        .map(coverageTypeInfo -> ImmutablePair.of(coverageTypeInfo, entityEntry.getKey())))
                .collect(ImmutableSetMultimap.toImmutableSetMultimap(
                        Pair::getKey,
                        Pair::getValue));

        return coverageTypeEntityMap.asMap().entrySet()
                .stream()
                .map(coverageTypeEntry -> CoverageGroup.builder()
                        .cloudServiceProvider(coverageContext.serviceProviderInfo())
                        .sourceTag(ruleTag())
                        .coverageTypeInfo(coverageTypeEntry.getKey())
                        .addCommitmentOids(commitmentOid)
                        .addAllEntityOids(coverageTypeEntry.getValue())
                        .build());
    }


    /**
     * Creates a new instance of {@link FirstPassCoverageRule}.
     * @param coverageContext An instance of {@link CloudProviderCoverageContext}
     * @param coverageJournal An instance of {@link CloudCommitmentCoverageJournal}
     * @return The newly created instance of {@link FirstPassCoverageRule}
     */
    @Nonnull
    public static CoverageRule newInstance(
            @Nonnull CloudProviderCoverageContext coverageContext,
            @Nonnull CloudCommitmentCoverageJournal coverageJournal) {

        return new FirstPassCoverageRule(coverageContext, coverageJournal);
    }
}
