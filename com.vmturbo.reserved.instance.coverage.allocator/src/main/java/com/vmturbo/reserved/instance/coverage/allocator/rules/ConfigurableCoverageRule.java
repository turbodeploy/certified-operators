package com.vmturbo.reserved.instance.coverage.allocator.rules;

import static com.google.common.base.Predicates.not;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.filter.CloudCommitmentFilter;
import com.vmturbo.cloud.common.commitment.filter.CloudCommitmentFilterFactory;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageJournal;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.CommitmentMatcher;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.ComputeCommitmentMatcher.ComputeCommitmentMatcherFactory;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.CoverageKey;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

/**
 * A {@link CoverageRule} implementation, based on a configurable set of rules to match commitments
 * to coverage entities. The rules are implemented through both a {@link CloudCommitmentFilter}
 * and {@link CommitmentMatcher}.
 */
public class ConfigurableCoverageRule implements CoverageRule {

    private final CloudProviderCoverageContext coverageContext;

    private final CoverageTopology coverageTopology;

    private final CloudCommitmentCoverageJournal coverageJournal;

    private final CommitmentMatcher commitmentMatcher;

    private final CloudCommitmentFilter cloudCommitmentFilter;

    private final SetMultimap<Long, CoverageKey> entityKeyMap;

    private final String ruleTag;

    private ConfigurableCoverageRule(@Nonnull CloudProviderCoverageContext coverageContext,
                                     @Nonnull CloudCommitmentCoverageJournal coverageJournal,
                                     @Nonnull CommitmentMatcher commitmentMatcher,
                                     @Nonnull CloudCommitmentFilter cloudCommitmentFilter,
                                     @Nonnull SetMultimap<Long, CoverageKey> entityKeyMap,
                                     @Nonnull String ruleTag) {

        this.coverageContext = Objects.requireNonNull(coverageContext);
        this.coverageTopology = Objects.requireNonNull(coverageContext.coverageTopology());
        this.coverageJournal = Objects.requireNonNull(coverageJournal);
        this.commitmentMatcher = Objects.requireNonNull(commitmentMatcher);
        this.cloudCommitmentFilter = Objects.requireNonNull(cloudCommitmentFilter);
        this.entityKeyMap = ImmutableSetMultimap.copyOf(Objects.requireNonNull(entityKeyMap));
        this.ruleTag = Objects.requireNonNull(ruleTag);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Stream<CoverageGroup> coverageGroups() {
        final CoverageKeyRepository keyRepository = createKeyRepository();

        final Set<CoverageKey> coverageKeyIntersection = Sets.intersection(
                keyRepository.commitmentsByKey().keySet(),
                keyRepository.entitiesByKey().keySet());


        return coverageKeyIntersection.stream()
                .flatMap(k -> this.createGroupsFromKey(k, keyRepository));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean createsDisjointGroups() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String ruleTag() {
        return ruleTag;
    }

    @Nonnull
    private CoverageKeyRepository createKeyRepository() {
        final CoverageKeyRepository.Builder repositoryBuilder = CoverageKeyRepository.builder();

        getCloudCommitmentsInScope().forEach(commitment ->
                commitmentMatcher.createKeysForCommitment(commitment).forEach(coverageKey ->
                    repositoryBuilder.putCommitmentsByKey(coverageKey, commitment.aggregateId())));


        coverageContext.coverableEntityOids().forEach(entityOid ->
                entityKeyMap.get(entityOid).forEach(coverageKey ->
                        repositoryBuilder.putEntitiesByKey(coverageKey, entityOid)));

        return repositoryBuilder.build();
    }

    @Nonnull
    private Stream<CoverageGroup> createGroupsFromKey(@Nonnull CoverageKey coverageKey,
                                                      @Nonnull CoverageKeyRepository keyRepository) {
        final Set<Long> commitmentOids = keyRepository.getCommitmentsForKey(coverageKey);

        final SetMultimap<CloudCommitmentCoverageTypeInfo, Long> commitmentsByCoverageType =
                commitmentOids.stream()
                        .map(coverageTopology::getCloudCommitment)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .flatMap(commitment -> commitment.coverageTypeInfoSet().stream()
                                .map(coverageTypeInfo -> ImmutablePair.of(coverageTypeInfo, commitment.aggregateId())))
                        .collect(ImmutableSetMultimap.toImmutableSetMultimap(
                                Pair::getKey,
                                Pair::getValue));

        final Set<Long> entityOids = keyRepository.getEntitiesForKey(coverageKey);

        return commitmentsByCoverageType.asMap().entrySet()
                .stream()
                .map(commitmentCoverageGroup -> CoverageGroup.builder()
                        .cloudServiceProvider(coverageContext.serviceProviderInfo())
                        .sourceKey(coverageKey)
                        .sourceTag(ruleTag)
                        .coverageTypeInfo(commitmentCoverageGroup.getKey())
                        .addAllCommitmentOids(commitmentCoverageGroup.getValue())
                        .addAllEntityOids(entityOids)
                        .build());

    }

    @Nonnull
    private Stream<CloudCommitmentAggregate> getCloudCommitmentsInScope() {
        return coverageContext.cloudCommitmentOids().stream()
                .filter(not(coverageJournal::isCommitmentAtCapacity))
                .map(coverageTopology::getCloudCommitment)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(cloudCommitmentFilter::filter);
    }

    /**
     * A factory class for producing {@link ConfigurableCoverageRule} instances.
     */
    public static class ConfigurableCoverageRuleFactory {

        private final CloudCommitmentFilterFactory cloudCommitmentFilterFactory;

        private final ComputeCommitmentMatcherFactory computeCommitmentMatcherFactory;

        /**
         * Constructs a new factory instance.
         * @param cloudCommitmentFilterFactory A factory for producing {@link CloudCommitmentFilter} instances.
         * @param computeCommitmentMatcherFactory A factory for producing {@link CommitmentMatcher} instances.
         */
        public ConfigurableCoverageRuleFactory(@Nonnull CloudCommitmentFilterFactory cloudCommitmentFilterFactory,
                                               @Nonnull ComputeCommitmentMatcherFactory computeCommitmentMatcherFactory) {
            this.cloudCommitmentFilterFactory = Objects.requireNonNull(cloudCommitmentFilterFactory);
            this.computeCommitmentMatcherFactory = Objects.requireNonNull(computeCommitmentMatcherFactory);
        }

        /**
         * Creates a new coverage rule, based on the {@link CoverageRuleConfig} provided. The configuration
         * will be converted to a {@link CloudCommitmentFilter} instance, in order to filter the cloud
         * commitment inventory within {@code coverageContext}, and a {@link CommitmentMatcher}, used
         * to generate coverag keys to match commitments to entities contained within {@code entityKeyMap}.
         * @param coverageContext The {@link CloudProviderCoverageContext}.
         * @param coverageJournal The {@link CloudCommitmentCoverageJournal}, used to check whether
         *                        the commitments or entities are at capacity.
         * @param entityKeyMap The map of entities to precomputed {@link CoverageKey} instances. The entity
         *                     coverage keys are precomputed based on all possible matching configurations
         *                     for the cloud provider. Only the keys for the cloud commitments are generated
         *                     based on the specific matching rule.
         * @param ruleConfig The rule configuration.
         * @return The newly constructed {@link ConfigurableCoverageRule} instance.
         */
        @Nonnull
        public ConfigurableCoverageRule createRule(@Nonnull CloudProviderCoverageContext coverageContext,
                                                   @Nonnull CloudCommitmentCoverageJournal coverageJournal,
                                                   @Nonnull SetMultimap<Long, CoverageKey> entityKeyMap,
                                                   @Nonnull CoverageRuleConfig ruleConfig) {

            final CloudCommitmentFilter cloudCommitmentFilter = cloudCommitmentFilterFactory.createFilter(
                    ruleConfig.commitmentSelectionConfig());
            final CommitmentMatcher commitmentMatcher = computeCommitmentMatcherFactory.newMatcher(
                    ruleConfig.commitmentMatcherConfig());

            return new ConfigurableCoverageRule(coverageContext,
                    coverageJournal,
                    commitmentMatcher,
                    cloudCommitmentFilter,
                    entityKeyMap,
                    ruleConfig.ruleTag());
        }
    }
}
