package com.vmturbo.reserved.instance.coverage.allocator.rules;

import static com.google.common.base.Predicates.not;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal;
import com.vmturbo.reserved.instance.coverage.allocator.context.CloudProviderCoverageContext;
import com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKey;
import com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKeyCreationConfig;
import com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKeyCreator;
import com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKeyCreator.CoverageKeyCreatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.key.CoverageKeyRepository;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.utils.ReservedInstanceHelper;

/**
 * A implementation of {@link ReservedInstanceCoverageRule} in which key creation for coverage
 * matching is configurable through {@link RICoverageRuleConfig}. A single instance of this class will
 * represent an individual rule for mapping RIs to entity for a specific cloud provider. For example,
 * in AWS the first rule is to match zonal RIs to VMs within the same account. Therefore, this class
 * can be configured to be zone scoped and not shared (matching the AWS rule)
 */
public class ConfigurableRICoverageRule implements ReservedInstanceCoverageRule {

    private final CloudProviderCoverageContext coverageContext;

    private final CoverageTopology coverageTopology;

    private final ReservedInstanceCoverageJournal coverageJournal;

    private final RICoverageRuleConfig ruleConfig;

    private final Comparator<Long> reservedInstanceComparator;

    private final Comparator<Long> entityComparator;

    private final Map<CoverageKeyCreationConfig, CoverageKeyCreator> keyCreatorsByConfig;

    private final String ruleIdentifier;

    private ConfigurableRICoverageRule(@Nonnull CloudProviderCoverageContext coverageContext,
                                      @Nonnull ReservedInstanceCoverageJournal coverageJournal,
                                      @Nonnull CoverageKeyCreatorFactory coverageKeyCreatorFactory,
                                      @Nonnull RICoverageRuleConfig ruleConfig) {

        this.coverageContext = Objects.requireNonNull(coverageContext);
        this.coverageTopology = Objects.requireNonNull(coverageContext.coverageTopology());
        this.coverageJournal = Objects.requireNonNull(coverageJournal);
        this.ruleConfig = Objects.requireNonNull(ruleConfig);

        // This comparator sorts the RIs smallest to largest (in regards to capacity and therefore instance
        // type). This mirrors the sorting applied for AWS billing (Azure does not publish their sorting
        // logic). It's further sorts by unallocated capacity (smallest to largest), attempting to fill
        // in those RIs that are close to being fully utilized. The last sort is by OID for determinism
        this.reservedInstanceComparator = Comparator.comparing(coverageJournal::getReservedInstanceCapacity)
                .thenComparing(coverageJournal::getUnallocatedCapacity)
                .thenComparing(Function.identity());
        // Sorts entities by smallest to largest instance type (based on coverage capacity). Similar to
        // the RI comparator, this mirrors AWS coverage application (Azure behavior is unknown and therefore
        // we treat it the same as AWS). It further sorts by uncovered capacity (smallest to largest),
        // attempting to fully cover partially covered entities rather than more evenly distribute
        // coverage across a topology. The final sort is by OID for determinism
        this.entityComparator = Comparator.comparing(coverageJournal::getEntityCapacity)
                .thenComparing(coverageJournal::getUncoveredCapacity)
                .thenComparing(Function.identity());

        this.ruleIdentifier = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("isSharedScope", ruleConfig.isSharedScope())
                .append("isZoneScoped", ruleConfig.isZoneScoped())
                .append("isPlatformFlexible", ruleConfig.isPlatformFlexible())
                .append("isSizeFlexbile", ruleConfig.isSizeFlexible())
                .build();

        this.keyCreatorsByConfig = createKeyCreators(Objects.requireNonNull(coverageKeyCreatorFactory));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Stream<ReservedInstanceCoverageGroup> coverageGroups() {
        final CoverageKeyRepository keyRepository = createKeyRepository();

        final Set<CoverageKey> coverageKeyIntersection = Sets.intersection(
                keyRepository.reservedInstancesByCoverageKey().keySet(),
                keyRepository.entitiesByCoverageKey().keySet());


        return coverageKeyIntersection.stream()
                .map(k -> this.createGroupFromKey(k, keyRepository));
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
    public String ruleIdentifier() {
        return ruleIdentifier;
    }

    /**
     * Creates and returns a new instance of {@link ConfigurableRICoverageRule}
     *
     * @param coverageContext An instance of {@link CloudProviderCoverageContext}
     * @param coverageJournal An instance of {@link ReservedInstanceCoverageJournal}
     * @param coverageKeyCreatorFactory An instance of {@link CoverageKeyCreatorFactory}
     * @param ruleConfig An instance of {@link RICoverageRuleConfig}
     * @return A newly created instance of {@link ConfigurableRICoverageRule}
     */
    public static ConfigurableRICoverageRule newInstance(
            @Nonnull CloudProviderCoverageContext coverageContext,
            @Nonnull ReservedInstanceCoverageJournal coverageJournal,
            @Nonnull CoverageKeyCreatorFactory coverageKeyCreatorFactory,
            @Nonnull RICoverageRuleConfig ruleConfig) {

        return new ConfigurableRICoverageRule(
                coverageContext,
                coverageJournal,
                coverageKeyCreatorFactory,
                ruleConfig);
    }

    /**
     * Creates all instances of {@link CoverageKeyCreator} based on the rule config for this instance.
     * The key creators are indexed by {@link CoverageKeyCreationConfig} to be able to find the
     * appropriate key creator for an instance of {@link ReservedInstanceBought}
     *
     * @param keyCreatorFactory An instance of {@link CoverageKeyCreatorFactory}
     * @return A map containing {@literal <Creation Config, Key Creator>}
     */
    private Map<CoverageKeyCreationConfig, CoverageKeyCreator> createKeyCreators(
            @Nonnull CoverageKeyCreatorFactory keyCreatorFactory) {

        Set<Boolean> isSizeFlexibleValues = ruleConfig.isSizeFlexible()
                .map(Collections::singleton)
                .orElse(Sets.newHashSet(Boolean.TRUE, Boolean.FALSE));
        Set<Boolean> isPlatformFlexibleValues = ruleConfig.isPlatformFlexible()
                .map(Collections::singleton)
                .orElse(Sets.newHashSet(Boolean.TRUE, Boolean.FALSE));

        final ImmutableMap.Builder keyCreatorsByConfigBuilder = ImmutableMap.builder();
        isSizeFlexibleValues.forEach(isSizeFlexible ->
                isPlatformFlexibleValues.forEach(isPlatformFlexible -> {
                    final CoverageKeyCreationConfig keyCreationConfig =
                            CoverageKeyCreationConfig.newBuilder()
                                    .isInstanceSizeFlexible(isSizeFlexible)
                                    .isPlatformFlexible(isPlatformFlexible)
                                    .isZoneScoped(ruleConfig.isZoneScoped())
                                    .isSharedScope(ruleConfig.isSharedScope())
                                    .build();
                    keyCreatorsByConfigBuilder.put(
                            keyCreationConfig,
                            keyCreatorFactory.newCreator(
                                    coverageTopology,
                                    keyCreationConfig));
                }));

        return keyCreatorsByConfigBuilder.build();
    }

    /**
     * Creates a new instance of {@link CoverageKeyRepository} based on {@link ReservedInstanceBought}
     * instances in scope (determined by {@link #getReservedInstancesInScope()} and {@link TopologyEntityDTO}
     * instances in scope (determined by {@link #getEntitiesInScope()}.
     *
     * @return A newly created instance of {@link CoverageKeyRepository}
     */
    @Nonnull
    private CoverageKeyRepository createKeyRepository() {
        final CoverageKeyRepository.Builder repositoryBuilder = CoverageKeyRepository.newBuilder();

        getReservedInstancesInScope().forEach(ri ->
                createKeyFromReservedInstance(ri)
                        .ifPresent(key -> repositoryBuilder.addReservedInstanceByKey(key, ri.getId())));

        getEntitiesInScope().forEach(entity ->
                createKeysFromEntity(entity)
                        .forEach(key -> repositoryBuilder.addEntityByKey(key, entity.getOid())));

        return repositoryBuilder.build();
    }

    /**
     * Creates a {@link ReservedInstanceCoverageGroup} based on {@link CoverageKey} entries within
     * an instance of {@link CoverageKeyRepository}
     * @param coverageKey An instance of {@link CoverageKey}
     * @param keyRepository An instance of {@link CoverageKeyRepository}
     * @return A newly created instance of {@link ReservedInstanceCoverageGroup}. The group will be
     * empty if {@code keyRepository} does not contain {@code coverageKey}
     */
    @Nonnull
    private ReservedInstanceCoverageGroup createGroupFromKey(@Nonnull CoverageKey coverageKey,
                                                             @Nonnull CoverageKeyRepository keyRepository) {
        final SortedSet<Long> sortedReservedInstanceOids =
                keyRepository.getReservedInstancesForKey(coverageKey)
                        .stream()
                        .collect(ImmutableSortedSet
                                .toImmutableSortedSet(reservedInstanceComparator));

        final SortedSet<Long> sortedEntityOids = keyRepository.getEntitiesForKey(coverageKey)
                .stream()
                .collect(ImmutableSortedSet
                        .toImmutableSortedSet(entityComparator));

        return ReservedInstanceCoverageGroup.of(
                coverageContext.cloudServiceProvider(),
                ruleIdentifier(),
                coverageKey,
                sortedReservedInstanceOids,
                sortedEntityOids);
    }

    /**
     * Determines the {@link TopologyEntityDTO} instances in scope for this rule, using
     * {@link CloudProviderCoverageContext#coverableEntityOids()} as a seed.
     *
     * @return A filtered {@link Stream} of {@link TopologyEntityDTO} instances
     */
    @Nonnull
    private Stream<TopologyEntityDTO> getEntitiesInScope() {
        return coverageContext.coverableEntityOids().stream()
                .filter(not(coverageJournal::isEntityAtCapacity))
                .map(coverageTopology::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    /**
     * Determines the {@link ReservedInstanceBought} instances in scope for this rule.
     *
     * @return A filtered {@link Stream} of {@link ReservedInstanceBought} instances
     */
    @Nonnull
    private Stream<ReservedInstanceBought> getReservedInstancesInScope() {
        return coverageContext.reservedInstanceOids().stream()
                .filter(not(coverageJournal::isReservedInstanceAtCapacity))
                .map(coverageTopology::getReservedInstanceBought)
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    /**
     * Creates instances of {@link CoverageKey} based on the context of the rule and
     * {@code reservedInstance}. Filters {@link ReservedInstanceBought} instances based on whether
     * their key config has an associated key creator
     *
     * @param reservedInstance The source instance of {@link ReservedInstanceBought} for the
     * {@link CoverageKey} instances
     * @return A {@link Set} of {@link CoverageKey} instances created from {@code reservedInstance}
     */
    @Nonnull
    private Optional<CoverageKey> createKeyFromReservedInstance(@Nonnull final ReservedInstanceBought reservedInstance) {

        return isReservedInstanceSizeFlexible(reservedInstance).map(isSizeFlexible ->
            isReservedInstancePlatformFlexible(reservedInstance).map(isPlatformFlexible -> {
                CoverageKeyCreationConfig riKeyConfig = CoverageKeyCreationConfig.newBuilder()
                        .isSharedScope(reservedInstance.getReservedInstanceBoughtInfo()
                                .getReservedInstanceScopeInfo().getShared())
                        .isZoneScoped(reservedInstance.getReservedInstanceBoughtInfo()
                                .hasAvailabilityZoneId())
                        .isInstanceSizeFlexible(isSizeFlexible)
                        .isPlatformFlexible(isPlatformFlexible)
                        .build();

                final CoverageKeyCreator keyCreator = keyCreatorsByConfig.get(riKeyConfig);

                return keyCreator != null ?
                        keyCreator.createKeyForReservedInstance(reservedInstance.getId()) :
                        Optional.<CoverageKey>empty();

            }).orElse(Optional.empty())
        ).orElse(Optional.empty());

    }

    /**
     * Creates instances of {@link CoverageKey} based on the context of the rule and
     * {@code entity}
     *
     * @param entity The source instance of {@link TopologyEntityDTO} for the {@link CoverageKey} instances
     * @return A {@link Set} of {@link CoverageKey} instances created from {@code entity}
     */
    @Nonnull
    private Set<CoverageKey> createKeysFromEntity(@Nonnull final TopologyEntityDTO entity) {
        return keyCreatorsByConfig.values().stream()
                .map(c -> c.createKeyForEntity(entity.getOid()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(ImmutableSet.toImmutableSet());
    }


    /**
     * Determines whether a {@link ReservedInstanceBought} is instance size flexible, based on the
     * {@link com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec} for the RI.
     * @param reservedInstance An instance of {@link ReservedInstanceBought}
     * @return An {@link Optional} containing a boolean determining size flexibility of the RI, if the
     * {@link com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec} for {@code reservedInstance}
     * can be found. An {@link Optional#empty()}, if the spec cannot be found.
     */
    private Optional<Boolean> isReservedInstanceSizeFlexible(@Nonnull final ReservedInstanceBought reservedInstance) {
        return coverageTopology.getSpecForReservedInstance(reservedInstance.getId())
                .map(ReservedInstanceHelper::isSpecInstanceSizeFlexible);
    }

    private Optional<Boolean> isReservedInstancePlatformFlexible(@Nonnull final ReservedInstanceBought reservedInstance) {
        return coverageTopology.getSpecForReservedInstance(reservedInstance.getId())
                .map(ReservedInstanceHelper::isSpecPlatformFlexible);
    }
}
