package com.vmturbo.reserved.instance.coverage.allocator.matcher.entity;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentLocation;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentScope;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.ComputeCoverageKey;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.ComputeCoverageKey.Builder;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.CoverageKey;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.VirtualMachineMatcherConfig.TierMatcher;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CloudAggregationInfo;
import com.vmturbo.reserved.instance.coverage.allocator.topology.ComputeTierInfo;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageEntityInfo;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.VirtualMachineInfo;

/**
 * A default implementation of {@link CoverageEntityMatcher}.
 */
public class DefaultCoverageEntityMatcher implements CoverageEntityMatcher {

    private final Logger logger = LogManager.getLogger();

    private final CoverageTopology coverageTopology;

    private final Set<EntityMatcherConfig> matcherConfigs;

    private DefaultCoverageEntityMatcher(@Nonnull CoverageTopology coverageTopology,
                                         @Nonnull Set<EntityMatcherConfig> matcherConfigs) {

        this.coverageTopology = Objects.requireNonNull(coverageTopology);
        this.matcherConfigs = ImmutableSet.copyOf(Objects.requireNonNull(matcherConfigs));
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Set<CoverageKey> createCoverageKeys(final long entityOid) {

        return createEntityContext(entityOid)
                .map(entityContext -> matcherConfigs.stream()
                        // Assume the config is a VM matcher config
                        .map(config -> flattenConfig((VirtualMachineMatcherConfig)config))
                        .flatMap(Set::stream)
                        .map(config -> createKeyFromConfig(config, entityContext))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet()))
                .orElse(Collections.<CoverageKey>emptySet());
    }

    private Set<FlattenedVMConfig> flattenConfig(VirtualMachineMatcherConfig vmConfig) {

        final Set<FlattenedVMConfig> configSet = new HashSet<>();

        vmConfig.locations().forEach(location ->
                vmConfig.scopes().forEach(scope ->
                        vmConfig.tierMatchers().forEach(tierMatcher ->
                                configSet.add(FlattenedVMConfig.builder()
                                        .location(location)
                                        .scope(scope)
                                        .tierMatcher(tierMatcher)
                                        .includePlatform(vmConfig.includePlatform())
                                        .includeTenancy(vmConfig.includeTenancy())
                                        .build()))));

        return configSet;
    }

    @Nullable
    private CoverageKey createKeyFromConfig(@Nonnull FlattenedVMConfig config,
                                            @Nonnull EntityContext entityContext) {

        final boolean requiredDataAvailable =
                ((config.tierMatcher() == TierMatcher.NONE)
                        || entityContext.computeTierInfo().family().isPresent());

        if (requiredDataAvailable) {

            final CloudAggregationInfo aggregationInfo = entityContext.aggregationInfo();
            final Builder keyBuilder = ComputeCoverageKey.builder()
                    .billingFamilyId(aggregationInfo.billingFamilyId());

            if (config.scope() == CloudCommitmentScope.ACCOUNT) {
                keyBuilder.accountOid(aggregationInfo.accountOid());
            }

            // Add location scoping info
            if (config.location() == CloudCommitmentLocation.REGION) {
                keyBuilder.regionOid(aggregationInfo.regionOid());
            } else if (config.location() == CloudCommitmentLocation.AVAILABILITY_ZONE) {
                keyBuilder.regionOid(aggregationInfo.regionOid());
                // If the zone isn't available, we'll just fall back to matching the entity
                // to only regional commitments
                keyBuilder.zoneOid(aggregationInfo.zoneOid());
            }

            // Add compute tier info
            if (config.tierMatcher() != TierMatcher.NONE) {
                final ComputeTierInfo computeTierInfo = entityContext.computeTierInfo();
                keyBuilder.tierFamily(computeTierInfo.family().get());

                if (config.tierMatcher() == TierMatcher.TIER) {
                    keyBuilder.tierOid(computeTierInfo.tierOid());
                }
            }

            // Add platform and tenancy info
            final VirtualMachineInfo vmInfo = (VirtualMachineInfo)entityContext.entityInfo();
            if (config.includePlatform()) {
                keyBuilder.platform(vmInfo.platform());
            }

            if (config.includeTenancy()) {
                keyBuilder.tenancy(vmInfo.tenancy());
            }

            return keyBuilder.build();
        } else {
            return null;
        }
    }

    @Nonnull
    private Optional<EntityContext> createEntityContext(long entityOid) {
        final Optional<ComputeTierInfo> computeTierInfo = coverageTopology.getComputeTierInfoForEntity(entityOid);
        final Optional<CoverageEntityInfo> entityInfo = coverageTopology.getEntityInfo(entityOid);
        final Optional<CloudAggregationInfo> aggregationInfo = coverageTopology.getAggregationInfo(entityOid);

        if (computeTierInfo.isPresent() && entityInfo.isPresent() && aggregationInfo.isPresent()) {
            return Optional.of(EntityContext.builder()
                    .entityOid(entityOid)
                    .entityInfo(entityInfo.get())
                    .aggregationInfo(aggregationInfo.get())
                    .computeTierInfo(computeTierInfo.get())
                    .build());
        } else {
            logger.error("Unable to find required info for entity (Tier Info={}, Entity Info={}, Aggregation Info={})",
                    computeTierInfo.isPresent(), entityInfo.isPresent(), aggregationInfo.isPresent());
            return Optional.empty();
        }
    }

    /**
     * Data class containing context information for creating coverage keys from an entity.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface EntityContext {

        /**
         * The entity OID.
         * @return The entity OID.
         */
        long entityOid();

        /**
         * Info about the entity, including its type and state.
         * @return Info about the entity.
         */
        @Nonnull
        CoverageEntityInfo entityInfo();

        /**
         * Aggregation info about an entity (e.g. region, account, etc).
         * @return Aggregation info about an entity (e.g. region, account, etc).
         */
        @Nonnull
        CloudAggregationInfo aggregationInfo();

        /**
         * Tier info about the entity (right now, only compute tier info is supported).
         * @return Tier info about the entity.
         */
        @Nonnull
        ComputeTierInfo computeTierInfo();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for {@link EntityContext}.
         */
        class Builder extends ImmutableEntityContext.Builder {}
    }

    /**
     * Represents a flattened version of {@link EntityMatcherConfig}. A flatted version has a one-to-one
     * relationship with {@link CoverageKey} generation.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface FlattenedVMConfig {

        /**
         * How to match the entity location to a cloud commitment.
         * @return The location matcher.
         */
        @Nonnull
        CloudCommitmentLocation location();

        /**
         * How to match the scope (billing family, account, etc) to a cloud commitment.
         * @return The commitment scope.
         */
        @Nonnull
        CloudCommitmentScope scope();

        /**
         * How to match the tier of an entity to a cloud commitment.
         * @return The tier matcher.
         */
        TierMatcher tierMatcher();

        /**
         * Whether the entity's platform should be included in the coverage key.
         * @return Whether the platform should be included.
         */
        boolean includePlatform();

        /**
         * Whether the entity's tenancy should be included in the coverage key.
         * @return Whether the tenancy should be included.
         */
        boolean includeTenancy();

        /**
         * Constructs and returns a new {@link EntityContext.Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for {@link FlattenedVMConfig} instances.
         */
        class Builder extends ImmutableFlattenedVMConfig.Builder {}
    }

    /**
     * The default implementation of {@link CoverageEntityMatcherFactory}, used to create
     * {@link DefaultCoverageEntityMatcher} instances.
     */
    public static class DefaultCoverageEntityMatcherFactory implements CoverageEntityMatcherFactory {

        /**
         * {@inheritDoc}.
         */
        @Override
        public CoverageEntityMatcher createEntityMatcher(@Nonnull final CoverageTopology coverageTopology,
                                                         @Nonnull final Set<EntityMatcherConfig> matcherConfigs) {
            return new DefaultCoverageEntityMatcher(coverageTopology, matcherConfigs);
        }
    }
}
