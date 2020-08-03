package com.vmturbo.reserved.instance.coverage.allocator.context;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;

/**
 * Contains the relevant contextual data for allocating coverage of a specific {@link CloudServiceProvider},
 * which will be a cloud service provider.
 * All {@link TopologyEntityDTO} instances and {@link ReservedInstanceBought} instances contained
 * within a context will be scoped to the corresponding {@link CloudServiceProvider}
 */
@Immutable
public class CloudProviderCoverageContext {

    /**
     * Represents a vendor/CSP as a single value of multiple probes. Note: this enum is a temporary
     * stopgap measure until CSP is a discovered topology entity.
     */
    public enum CloudServiceProvider {
        UNKNOWN,
        AWS,
        AZURE,
        GCP
    };

    private static final Map<SDKProbeType, CloudServiceProvider> PROBE_TYPE_TO_CSP =
            ImmutableMap.<SDKProbeType, CloudServiceProvider>builder()
                    .put(SDKProbeType.AWS, CloudServiceProvider.AWS)
                    .put(SDKProbeType.AWS_COST, CloudServiceProvider.AWS)
                    .put(SDKProbeType.AWS_BILLING, CloudServiceProvider.AWS)
                    .put(SDKProbeType.AWS_LAMBDA, CloudServiceProvider.AWS)
                    .put(SDKProbeType.AZURE, CloudServiceProvider.AZURE)
                    .put(SDKProbeType.AZURE_EA, CloudServiceProvider.AZURE)
                    .put(SDKProbeType.AZURE_SERVICE_PRINCIPAL, CloudServiceProvider.AZURE)
                    .put(SDKProbeType.AZURE_STORAGE_BROWSE, CloudServiceProvider.AZURE)
                    .put(SDKProbeType.AZURE_COST, CloudServiceProvider.AZURE)
                    .put(SDKProbeType.APPINSIGHTS, CloudServiceProvider.AZURE)
                    .put(SDKProbeType.GCP, CloudServiceProvider.GCP)
                    .build();

    private final CloudServiceProvider cloudServiceProvider;
    private final CoverageTopology coverageTopology;
    private final Set<Long> reservedInstanceOids;
    private final Set<Long> coverableEntityOids;


    private CloudProviderCoverageContext(@Nonnull Builder builder) {

        this.cloudServiceProvider = Objects.requireNonNull(builder.cloudServiceProvider);
        this.coverageTopology = Objects.requireNonNull(builder.coverageTopology);
        this.reservedInstanceOids = ImmutableSet.copyOf(builder.reservedInstanceOids);
        this.coverableEntityOids = ImmutableSet.copyOf(builder.coverableEntityOids);
    }

    /**
     * @return The {@link CloudServiceProvider} this context is scoped to
     */
    public CloudServiceProvider cloudServiceProvider() {
        return cloudServiceProvider;
    }

    /**
     * @return The {@link CoverageTopology}. Note: the topology *will not* be scoped to a
     * {@link CloudServiceProvider}. Rather, access to the {@link CoverageTopology} is meant
     * to facilitate resolving entities and reserved instances contained within this context.
     */
    @Nonnull
    public CoverageTopology coverageTopology() {
        return coverageTopology;
    }

    /**
     * @return Oids of {@link ReservedInstanceBought} instances, scoped to a {@link CloudServiceProvider}
     * of this context
     */
    @Nonnull
    public Set<Long> reservedInstanceOids() {
        return reservedInstanceOids;
    }

    /**
     * @return Oids of {@link TopologyEntityDTO} instances, scoped to a {@link CloudServiceProvider}
     * of this context
     */
    @Nonnull
    public Set<Long> coverableEntityOids() {
        return coverableEntityOids;
    }

    /**
     * Creates a set of {@link CloudProviderCoverageContext} instances, based on the {@link CoverageTopology},
     * {@link ReservedInstanceBought} instances, and {@link TopologyEntityDTO} instances. The RIs and
     * entities will be split into separate contexts based on the {@link CloudServiceProvider}
     * associated with the origin of each entity
     *
     *
     * @param coverageTopology An instance of {@link CoverageTopology}, used to resolve oids of
     *                         both {@link ReservedInstanceBought} instances and
     *                         {@link TopologyEntityDTO} instances.
     * @param reservedInstances A {@link Stream} of {@link ReservedInstanceBought} instances
     * @param entities A {@link Stream} of {@link TopologyEntityDTO} instances
     * @param skipPartialContexts If true, any context with only {@link ReservedInstanceBought}
     *                            instances or {@link TopologyEntityDTO} instances will not be returned.
     *                            If false, all created contexts will be returned.
     * @return A set of {@link CloudProviderCoverageContext} instances, containing only oid references
     * to those {@link ReservedInstanceBought} and {@link TopologyEntityDTO} instances passed in
     * through {@code reservedInstances} and {@code entities}.
     */
    public static Set<CloudProviderCoverageContext> createContexts(
            @Nonnull CoverageTopology coverageTopology,
            @Nonnull Stream<ReservedInstanceBought> reservedInstances,
            @Nonnull Stream<TopologyEntityDTO> entities,
            boolean skipPartialContexts) {


        final Map<CloudServiceProvider, CloudProviderCoverageContext.Builder> contextBuildersByProvider =
                new EnumMap<>(CloudServiceProvider.class);

        final Function<Long, Optional<CloudServiceProvider>> resolveCSPForEntity = (entityOid) ->
                coverageTopology.getProbeTypesForEntity(entityOid)
                        .stream()
                        .map(PROBE_TYPE_TO_CSP::get)
                        .filter(Objects::nonNull)
                        .distinct()
                        // If more than one CSP is is linked to an entity, return no CSP
                        .collect(Collectors.reducing((csp1, csp2) -> null));

        reservedInstances.forEach(ri -> {
            final long accountOid = ri.getReservedInstanceBoughtInfo().getBusinessAccountId();
            resolveCSPForEntity.apply(accountOid).ifPresent(cloudServiceProvider ->
                    contextBuildersByProvider.computeIfAbsent(cloudServiceProvider, (csp) ->
                            CloudProviderCoverageContext.newBuilder()
                                    .cloudServiceProvider(cloudServiceProvider)
                                    .coverageTopology(coverageTopology))
                            .reservedInstanceOid(ri.getId()));
        });

        entities.forEach(entity ->
            resolveCSPForEntity.apply(entity.getOid()).ifPresent(cloudServiceProvider ->
                    contextBuildersByProvider.computeIfAbsent(cloudServiceProvider, (csp) ->
                        CloudProviderCoverageContext.newBuilder()
                                .cloudServiceProvider(cloudServiceProvider)
                                .coverageTopology(coverageTopology))
                        .coverableEntityOid(entity.getOid())));

        return contextBuildersByProvider.values().stream()
                .filter(contextBuilder -> !skipPartialContexts ||
                        (contextBuilder.hasReservedInstanceOids() &&
                                contextBuilder.hasCoverableEntityOids()))
                .map(CloudProviderCoverageContext.Builder::build)
                .collect(Collectors.toSet());
    }

    /**
     * @return A new instance of {@link Builder}
     */
    public static Builder newBuilder() {
        return new Builder();
    }


    /**
     * A builder class used to create an instance of {@link CloudProviderCoverageContext}
     */
    public static class Builder {
        private CloudServiceProvider cloudServiceProvider;
        private CoverageTopology coverageTopology;
        private final Set<Long> reservedInstanceOids = new HashSet<>();
        private final Set<Long> coverableEntityOids = new HashSet<>();

        /**
         * Set the cloud service provider (represented as a {@link CloudServiceProvider}) of this builder.
         * @param cloudServiceProvider An instance of {@link CloudServiceProvider}
         * @return The instance of {@link Builder} for method chaining
         */
        public Builder cloudServiceProvider(@Nonnull CloudServiceProvider cloudServiceProvider) {
            this.cloudServiceProvider = Objects.requireNonNull(cloudServiceProvider);
            return this;
        }

        /**
         * Set the {@link CoverageTopology} of this builder
         * @param coverageTopology An instance of {@link CoverageTopology}
         * @return The instance of {@link Builder} for method chaining
         */
        public Builder coverageTopology(@Nonnull CoverageTopology coverageTopology) {
            this.coverageTopology = Objects.requireNonNull(coverageTopology);
            return this;
        }

        /**
         * Add an oid of a {@link ReservedInstanceBought} to this builder
         * @param riOid A {@link ReservedInstanceBought} oid
         * @return The instance of {@link Builder} for method chaining
         */
        public Builder reservedInstanceOid(@Nonnull long riOid) {
            this.reservedInstanceOids.add(Objects.requireNonNull(riOid));
            return this;
        }

        /**
         * @return True, if this builder is configured with {@link ReservedInstanceBought} oids.
         * False, otherwise.
         */
        public boolean hasReservedInstanceOids() {
            return !reservedInstanceOids.isEmpty();
        }

        /**
         * Add an oid of a {@link TopologyEntityDTO} to this builder
         * @param entityOid A {@link TopologyEntityDTO} oid
         * @return The instance of {@link Builder} for method chaining
         */
        public Builder coverableEntityOid(long entityOid) {
            this.coverableEntityOids.add(entityOid);
            return this;
        }

        /**
         * @return True, if this builder is configured with {@link TopologyEntityDTO} oids.
         * False, otherwise.
         */
        public boolean hasCoverableEntityOids() {
            return !coverableEntityOids.isEmpty();
        }

        /**
         * @return A new instance of {@link CloudProviderCoverageContext}, created from this builder.
         */
        public CloudProviderCoverageContext build() {
            return new CloudProviderCoverageContext(this);
        }
    }
}
