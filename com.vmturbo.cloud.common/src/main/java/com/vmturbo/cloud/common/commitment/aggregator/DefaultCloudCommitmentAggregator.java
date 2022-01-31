package com.vmturbo.cloud.common.commitment.aggregator;

import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.cloud.common.commitment.CloudCommitmentResourceScope;
import com.vmturbo.cloud.common.commitment.CloudCommitmentResourceScope.ComputeTierResourceScope;
import com.vmturbo.cloud.common.commitment.CloudCommitmentResourceScope.ComputeTierResourceScope.PlatformInfo;
import com.vmturbo.cloud.common.commitment.CloudCommitmentTopology;
import com.vmturbo.cloud.common.commitment.CloudCommitmentTopology.CloudCommitmentTopologyFactory;
import com.vmturbo.cloud.common.commitment.ReservedInstanceData;
import com.vmturbo.cloud.common.commitment.TopologyCommitmentData;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.topology.BillingFamilyRetriever;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentEntityScope;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentEntityScope.EntityScope;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentEntityScope.GroupScope;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentLocation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentLocationType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentScope;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * The default implementation of {@link CloudCommitmentAggregator}.
 */
public class DefaultCloudCommitmentAggregator implements CloudCommitmentAggregator {

    private static final CloudCommitmentLocation GLOBAL_LOCATION = CloudCommitmentLocation.newBuilder()
            .setLocationType(CloudCommitmentLocationType.GLOBAL)
            .build();

    private final Logger logger = LogManager.getLogger();

    private final IdentityProvider identityProvider;

    private final ComputeTierFamilyResolver computeTierFamilyResolver;

    private final BillingFamilyRetriever billingFamilyRetriever;

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    private final CloudCommitmentTopology commitmentTopology;

    private final ConcurrentMap<AggregationInfo, Set<CloudCommitmentData<?>>> commitmentAggregateMap =
            new ConcurrentHashMap<>();

    private DefaultCloudCommitmentAggregator(@Nonnull IdentityProvider identityProvider,
                                             @Nonnull ComputeTierFamilyResolver computeTierFamilyResolver,
                                             @Nonnull BillingFamilyRetriever billingFamilyRetriever,
                                             @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                             @Nonnull CloudCommitmentTopology commitmentTopology) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.computeTierFamilyResolver = Objects.requireNonNull(computeTierFamilyResolver);
        this.billingFamilyRetriever = Objects.requireNonNull(billingFamilyRetriever);
        this.cloudTopology = Objects.requireNonNull(cloudTopology);
        this.commitmentTopology = Objects.requireNonNull(commitmentTopology);
    }

    /**
     * This lock is reversed, allowing multiple threads to write (through {@link #collectCommitment(CloudCommitmentData)}),
     * while only allowing a single thread to query the stored aggregates.
     */
    private final ReadWriteLock commitmentsLock = new ReentrantReadWriteLock();


    /**
     * {@inheritDoc}.
     */
    @Override
    public void collectCommitment(@Nonnull final CloudCommitmentData commitmentData)
            throws AggregationFailureException {

        commitmentsLock.readLock().lock();
        try {
            final AggregationInfo aggregateInfo;
            if (commitmentData.type() == CloudCommitmentType.RESERVED_INSTANCE) {
                aggregateInfo = buildRIAggregateInfo(commitmentData.asReservedInstance());
            } else if (commitmentData.type() == CloudCommitmentType.TOPOLOGY_COMMITMENT) {
                aggregateInfo = buildCommitmentAggregationInfo(commitmentData.asTopologyCommitment());
            } else {
                throw new UnsupportedOperationException(
                        String.format("%s is not a supported commitment type", commitmentData.type()));
            }

            commitmentAggregateMap.computeIfAbsent(aggregateInfo, ai -> Sets.newConcurrentHashSet())
                    .add(commitmentData);
        } finally {
            commitmentsLock.readLock().unlock();
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Set<CloudCommitmentAggregate> getAggregates() {
        commitmentsLock.writeLock().lock();
        try {
            return commitmentAggregateMap.entrySet().stream()
                    .map(aggregateEntry -> {
                        final AggregationInfo aggregateInfo = aggregateEntry.getKey();
                        if (aggregateInfo.commitmentType() == CloudCommitmentType.RESERVED_INSTANCE) {
                            return ReservedInstanceAggregate.builder()
                                    .aggregateId(identityProvider.next())
                                    .aggregationInfo((ReservedInstanceAggregationInfo)aggregateInfo)
                                    // There's an assumption that all aggregates under the aggregate
                                    // info conform to the info's type.
                                    .addAllCommitments((Set)aggregateEntry.getValue())
                                    .build();

                        } else if (aggregateInfo.commitmentType() == CloudCommitmentType.TOPOLOGY_COMMITMENT) {
                            return TopologyCommitmentAggregate.builder()
                                    .aggregateId(identityProvider.next())
                                    .aggregationInfo((TopologyCommitmentAggregationInfo)aggregateInfo)
                                    .addAllCommitments((Set)aggregateEntry.getValue())
                                    .build();
                        } else {
                            throw new UnsupportedOperationException(
                                    String.format("%s is not a supported commitment type",
                                            aggregateInfo.commitmentType()));
                        }
                    }).collect(ImmutableSet.toImmutableSet());
        } finally {
            commitmentsLock.writeLock().unlock();
        }
    }


    protected ReservedInstanceAggregationInfo buildRIAggregateInfo(@Nonnull ReservedInstanceData commitmentData)
            throws AggregationFailureException {

        try {
            final ReservedInstanceBoughtInfo riBoughtInfo = commitmentData.commitment().getReservedInstanceBoughtInfo();
            final ReservedInstanceScopeInfo scopeInfo = riBoughtInfo.getReservedInstanceScopeInfo();
            final ReservedInstanceSpecInfo riSpecInfo = commitmentData.spec().getReservedInstanceSpecInfo();

            final long serviceProviderOid = cloudTopology.getServiceProvider(riSpecInfo.getTierId())
                    .map(TopologyEntityDTO::getOid)
                    .orElse(0L);

            final CloudCommitmentLocation commitmentLocation = CloudCommitmentLocation.newBuilder()
                    .setLocationType(riBoughtInfo.hasAvailabilityZoneId()
                            ? CloudCommitmentLocationType.AVAILABILITY_ZONE
                            : CloudCommitmentLocationType.REGION)
                    .setLocationOid(riBoughtInfo.hasAvailabilityZoneId()
                            ? riBoughtInfo.getAvailabilityZoneId()
                            : riSpecInfo.getRegionId())
                    .build();

            // Determine the purchasing account/billing family scope. If the commitment does not
            // have a billing family or the commitment is shared across the BF/group of accounts including
            // the purchasing account, the aggregate info will include the purchasing account.
            final long purchasingAccount = riBoughtInfo.getBusinessAccountId();
            final OptionalLong billingFamilyId = billingFamilyRetriever.getBillingFamilyForAccount(purchasingAccount)
                    .map(GroupAndMembers::group)
                    .map(Grouping::getId)
                    .map(OptionalLong::of)
                    .orElse(OptionalLong.empty());

            final boolean isPurchasingAccountScoped = !billingFamilyId.isPresent()
                    || scopeInfo.getShared()
                    || scopeInfo.getApplicableBusinessAccountIdList().contains(purchasingAccount);

            final OptionalLong purchaseAccountScope = isPurchasingAccountScoped
                    ? OptionalLong.of(purchasingAccount)
                    : OptionalLong.empty();

            final CloudCommitmentEntityScope entityScope;
            if (scopeInfo.getShared() && billingFamilyId.isPresent()) {
                entityScope = CloudCommitmentEntityScope.newBuilder()
                        .setScopeType(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                        .setGroupScope(GroupScope.newBuilder()
                                .addGroupId(billingFamilyId.getAsLong())
                                .build())
                        .build();
            } else if (scopeInfo.getApplicableBusinessAccountIdCount() != 0) {
                entityScope = CloudCommitmentEntityScope.newBuilder()
                        .setScopeType(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)
                        .setEntityScope(EntityScope.newBuilder()
                                .addAllEntityOid(scopeInfo.getApplicableBusinessAccountIdList())
                                .build())
                        .build();
            } else {
                entityScope = CloudCommitmentEntityScope.newBuilder()
                        .setScopeType(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_UNKNOWN)
                        .setEntityScope(EntityScope.newBuilder()
                                .addEntityOid(purchasingAccount)
                                .build())
                        .build();
            }

            final PlatformInfo platformInfo = PlatformInfo.builder()
                    .isPlatformFlexible(riSpecInfo.getPlatformFlexible())
                    .platform(riSpecInfo.getPlatformFlexible()
                            ? OSType.UNKNOWN_OS
                            : riSpecInfo.getOs())
                    .build();

            final ComputeTierResourceScope resourceScope = ComputeTierResourceScope.builder()
                    .platformInfo(platformInfo)
                    .computeTierFamily(riSpecInfo.getSizeFlexible()
                            ? computeTierFamilyResolver.getCoverageFamily(riSpecInfo.getTierId())
                                .orElseThrow(() -> new AggregationFailureException(String.format(
                                        "Unable to resolve family for tier ID %s for size flexible RI %s",
                                        riSpecInfo.getTierId(), commitmentData.commitmentId())))
                            : null)
                    .computeTier(riSpecInfo.getSizeFlexible()
                            ? OptionalLong.empty()
                            : OptionalLong.of(riSpecInfo.getTierId()))
                    .addTenancies(riSpecInfo.hasTenancy() ? riSpecInfo.getTenancy() : Tenancy.DEFAULT)
                    .build();

            return ReservedInstanceAggregationInfo.builder()
                    .serviceProviderOid(serviceProviderOid)
                    .purchasingAccountOid(purchaseAccountScope)
                    .coverageType(CloudCommitmentCoverageType.COUPONS)
                    .entityScope(entityScope)
                    .location(commitmentLocation)
                    .resourceScope(resourceScope)
                    .build();
        } catch (Exception e) {
            // This may happen if billing family or compute tier retrieval fails
            logger.error("Error aggregating cloud commitment (OID={}, Type={})",
                    commitmentData.commitmentId(), commitmentData.type(), e);

            if (e instanceof AggregationFailureException) {
                throw e;
            } else {
                throw new AggregationFailureException(e);
            }
        }
    }

    protected TopologyCommitmentAggregationInfo buildCommitmentAggregationInfo(@Nonnull TopologyCommitmentData commitmentData)
            throws AggregationFailureException {

        Preconditions.checkArgument((commitmentData.commitment().getTypeSpecificInfo().hasCloudCommitmentData()));

        final CloudCommitmentInfo commitmentInfo = commitmentData.commitment().getTypeSpecificInfo().getCloudCommitmentData();
        final long commitmentId = commitmentData.commitmentId();

        final long serviceProviderId = cloudTopology.getServiceProvider(commitmentId)
                .map(TopologyEntityDTO::getOid)
                .orElseThrow(() -> new AggregationFailureException(String.format(
                        "Unable to resolve service provider for commitment %s", commitmentId)));

        final CloudCommitmentLocation commitmentLocation = cloudTopology.getConnectedAvailabilityZone(commitmentId)
                .map(zone -> CloudCommitmentLocation.newBuilder()
                        .setLocationType(CloudCommitmentLocationType.AVAILABILITY_ZONE)
                        .setLocationOid(zone.getOid())
                        .build())
                .orElseGet(() -> cloudTopology.getConnectedRegion(commitmentId)
                        .map(region -> CloudCommitmentLocation.newBuilder()
                                .setLocationType(CloudCommitmentLocationType.REGION)
                                .setLocationOid(region.getOid())
                                .build())
                        .orElse(GLOBAL_LOCATION));

        final long purchasingAccountOid = cloudTopology.getOwner(commitmentId)
                .map(TopologyEntityDTO::getOid)
                .orElseThrow(() -> new AggregationFailureException(String.format(
                        "Unable to resolve purchasing account for commitment %s", commitmentId)));

        final CloudCommitmentEntityScope entityScope;
        switch (commitmentInfo.getCommitmentScope()) {
            case CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP:
                final long billingFamilyId = billingFamilyRetriever.getBillingFamilyForAccount(purchasingAccountOid)
                        .map(GroupAndMembers::group)
                        .map(Grouping::getId)
                        .orElseThrow(() -> new AggregationFailureException(String.format(
                                "Unable to resolve purchasing account for commitment %s", commitmentId)));
                entityScope = CloudCommitmentEntityScope.newBuilder()
                        .setScopeType(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP)
                        .setGroupScope(GroupScope.newBuilder()
                                .addGroupId(billingFamilyId)
                                .build())
                        .build();
                break;
            case CLOUD_COMMITMENT_SCOPE_ACCOUNT:
                entityScope = CloudCommitmentEntityScope.newBuilder()
                        .setScopeType(CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)
                        .setEntityScope(EntityScope.newBuilder()
                                .addAllEntityOid(commitmentTopology.getCoveredAccounts(commitmentId))
                                .build())
                        .build();
                break;
            case CLOUD_COMMITMENT_SCOPE_UNKNOWN:
                throw new AggregationFailureException(String.format("Unknown entity scope for commitment %s", commitmentId));
            default:
                throw new UnsupportedOperationException(String.format("Unsupported entity scope for commitment %s", commitmentId));
        }

        // resource scope
        final CloudCommitmentResourceScope resourceScope;
        switch (commitmentInfo.getScopeCase()) {
            case SERVICE_RESTRICTED:
                resourceScope = CloudCommitmentResourceScope.builder()
                        .addAllCloudServices(commitmentTopology.getCoveredCloudServices(commitmentId))
                        .build();
                break;
            case FAMILY_RESTRICTED:
                resourceScope = ComputeTierResourceScope.builder()
                        .addAllCloudServices(commitmentTopology.getCoveredCloudServices(commitmentId))
                        .addCoveredEntityType(EntityType.VIRTUAL_MACHINE)
                        .computeTierFamily(commitmentInfo.getFamilyRestricted().getInstanceFamily())
                        .platformInfo(ComputeTierResourceScope.PlatformInfo.builder()
                                .isPlatformFlexible(true)
                                .build())
                        .addTenancies(Tenancy.DEFAULT)
                        .build();
                break;
            case SCOPE_NOT_SET:
                throw new AggregationFailureException(String.format("Resource scope not set for commitment %s", commitmentId));
            default:
                throw new AggregationFailureException(String.format("Unsupported resource scope for commitment %s", commitmentId));
        }

        final CloudCommitmentCoverageType coverageType;
        switch (commitmentInfo.getCommitmentCase()) {
            case NUMBER_COUPONS:
                coverageType = CloudCommitmentCoverageType.COUPONS;
                break;
            case SPEND:
                coverageType = CloudCommitmentCoverageType.SPEND_COMMITMENT;
                break;
            case COMMODITIES_BOUGHT:
                coverageType = CloudCommitmentCoverageType.COMMODITY;
                break;
            default:
                throw new AggregationFailureException(String.format("Coverage type not set for commitment %s", commitmentId));
        }

        return TopologyCommitmentAggregationInfo.builder()
                .serviceProviderOid(serviceProviderId)
                .purchasingAccountOid(purchasingAccountOid)
                .status(commitmentInfo.getCommitmentStatus())
                .coverageType(coverageType)
                .entityScope(entityScope)
                .location(commitmentLocation)
                .resourceScope(resourceScope)
                .build();
    }

    /**
     * The default implementation of an identity aggregator, which aggregates commitments based solely
     * on their assigned IDs.
     */
    public static class DefaultIdentityAggregator extends DefaultCloudCommitmentAggregator {

        private final Map<Long, CloudCommitmentAggregate> commitmentAggregatesMap =
                Maps.newConcurrentMap();

        private DefaultIdentityAggregator(@Nonnull IdentityProvider identityProvider,
                                          @Nonnull ComputeTierFamilyResolver computeTierFamilyResolver,
                                          @Nonnull BillingFamilyRetriever billingFamilyRetriever,
                                          @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                          @Nonnull CloudCommitmentTopology commitmentTopology) {
            super(identityProvider, computeTierFamilyResolver, billingFamilyRetriever, cloudTopology, commitmentTopology);
        }

        /**
         * {@inheritDoc}l
         */
        @Override
        public void collectCommitment(@Nonnull final CloudCommitmentData commitmentData)
                throws AggregationFailureException {

            final CloudCommitmentAggregate commitmentAggregate;
            if (commitmentData.type() == CloudCommitmentType.RESERVED_INSTANCE) {
                commitmentAggregate = ReservedInstanceAggregate.builder()
                        .aggregateId(commitmentData.commitmentId())
                        .aggregationInfo(buildRIAggregateInfo(commitmentData.asReservedInstance()))
                        .addCommitments(commitmentData.asReservedInstance())
                        .build();
            } else if (commitmentData.type() == CloudCommitmentType.TOPOLOGY_COMMITMENT) {
                commitmentAggregate = TopologyCommitmentAggregate.builder()
                        .aggregateId(commitmentData.commitmentId())
                        .aggregationInfo(buildCommitmentAggregationInfo(commitmentData.asTopologyCommitment()))
                        .addCommitments(commitmentData.asTopologyCommitment())
                        .build();
            } else {
                throw new UnsupportedOperationException(
                        String.format("%s is not a supported commitment type", commitmentData.type()));
            }

            commitmentAggregatesMap.put(commitmentAggregate.aggregateId(), commitmentAggregate);
        }

        /**
         * {@inheritDoc}l
         */
        @Override
        public Set<CloudCommitmentAggregate> getAggregates() {
            return ImmutableSet.copyOf(commitmentAggregatesMap.values());
        }
    }

    /**
     * The default implementation of {@link CloudCommitmentAggregatorFactory}.
     */
    public static class DefaultCloudCommitmentAggregatorFactory implements CloudCommitmentAggregatorFactory {

        private final IdentityProvider identityProvider;

        private final ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory;

        private final BillingFamilyRetrieverFactory billingFamilyRetrieverFactory;

        private final CloudCommitmentTopologyFactory<TopologyEntityDTO> commitmentTopologyFactory;

        /**
         * Constructs a new aggregator factory.
         * @param identityProvider The identity provider, used to assign aggregate IDs.
         * @param computeTierFamilyResolverFactory A factory for {@link ComputeTierFamilyResolver} instances,
         *                                         used to determine family connections for size flexible
         *                                         commitments.
         * @param billingFamilyRetrieverFactory A factory for {@link BillingFamilyRetriever} instances,
         *                                      used to scope aggregates based on the associated billing
         *                                      family.
         * @param commitmentTopologyFactory A factory for {@link CloudCommitmentTopology} instances.
         */
        public DefaultCloudCommitmentAggregatorFactory(@Nonnull IdentityProvider identityProvider,
                                                       @Nonnull ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory,
                                                       @Nonnull BillingFamilyRetrieverFactory billingFamilyRetrieverFactory,
                                                       @Nonnull CloudCommitmentTopologyFactory<TopologyEntityDTO> commitmentTopologyFactory) {
            this.identityProvider = Objects.requireNonNull(identityProvider);
            this.computeTierFamilyResolverFactory = Objects.requireNonNull(computeTierFamilyResolverFactory);
            this.billingFamilyRetrieverFactory = Objects.requireNonNull(billingFamilyRetrieverFactory);
            this.commitmentTopologyFactory = Objects.requireNonNull(commitmentTopologyFactory);
        }

        /**
         * {@inheritDoc}l
         */
        @Override
        public CloudCommitmentAggregator newAggregator(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {

            Preconditions.checkNotNull(cloudTopology);

            final CloudCommitmentTopology commitmentTopology = commitmentTopologyFactory.createTopology(cloudTopology);

            return new DefaultCloudCommitmentAggregator(
                    identityProvider,
                    computeTierFamilyResolverFactory.createResolver(cloudTopology),
                    billingFamilyRetrieverFactory.newInstance(),
                    cloudTopology,
                    commitmentTopology);
        }

        /**
         * {@inheritDoc}l
         */
        @Override
        public CloudCommitmentAggregator newIdentityAggregator(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {

            Preconditions.checkNotNull(cloudTopology);

            final CloudCommitmentTopology commitmentTopology = commitmentTopologyFactory.createTopology(cloudTopology);

            return new DefaultIdentityAggregator(
                    identityProvider,
                    computeTierFamilyResolverFactory.createResolver(cloudTopology),
                    billingFamilyRetrieverFactory.newInstance(),
                    cloudTopology,
                    commitmentTopology);
        }
    }
}
