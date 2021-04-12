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
import com.vmturbo.cloud.common.commitment.ReservedInstanceData;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregateInfo.PlatformInfo;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregateInfo.TierInfo;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.topology.BillingFamilyRetriever;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * The default implementation of {@link CloudCommitmentAggregator}.
 */
public class DefaultCloudCommitmentAggregator implements CloudCommitmentAggregator {

    private final Logger logger = LogManager.getLogger();

    private final IdentityProvider identityProvider;

    private final ComputeTierFamilyResolver computeTierFamilyResolver;

    private final BillingFamilyRetriever billingFamilyRetriever;

    private final ConcurrentMap<AggregateInfo, Set<CloudCommitmentData>> commitmentAggregateMap =
            new ConcurrentHashMap<>();

    private DefaultCloudCommitmentAggregator(@Nonnull IdentityProvider identityProvider,
                                             @Nonnull ComputeTierFamilyResolver computeTierFamilyResolver,
                                             @Nonnull BillingFamilyRetriever billingFamilyRetriever) {
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.computeTierFamilyResolver = Objects.requireNonNull(computeTierFamilyResolver);
        this.billingFamilyRetriever = Objects.requireNonNull(billingFamilyRetriever);
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
            final AggregateInfo aggregateInfo;
            if (commitmentData.type() == CloudCommitmentType.RESERVED_INSTANCE) {
                aggregateInfo = buildRIAggregateInfo(commitmentData.asReservedInstance());
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
                        final AggregateInfo aggregateInfo = aggregateEntry.getKey();
                        if (aggregateInfo.commitmentType() == CloudCommitmentType.RESERVED_INSTANCE) {
                            return ReservedInstanceAggregate.builder()
                                    .aggregateId(identityProvider.next())
                                    .aggregateInfo((ReservedInstanceAggregateInfo)aggregateInfo)
                                    // There's an assumption that all aggregates under the aggregate
                                    // info conform to the info's type.
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


    protected ReservedInstanceAggregateInfo buildRIAggregateInfo(@Nonnull ReservedInstanceData commitmentData)
            throws AggregationFailureException {

        try {
            final ReservedInstanceBoughtInfo riBoughtInfo = commitmentData.commitment().getReservedInstanceBoughtInfo();
            final ReservedInstanceScopeInfo scopeInfo = riBoughtInfo.getReservedInstanceScopeInfo();
            final ReservedInstanceSpecInfo riSpecInfo = commitmentData.spec().getReservedInstanceSpecInfo();

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

            return ReservedInstanceAggregateInfo.builder()
                    .billingFamilyId(billingFamilyId)
                    .platformInfo(PlatformInfo.builder()
                            .isPlatformFlexible(riSpecInfo.getPlatformFlexible())
                            // platform will be ignored if the RI is platform flexible
                            .platform(riSpecInfo.getOs())
                            .build())
                    .scopeInfo(ReservedInstanceScopeInfo.newBuilder()
                            .setShared(riBoughtInfo.getReservedInstanceScopeInfo().getShared())
                            .addAllApplicableBusinessAccountId(
                                    riBoughtInfo.getReservedInstanceScopeInfo().getApplicableBusinessAccountIdList())
                            .build())
                    .tierInfo(TierInfo.builder()
                            .tierType(EntityType.COMPUTE_TIER)
                            .tierFamily(computeTierFamilyResolver.getCoverageFamily(riSpecInfo.getTierId()))
                            .tierOid(riSpecInfo.getTierId())
                            .isSizeFlexible(riSpecInfo.getSizeFlexible())
                            .build())
                    .regionOid(riSpecInfo.getRegionId())
                    .zoneOid(riBoughtInfo.hasAvailabilityZoneId()
                            ? OptionalLong.of(riBoughtInfo.getAvailabilityZoneId())
                            : OptionalLong.empty())
                    .purchasingAccountOid(purchaseAccountScope)
                    .tenancy(riSpecInfo.hasTenancy() ? riSpecInfo.getTenancy() : Tenancy.DEFAULT)
                    .build();
        } catch (Exception e) {
            // This may happen if billing family or compute tier retrieval fails
            logger.error("Error aggregating cloud commitment (OID={}, Type={})",
                    commitmentData.commitmentId(), commitmentData.type(), e);
            throw new AggregationFailureException(e);
        }
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
                                          @Nonnull BillingFamilyRetriever billingFamilyRetriever) {
            super(identityProvider, computeTierFamilyResolver, billingFamilyRetriever);
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
                        .aggregateInfo(buildRIAggregateInfo(commitmentData.asReservedInstance()))
                        .addCommitments(commitmentData.asReservedInstance())
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

        /**
         * Constructs a new aggregator factory.
         * @param identityProvider The identity provider, used to assign aggregate IDs.
         * @param computeTierFamilyResolverFactory A factory for {@link ComputeTierFamilyResolver} instances,
         *                                         used to determine family connections for size flexible
         *                                         commitments.
         * @param billingFamilyRetrieverFactory A factory for {@link BillingFamilyRetriever} instances,
         *                                      used to scope aggregates based on the associated billing
         *                                      family.
         */
        public DefaultCloudCommitmentAggregatorFactory(@Nonnull IdentityProvider identityProvider,
                                                       @Nonnull ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory,
                                                       @Nonnull BillingFamilyRetrieverFactory billingFamilyRetrieverFactory) {
            this.identityProvider = Objects.requireNonNull(identityProvider);
            this.computeTierFamilyResolverFactory = Objects.requireNonNull(computeTierFamilyResolverFactory);
            this.billingFamilyRetrieverFactory = Objects.requireNonNull(billingFamilyRetrieverFactory);
        }

        /**
         * {@inheritDoc}l
         */
        @Override
        public CloudCommitmentAggregator newAggregator(@Nonnull CloudTopology<TopologyEntityDTO> tierTopology) {

            Preconditions.checkNotNull(tierTopology);

            return new DefaultCloudCommitmentAggregator(
                    identityProvider,
                    computeTierFamilyResolverFactory.createResolver(tierTopology),
                    billingFamilyRetrieverFactory.newInstance());
        }

        /**
         * {@inheritDoc}l
         */
        @Override
        public CloudCommitmentAggregator newIdentityAggregator(@Nonnull CloudTopology<TopologyEntityDTO> tierTopology) {

            Preconditions.checkNotNull(tierTopology);

            return new DefaultIdentityAggregator(
                    identityProvider,
                    computeTierFamilyResolverFactory.createResolver(tierTopology),
                    billingFamilyRetrieverFactory.newInstance());
        }
    }
}
