package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.topology.conversions.ReservedInstanceKey;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentScope;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.FamilyRestricted;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Commitment processor.
 */
public class CommitmentProcessor {

    private static final Logger logger = LogManager.getLogger();

    private CommitmentProcessor() {}

    /**
     * Process commitment.
     *
     * @param regionByCommitmentGetter the region by commitment getter.
     * @param zoneByCommitmentGetter the zone by commitment getter.
     * @param cspByRegionResolver the CSP by region resolver.
     * @param businessAccountByCommitmentGetter the business account by commitment getter.
     * @param billingFamilyByAccountGetter the billing family by account getter.
     * @param templateByFamilyAndContextGetter the template by family and context
     *         getter.
     * @param coveredAccountsByCommitmentResolver the covered accounts by commitment
     *         resolver.
     * @param reservedInstanceKeyIDGenerator the key ID generator.
     * @param data the commitment data.
     * @return the optional pair of context and reserved instance.
     */
    public static Optional<Pair<SMAContext, SMAReservedInstance>> processCommitment(
            @Nonnull final LongUnaryOperator regionByCommitmentGetter,
            @Nonnull final LongFunction<Optional<Long>> zoneByCommitmentGetter,
            @Nonnull final LongFunction<SMACSP> cspByRegionResolver,
            @Nonnull final LongUnaryOperator businessAccountByCommitmentGetter,
            @Nonnull final LongUnaryOperator billingFamilyByAccountGetter,
            @Nonnull final Function<String, Optional<SMATemplate>> templateByFamilyAndContextGetter,
            @Nonnull final LongFunction<Optional<Set<Long>>> coveredAccountsByCommitmentResolver,
            @Nonnull final BiFunction<ReservedInstanceKey, Long, Long> reservedInstanceKeyIDGenerator,
            @Nonnull final CloudCommitmentData<TopologyEntityDTO> data) {
        final TopologyEntityDTO commitment = data.commitment();
        final long commitmentId = data.commitmentId();
        final CloudCommitmentInfo cloudCommitmentData =
                commitment.getTypeSpecificInfo().getCloudCommitmentData();
        final FamilyRestricted familyRestricted = cloudCommitmentData.getFamilyRestricted();

        if (!familyRestricted.hasInstanceFamily()) {
            logger.info("Skip commitment {} with unsupported scope", commitmentId);
            return Optional.empty();
        }

        final long regionId = regionByCommitmentGetter.applyAsLong(commitmentId);
        final SMACSP csp = cspByRegionResolver.apply(regionId);
        final long accountId = businessAccountByCommitmentGetter.applyAsLong(commitmentId);
        final long billingFamilyId = billingFamilyByAccountGetter.applyAsLong(accountId);
        final OSType osType = SMAUtils.UNKNOWN_OS_TYPE_PLACEHOLDER;
        final Tenancy tenancy = SMAUtils.UNKNOWN_TENANCY_PLACEHOLDER;
        final String family = familyRestricted.getInstanceFamily();
        final SMAContext context = new SMAContext(csp, osType, regionId, billingFamilyId, tenancy);

        return templateByFamilyAndContextGetter.apply(family).flatMap(
                template -> processCommitment(zoneByCommitmentGetter, coveredAccountsByCommitmentResolver,
                        reservedInstanceKeyIDGenerator, data, commitment, commitmentId,
                        cloudCommitmentData, regionId, accountId, osType, tenancy, family, context,
                        template));
    }

    @Nonnull
    private static Optional<Pair<SMAContext, SMAReservedInstance>> processCommitment(
            @Nonnull final LongFunction<Optional<Long>> zoneByEntityGetter,
            @Nonnull final LongFunction<Optional<Set<Long>>> coveredAccountsByCommitmentResolver,
            @Nonnull final BiFunction<ReservedInstanceKey, Long, Long> reservedInstanceKeyIDGenerator,
            @Nonnull final CloudCommitmentData<TopologyEntityDTO> data,
            @Nonnull final TopologyEntityDTO commitment, final long commitmentId,
            @Nonnull final CloudCommitmentInfo cloudCommitmentData, final long regionId,
            final long accountId, @Nonnull final OSType osType, @Nonnull final Tenancy tenancy,
            @Nonnull final String family, @Nonnull final SMAContext context,
            @Nonnull final SMATemplate template) {
        final CloudCommitmentScope commitmentScope = cloudCommitmentData.getCommitmentScope();

        final Set<Long> scopedAccounts =
                commitmentScope == CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT
                        ? coveredAccountsByCommitmentResolver.apply(commitmentId).orElse(
                        Collections.emptySet()) : Collections.emptySet();

        final boolean shared =
                commitmentScope == CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_BILLING_FAMILY_GROUP;

        final boolean instanceSizeFlexible = true;

        final boolean platformFlexible = true;

        final long zoneId = zoneByEntityGetter.apply(commitmentId).orElse(SMAUtils.NO_ZONE);

        final ReservedInstanceKey key = new ReservedInstanceKey(tenancy, osType, regionId, zoneId,
                family, template.getOid(), instanceSizeFlexible, platformFlexible,
                shared ? Collections.singleton(context.getBillingFamilyId()) : scopedAccounts,
                shared);

        final long keyId = reservedInstanceKeyIDGenerator.apply(key, commitmentId);

        final SMAReservedInstance ri = new SMAReservedInstance(commitmentId, keyId,
                commitment.getDisplayName(), accountId, scopedAccounts, template, zoneId, 1,
                instanceSizeFlexible, shared, platformFlexible, data.capacity());

        return Optional.of(Pair.create(context, ri));
    }
}
