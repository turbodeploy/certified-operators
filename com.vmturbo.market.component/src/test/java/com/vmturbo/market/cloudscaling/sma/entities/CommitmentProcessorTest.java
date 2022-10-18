package com.vmturbo.market.cloudscaling.sma.entities;

import static org.junit.Assert.assertFalse;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.cloud.common.commitment.TopologyCommitmentData;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.topology.conversions.ReservedInstanceKey;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentScope;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentStatus;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.FamilyRestricted;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Unit tests for {@link CommitmentProcessor}.
 */
public class CommitmentProcessorTest {

    private static final long REGION_ID = 1L;
    private static final long ZONE_ID = 2L;
    private static final SMACSP CSP = SMACSP.GCP;
    private static final long BUSINESS_ACCOUNT_ID = 3L;
    private static final long BILLING_FAMILY_ID = 4L;
    private static final SMATemplate TEMPLATE = SMAUtils.BOGUS_TEMPLATE;
    private static final ImmutableSet<Long> COVERED_ACCOUNTS = ImmutableSet.of(123L, 1234L, 12345L);
    private static final long KEY = 5L;
    private static final long COMMITMENT_ID = 6L;
    private static final String COMMITMENT_DISPLAY_NAME = "Commitment Name";
    private static final TopologyEntityDTO ACTIVE_COMMITMENT = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.CLOUD_COMMITMENT_VALUE)
            .setDisplayName(COMMITMENT_DISPLAY_NAME)
            .setOid(COMMITMENT_ID)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setCloudCommitmentData(CloudCommitmentInfo.newBuilder()
                            .setCommitmentStatus(CloudCommitmentStatus.CLOUD_COMMITMENT_STATUS_ACTIVE)
                            .setFamilyRestricted(
                                    FamilyRestricted.newBuilder().setInstanceFamily("N2"))
                            .setCommitmentScope(
                                    CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_ACCOUNT)))
            .build();

    /**
     * Test for {@link CommitmentProcessor#processCommitment}.
     */
    @Test
    public void testProcess() {

        final Optional<Pair<SMAContext, SMAReservedInstance>> result = processCommitment(ACTIVE_COMMITMENT);

        Assert.assertTrue(result.isPresent());
        final Pair<SMAContext, SMAReservedInstance> pair = result.get();
        final SMAContext context = pair.getFirst();
        final SMAReservedInstance instance = pair.getSecond();
        Assert.assertEquals(
                new SMAContext(SMACSP.GCP, SMAUtils.UNKNOWN_OS_TYPE_PLACEHOLDER, REGION_ID,
                        BILLING_FAMILY_ID, SMAUtils.UNKNOWN_TENANCY_PLACEHOLDER), context);
        Assert.assertEquals(COMMITMENT_ID, instance.getOid());
        Assert.assertEquals(KEY, instance.getRiKeyOid());
        Assert.assertEquals(COMMITMENT_DISPLAY_NAME, instance.getName());
        Assert.assertEquals(BUSINESS_ACCOUNT_ID, instance.getBusinessAccountId());
        Assert.assertSame(COVERED_ACCOUNTS, instance.getApplicableBusinessAccounts());
        Assert.assertSame(TEMPLATE, instance.getTemplate());
        Assert.assertTrue(instance.isIsf());
        assertFalse(instance.isShared());
        Assert.assertTrue(instance.isPlatformFlexible());
        Assert.assertEquals(ZONE_ID, instance.getZoneId());
    }

    /**
     * Test that an inactive commitment is skipped.
     */
    @Test
    public void testInactiveCommitment() {

        final TopologyEntityDTO inactiveCommitment = ACTIVE_COMMITMENT.toBuilder()
                .setTypeSpecificInfo(ACTIVE_COMMITMENT.getTypeSpecificInfo()
                        .toBuilder()
                        .setCloudCommitmentData(ACTIVE_COMMITMENT.getTypeSpecificInfo().getCloudCommitmentData()
                                .toBuilder()
                                .setCommitmentStatus(CloudCommitmentStatus.CLOUD_COMMITMENT_STATUS_UNKNOWN)))
                .build();

        final Optional<Pair<SMAContext, SMAReservedInstance>> result = processCommitment(inactiveCommitment);

        assertFalse(result.isPresent());
    }

    private Optional<Pair<SMAContext, SMAReservedInstance>> processCommitment(TopologyEntityDTO commitment) {

        final LongUnaryOperator regionByCommitmentGetter = Mockito.mock(LongUnaryOperator.class);
        final LongFunction<Optional<Long>> zoneByEntityGetter = Mockito.mock(LongFunction.class);
        final LongFunction<SMACSP> cspByRegionResolver = Mockito.mock(LongFunction.class);
        final LongUnaryOperator businessAccountByCommitmentGetter = Mockito.mock(
                LongUnaryOperator.class);
        final LongUnaryOperator billingFamilyByAccountGetter = Mockito.mock(
                LongUnaryOperator.class);
        final Function<String, Optional<SMATemplate>>
                templateByFamilyAndContextGetter = Mockito.mock(Function.class);
        final LongFunction<Optional<Set<Long>>> coveredAccountsByCommitmentResolver = Mockito.mock(
                LongFunction.class);
        final BiFunction<ReservedInstanceKey, Long, Long> reservedInstanceKeyIDGenerator =
                Mockito.mock(BiFunction.class);

        Mockito.when(regionByCommitmentGetter.applyAsLong(commitment.getOid())).thenReturn(REGION_ID);
        Mockito.when(zoneByEntityGetter.apply(commitment.getOid())).thenReturn(Optional.of(ZONE_ID));
        Mockito.when(cspByRegionResolver.apply(REGION_ID)).thenReturn(CSP);
        Mockito.when(businessAccountByCommitmentGetter.applyAsLong(commitment.getOid())).thenReturn(
                BUSINESS_ACCOUNT_ID);
        Mockito.when(billingFamilyByAccountGetter.applyAsLong(BUSINESS_ACCOUNT_ID)).thenReturn(
                BILLING_FAMILY_ID);
        Mockito.when(templateByFamilyAndContextGetter.apply(Mockito.eq("N2"))).thenReturn(Optional.of(TEMPLATE));
        Mockito.when(coveredAccountsByCommitmentResolver.apply(commitment.getOid())).thenReturn(
                Optional.of(COVERED_ACCOUNTS));
        Mockito.when(reservedInstanceKeyIDGenerator.apply(Mockito.any(ReservedInstanceKey.class),
                Mockito.eq(commitment.getOid()))).thenReturn(KEY);

        return CommitmentProcessor.processCommitment(regionByCommitmentGetter, zoneByEntityGetter,
                        cspByRegionResolver, businessAccountByCommitmentGetter,
                        billingFamilyByAccountGetter, templateByFamilyAndContextGetter,
                        coveredAccountsByCommitmentResolver, reservedInstanceKeyIDGenerator,
                        TopologyCommitmentData.builder().commitment(commitment).build());
    }
}