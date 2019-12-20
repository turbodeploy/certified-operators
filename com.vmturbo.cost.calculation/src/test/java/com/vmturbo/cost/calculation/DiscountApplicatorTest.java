package com.vmturbo.cost.calculation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.AccountLevelDiscount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.ServiceLevelDiscount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo.TierLevelDiscount;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class DiscountApplicatorTest {

    private final CloudTopology<TestEntityClass> topology =
            (CloudTopology<TestEntityClass>)mock(CloudTopology.class);

    private EntityInfoExtractor<TestEntityClass> infoExtractor =
            (EntityInfoExtractor<TestEntityClass>)mock(EntityInfoExtractor.class);

    private CloudCostData cloudCostData = mock(CloudCostData.class);

    private DiscountApplicatorFactory<TestEntityClass> factory = DiscountApplicator.newFactory();

    private final AccountPricingData accountPricingData = mock(AccountPricingData.class);

    private CloudCostData emptyCloudCostData = new CloudCostData(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
         Collections.emptyMap(), Collections.emptyMap());

    @Before
    public void setup() {
        when(cloudCostData.getAccountPricingData(anyLong())).thenReturn(Optional.ofNullable(accountPricingData));
        when(cloudCostData.getAccountPricingData(anyLong()).get().getDiscountApplicator()).thenReturn(null);
        when(topology.getOwner(anyLong())).thenReturn(Optional.empty());
    }

    @Test
    public void testFactoryEntityOneOwner() {
        final TestEntityClass entity = TestEntityClass.newBuilder(7)
                .build(infoExtractor);

        final TestEntityClass owner = TestEntityClass.newBuilder(1)
                .setType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .build(infoExtractor);

        when(topology.getOwner(entity.getId())).thenReturn(Optional.of(owner));
        final Discount discount = Discount.newBuilder()
                .setId(100)
                .build();
        when(topology.getEntity(entity.getId())).thenReturn(Optional.of(entity));
        when(topology.getEntity(owner.getId())).thenReturn(Optional.of(owner));
        final DiscountApplicator<TestEntityClass> applicator =
                factory.accountDiscountApplicator(owner.getId(), topology, infoExtractor, Optional.ofNullable(discount));
        assertThat(applicator.getDiscount(), is(discount));
    }

    @Test
    public void testFactoryEntityOwnerChain() {
        final TestEntityClass entity = TestEntityClass.newBuilder(7)
                .build(infoExtractor);

        final TestEntityClass subAccount = TestEntityClass.newBuilder(1)
                .setType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .build(infoExtractor);

        final TestEntityClass masterAccount = TestEntityClass.newBuilder(3)
                .setType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .build(infoExtractor);

        when(topology.getOwner(entity.getId())).thenReturn(Optional.of(subAccount));
        when(topology.getOwner(subAccount.getId())).thenReturn(Optional.of(masterAccount));

        final Discount discount = Discount.newBuilder()
                .setId(100)
                .build();

        when(topology.getEntity(entity.getId())).thenReturn(Optional.of(entity));
        when(topology.getEntity(masterAccount.getId())).thenReturn(Optional.of(masterAccount));
        final DiscountApplicator<TestEntityClass> applicator =
                factory.accountDiscountApplicator(masterAccount.getId(), topology, infoExtractor, Optional.of(discount));
        assertThat(applicator.getDiscount(), is(discount));
    }

    @Test
    public void testFactoryEntityNoDiscount() {
        final TestEntityClass entity = TestEntityClass.newBuilder(7)
                .build(infoExtractor);

        final TestEntityClass owner = TestEntityClass.newBuilder(1)
                .setType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .build(infoExtractor);

        when(topology.getEntity(entity.getId())).thenReturn(Optional.of(entity));
        when(topology.getOwner(entity.getId())).thenReturn(Optional.of(owner));
        Optional<Discount> discount = Optional.empty();
        final DiscountApplicator<TestEntityClass> applicator =
                factory.accountDiscountApplicator(entity.getId(), topology, infoExtractor, discount);
        assertThat(applicator, is(DiscountApplicator.noDiscount()));
    }

    /**
     * Only one test for the account case, because it's a subset of the entity case.
     */
    @Test
    public void testFactoryAccount() {
        final TestEntityClass subAccount = TestEntityClass.newBuilder(1)
                .setType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .build(infoExtractor);

        final TestEntityClass masterAccount = TestEntityClass.newBuilder(3)
                .setType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .build(infoExtractor);

        when(topology.getEntity(subAccount.getId())).thenReturn(Optional.of(subAccount));
        when(topology.getOwner(subAccount.getId())).thenReturn(Optional.of(masterAccount));

        final Discount discount = Discount.newBuilder()
                .setId(100)
                .build();

        final DiscountApplicator<TestEntityClass> applicator =
                factory.accountDiscountApplicator(subAccount.getId(), topology, infoExtractor, Optional.ofNullable(discount));
        assertThat(applicator.getDiscount(), is(discount));
    }

    @Test
    public void testTierDiscount() {
        final TestEntityClass entity = TestEntityClass.newBuilder(7)
                .build(infoExtractor);
        final TestEntityClass tier = TestEntityClass.newBuilder(10)
                .setType(EntityType.COMPUTE_TIER_VALUE)
                .build(infoExtractor);
        final TestEntityClass service = TestEntityClass.newBuilder(11)
                .setType(EntityType.SERVICE_VALUE)
                .build(infoExtractor);
        when(topology.getConnectedService(tier.getId())).thenReturn(Optional.of(service));

        final double discountPercentage = 20;

        final Discount discount = Discount.newBuilder()
                .setId(100)
                .setDiscountInfo(DiscountInfo.newBuilder()
                    .setTierLevelDiscount(TierLevelDiscount.newBuilder()
                        .putDiscountPercentageByTierId(tier.getId(), discountPercentage))
                    .setAccountLevelDiscount(AccountLevelDiscount.newBuilder()
                        .setDiscountPercentage(50))
                        .setServiceLevelDiscount(ServiceLevelDiscount.newBuilder()
                                .putDiscountPercentageByServiceId(service.getId(), 70)))
                .build();
        final DiscountApplicator<TestEntityClass> applicator = makeDiscountApplicator(entity, discount);

        // The input to the applicator is the tier, because the applicator itself is entity-specific.
        assertThat(applicator.getDiscountPercentage(tier.getId()).getValue(), is(discountPercentage / 100));
    }

    @Test
    public void testServiceDiscount() {
        final TestEntityClass entity = TestEntityClass.newBuilder(7)
                .build(infoExtractor);
        final TestEntityClass tier = TestEntityClass.newBuilder(10)
                .setType(EntityType.COMPUTE_TIER_VALUE)
                .build(infoExtractor);
        final TestEntityClass service = TestEntityClass.newBuilder(11)
                .setType(EntityType.SERVICE_VALUE)
                .build(infoExtractor);
        when(topology.getConnectedService(tier.getId())).thenReturn(Optional.of(service));

        final double discountPercentage = 20;

        final Discount discount = Discount.newBuilder()
                .setId(100)
                .setDiscountInfo(DiscountInfo.newBuilder()
                        .setAccountLevelDiscount(AccountLevelDiscount.newBuilder()
                                .setDiscountPercentage(50))
                        .setServiceLevelDiscount(ServiceLevelDiscount.newBuilder()
                                .putDiscountPercentageByServiceId(service.getId(), discountPercentage)))
                .build();
        final DiscountApplicator<TestEntityClass> applicator = makeDiscountApplicator(entity, discount);

        // The input to the applicator is the tier, because the applicator itself is entity-specific.
        assertThat(applicator.getDiscountPercentage(tier.getId()).getValue(), is(discountPercentage / 100));
    }

    @Test
    public void testAccountDiscount() {
        final TestEntityClass entity = TestEntityClass.newBuilder(7)
                .build(infoExtractor);
        final TestEntityClass tier = TestEntityClass.newBuilder(10)
                .setType(EntityType.COMPUTE_TIER_VALUE)
                .build(infoExtractor);
        final TestEntityClass service = TestEntityClass.newBuilder(11)
                .setType(EntityType.SERVICE_VALUE)
                .build(infoExtractor);
        when(topology.getConnectedService(tier.getId())).thenReturn(Optional.of(service));

        final double discountPercentage = 20;

        final Discount discount = Discount.newBuilder()
                .setId(100)
                .setDiscountInfo(DiscountInfo.newBuilder()
                        .setAccountLevelDiscount(AccountLevelDiscount.newBuilder()
                                .setDiscountPercentage(discountPercentage)))
                .build();
        final DiscountApplicator<TestEntityClass> applicator = makeDiscountApplicator(entity, discount);

        // The input to the applicator is the tier, because the applicator itself is entity-specific.
        assertThat(applicator.getDiscountPercentage(tier.getId()).getValue(), is(discountPercentage / 100));
    }

    @Test
    public void testEmptyCloudCostData(){
        Assert.assertFalse(emptyCloudCostData.getRiCoverageForEntity(1L).isPresent());
        Assert.assertTrue(emptyCloudCostData.getCurrentRiCoverage().isEmpty());
        Assert.assertFalse(emptyCloudCostData.getExistingRiBoughtData(1L).isPresent());
        Assert.assertTrue(emptyCloudCostData.getExistingRiBought().isEmpty());
        Assert.assertTrue(emptyCloudCostData.getAllRiBought().isEmpty());
    }

    @Test
    public void testInvalidReservedInstanceData(){
        final ReservedInstanceData reservedInstanceData =
            new ReservedInstanceData(ReservedInstanceBought.getDefaultInstance(), ReservedInstanceSpec.getDefaultInstance());
        Assert.assertFalse(reservedInstanceData.isValid(new HashMap<>()));
    }

    @Test
    public void testNoDiscount() {
        final TestEntityClass entity = TestEntityClass.newBuilder(7)
                .build(infoExtractor);
        final TestEntityClass tier = TestEntityClass.newBuilder(10)
                .setType(EntityType.COMPUTE_TIER_VALUE)
                .build(infoExtractor);
        final TestEntityClass service = TestEntityClass.newBuilder(11)
                .setType(EntityType.SERVICE_VALUE)
                .build(infoExtractor);
        when(topology.getConnectedService(tier.getId())).thenReturn(Optional.of(service));

        final Discount discount = Discount.newBuilder()
                .setId(100)
                .setDiscountInfo(DiscountInfo.newBuilder())
                .build();
        final DiscountApplicator<TestEntityClass> applicator = makeDiscountApplicator(entity, discount);

        // The input to the applicator is the tier, because the applicator itself is entity-specific.
        assertThat(applicator.getDiscountPercentage(tier.getId()), is(DiscountApplicator.NO_DISCOUNT));
    }

    @Nonnull
    private DiscountApplicator<TestEntityClass> makeDiscountApplicator(final TestEntityClass entity, final Discount discount) {
        final TestEntityClass owner = TestEntityClass.newBuilder(1)
                .setType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .build(infoExtractor);

        when(topology.getOwner(entity.getId())).thenReturn(Optional.of(owner));
        when(topology.getEntity(entity.getId())).thenReturn(Optional.of(entity));

        final DiscountApplicator<TestEntityClass> applicator =
                factory.accountDiscountApplicator(entity.getId(), topology, infoExtractor, Optional.ofNullable(discount));
        assertThat(applicator.getDiscount(), is(discount));
        return applicator;
    }
}
