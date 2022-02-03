package com.vmturbo.cost.component.reserved.instance.coverage.analysis;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;

import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.vmturbo.cloud.common.commitment.CloudCommitmentUtils;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.CloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.topology.BillingFamilyRetriever;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage.RICoverageSource;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.AccountRIMappingStore;
import com.vmturbo.cost.component.reserved.instance.AccountRIMappingStore.AccountRIMappingItem;
import com.vmturbo.cost.component.reserved.instance.EntityReservedInstanceMappingStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceCostCalculator;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.SQLReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageAllocation;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageAllocator;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

public class SupplementalCoverageAnalysisTest {

    private final CoverageAllocatorFactory allocatorFactory =
            mock(CoverageAllocatorFactory.class);

    private final CoverageTopologyFactory coverageTopologyFactory =
            mock(CoverageTopologyFactory.class);


    private final DSLContext dsl = Mockito.mock(DSLContext.class);
    private final IdentityProvider identityProvider = Mockito.mock(IdentityProvider.class);
    private final ReservedInstanceCostCalculator reservedInstanceCostCalculato =
            Mockito.mock(ReservedInstanceCostCalculator.class);
    private final PriceTableStore priceTableStore = Mockito.mock(PriceTableStore.class);
    private final EntityReservedInstanceMappingStore reservedInstanceMappingStore =
            mock(EntityReservedInstanceMappingStore.class);
    private final AccountRIMappingStore accountMappingStore = mock(AccountRIMappingStore.class);
    private final BusinessAccountHelper businessAccountHelper = mock(BusinessAccountHelper.class);
    private final ReservedInstanceBoughtStore riBoughtStore =
            new SQLReservedInstanceBoughtStore(dsl, identityProvider, reservedInstanceCostCalculato,
                    priceTableStore, reservedInstanceMappingStore, accountMappingStore, businessAccountHelper);
    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore = spy(riBoughtStore);

    private final ReservedInstanceSpecStore reservedInstanceSpecStore =
            mock(ReservedInstanceSpecStore.class);

    private CloudCommitmentAggregatorFactory cloudCommitmentAggregatorFactory;

    private final CloudCommitmentCoverageAllocator riCoverageAllocator =
            mock(CloudCommitmentCoverageAllocator.class);
    private final CoverageTopology coverageTopology = mock(CoverageTopology.class);



    @Before
    public void setup() {
        when(allocatorFactory.createAllocator(any())).thenReturn(riCoverageAllocator);
        when(businessAccountHelper.getDiscoveredBusinessAccounts()).thenReturn(ImmutableSet.of(1L));

        final BillingFamilyRetriever billingFamilyRetriever = mock(BillingFamilyRetriever.class);
        final BillingFamilyRetrieverFactory billingFamilyRetrieverFactory = mock(BillingFamilyRetrieverFactory.class);
        when(billingFamilyRetrieverFactory.newInstance()).thenReturn(billingFamilyRetriever);


        final CloudCommitmentAggregator cloudCommitmentAggregator = mock(CloudCommitmentAggregator.class);
        when(cloudCommitmentAggregator.getAggregates()).thenReturn(Collections.emptySet());
        cloudCommitmentAggregatorFactory = mock(CloudCommitmentAggregatorFactory.class);
        when(cloudCommitmentAggregatorFactory.newIdentityAggregator(any())).thenReturn(cloudCommitmentAggregator);
    }

    @Test
    public void testCreateCoverageRecordsFromSupplementalAllocation() {

        TopologyEntityDTO discoveredBA = TopologyEntityDTO.newBuilder()
                                    .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                                    .setOid(1)
                                    .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                                            .setBusinessAccount(BusinessAccountInfo.newBuilder()
                                                    .setAccountId("1")
                                                    .setAssociatedTargetId(1).build()))
                                    .build();
        TopologyEntityDTO unDiscoveredBA = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(2)
                .build();


        // setup billing coverage records
        final List<EntityRICoverageUpload> entityRICoverageUploads = ImmutableList.of(
                EntityRICoverageUpload.newBuilder()
                        .setEntityId(1)
                        .setTotalCouponsRequired(4)
                        .addCoverage(
                                Coverage.newBuilder()
                                        .setReservedInstanceId(4)
                                        .setCoveredCoupons(2)
                                        .setRiCoverageSource(RICoverageSource.BILLING)
                                        .build())
                        .build(),
                EntityRICoverageUpload.newBuilder()
                        .setEntityId(2)
                        .setTotalCouponsRequired(4)
                        .addCoverage(
                                Coverage.newBuilder()
                                        .setReservedInstanceId(5)
                                        .setCoveredCoupons(4)
                                        .setRiCoverageSource(RICoverageSource.BILLING)
                                        .build())
                        .build());

        // setup RI allocator output
        final CloudCommitmentCoverageAllocation coverageAllocation = CloudCommitmentCoverageAllocation.from(
                // total coverage
                ImmutableTable.<Long, Long, CloudCommitmentAmount>builder()
                        .put(1L, 4L, CloudCommitmentAmount.newBuilder().setCoupons(4.0).build())
                        .put(2L, 5L, CloudCommitmentAmount.newBuilder().setCoupons(4.0).build())
                        .put(3L, 6L, CloudCommitmentAmount.newBuilder().setCoupons(4.0).build())
                        .build(),
                // supplemental allocations
                ImmutableTable.<Long, Long, CloudCommitmentAmount>builder()
                        .put(1L, 4L, CloudCommitmentAmount.newBuilder().setCoupons(2.0).build())
                        .put(3L, 6L, CloudCommitmentAmount.newBuilder().setCoupons(4.0).build())
                        .build());
        when(riCoverageAllocator.allocateCoverage()).thenReturn(coverageAllocation);

        // setup coverage topology
        // Entity ID 3 will require resolution of the coverage capacity
        when(coverageTopology.getCoverageCapacityForEntity(3L, CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO))
                .thenReturn(8.0);

        // set up undiscovered usage
        AccountRIMappingItem item = mock(AccountRIMappingItem.class);
        when(item.getUsedCoupons()).thenReturn(2d);
        when(item.getReservedInstanceId()).thenReturn(1L);
        when(item.getBusinessAccountOid()).thenReturn(1L);

        final Map<Long, List<AccountRIMappingItem>> riAccountMappings = ImmutableMap.of(
                1L, ImmutableList.of(item));
        when(accountMappingStore.getAccountRICoverageMappings(anyList()))
                .thenReturn(riAccountMappings);
        when(accountMappingStore.getUndiscoveredAccountUsageForRI())
                .thenReturn(ImmutableMap.of(1L, 2d));

        doReturn(ImmutableMap.of(3L, 1d))
                .when(reservedInstanceMappingStore).getReservedInstanceUsedCouponsMapByFilter(any());

        /*ReservedInstanceBoughtRpcServiceTest.java
        Setup Factory
         */

        // 4,6 - RIS from discovered accounts. 5 - RI from undiscovered account.
        ReservedInstanceBought discoveredRI1 = ReservedInstanceBought.newBuilder()
                .setId(4)
                .setReservedInstanceBoughtInfo(
                        ReservedInstanceBoughtInfo.newBuilder()
                                .setBusinessAccountId(discoveredBA.getOid())
                                .setReservedInstanceBoughtCoupons(
                                        ReservedInstanceBoughtCoupons.newBuilder()
                                                .setNumberOfCoupons(10)
                                                .build())
                                .build())
                .build();
        ReservedInstanceBought discoveredRI2 = ReservedInstanceBought.newBuilder()
                .setId(6)
                .setReservedInstanceBoughtInfo(
                        ReservedInstanceBoughtInfo.newBuilder()
                                .setBusinessAccountId(discoveredBA.getOid())
                                .setReservedInstanceBoughtCoupons(
                                        ReservedInstanceBoughtCoupons.newBuilder()
                                                .setNumberOfCoupons(10)
                                                .setNumberOfCouponsUsed(1)
                                                .build())
                                .build())
                .build();
        ReservedInstanceBought undiscoveredRI = ReservedInstanceBought.newBuilder()
                .setId(5)
                .setReservedInstanceBoughtInfo(
                        ReservedInstanceBoughtInfo.newBuilder()
                                .setBusinessAccountId(unDiscoveredBA.getOid())
                                .setReservedInstanceBoughtCoupons(
                                        ReservedInstanceBoughtCoupons.newBuilder()
                                                .setNumberOfCoupons(10)
                                                .setNumberOfCouponsUsed(1)
                                                .build())
                                .build())
                .build();
        final List<ReservedInstanceBought> reservedInstances = ImmutableList.of(
                discoveredRI1, discoveredRI2, undiscoveredRI);

        final List<ReservedInstanceSpec> riSpecs = ImmutableList.of(
                ReservedInstanceSpec.newBuilder()
                        .setId(3)
                        .build(),
                ReservedInstanceSpec.newBuilder()
                        .setId(4)
                        .build());
        final CloudTopology cloudTopology = mock(CloudTopology.class);
        when(cloudTopology.getAllEntitiesOfType(EntityType.BUSINESS_ACCOUNT_VALUE)).thenReturn(
                ImmutableList.of(discoveredBA, unDiscoveredBA));

        /*
        Setup mocks for factory
         */
        doReturn(reservedInstances).when(reservedInstanceBoughtStore)
            .getReservedInstanceBoughtByFilter(Matchers.any(ReservedInstanceBoughtFilter.class));
        doReturn(Collections.singletonList(undiscoveredRI)).when(reservedInstanceBoughtStore)
                .getUndiscoveredReservedInstances();


        when(reservedInstanceSpecStore.getReservedInstanceSpecByIds(any()))
                .thenReturn(riSpecs);
        when(coverageTopologyFactory.createCoverageTopology(
                eq(cloudTopology),
                any())).thenReturn(coverageTopology);

        /*
        Setup SUT
         */
        final SupplementalCoverageAnalysisFactory factory =
                new SupplementalCoverageAnalysisFactory(
                        allocatorFactory,
                        coverageTopologyFactory,
                        reservedInstanceBoughtStore,
                        reservedInstanceSpecStore,
                        cloudCommitmentAggregatorFactory,
                        true,
                        false);

        /*
        Invoke SUT
         */
        final SupplementalCoverageAnalysis coverageAnalysis = factory.createCoverageAnalysis(
                cloudTopology,
                entityRICoverageUploads);
        final List<EntityRICoverageUpload> aggregateRICoverages =
                coverageAnalysis.createCoverageRecordsFromSupplementalAllocation();


        /*
        Expected results
         */
        final List<EntityRICoverageUpload> expectedAggregateRICoverages = ImmutableList.of(
                EntityRICoverageUpload.newBuilder()
                        .setEntityId(1)
                        .setTotalCouponsRequired(4)
                        .addCoverage(
                                Coverage.newBuilder()
                                        .setReservedInstanceId(4)
                                        .setCoveredCoupons(2)
                                        .setRiCoverageSource(RICoverageSource.BILLING)
                                        .build())
                        .addCoverage(
                                Coverage.newBuilder()
                                        .setReservedInstanceId(4)
                                        .setCoveredCoupons(2)
                                        .setRiCoverageSource(RICoverageSource.SUPPLEMENTAL_COVERAGE_ALLOCATION)
                                        .build())
                        .build(),
                EntityRICoverageUpload.newBuilder()
                        .setEntityId(2)
                        .setTotalCouponsRequired(4)
                        .addCoverage(
                                Coverage.newBuilder()
                                        .setReservedInstanceId(5)
                                        .setCoveredCoupons(4)
                                        .setRiCoverageSource(RICoverageSource.BILLING)
                                        .build())
                        .build(),
                EntityRICoverageUpload.newBuilder()
                        .setEntityId(3)
                        .setTotalCouponsRequired(8L)
                        .addCoverage(
                                Coverage.newBuilder()
                                        .setReservedInstanceId(6)
                                        .setCoveredCoupons(4)
                                        .setRiCoverageSource(RICoverageSource.SUPPLEMENTAL_COVERAGE_ALLOCATION)
                                        .build())
                        .build());

        /*
        Assertions
         */
        assertThat(aggregateRICoverages, iterableWithSize(3));

        for (EntityRICoverageUpload expectedCoverageUpload: expectedAggregateRICoverages) {
            Optional<EntityRICoverageUpload> actualCoverageUpload =
                    aggregateRICoverages.stream().filter(c -> expectedCoverageUpload.getEntityId()
                            == c.getEntityId()).findFirst();
            Assert.assertTrue(actualCoverageUpload.isPresent());
            Assert.assertTrue(actualCoverageUpload.get().getCoverageList().size()
                    == expectedCoverageUpload.getCoverageList().size());
            for (Coverage expectedCoverage: expectedCoverageUpload.getCoverageList()) {
                Optional<Coverage> actualCoverageOpt = actualCoverageUpload.get().getCoverageList()
                        .stream().filter(
                                c -> c.getReservedInstanceId() == expectedCoverage.getReservedInstanceId()
                                && c.getRiCoverageSource() == expectedCoverage.getRiCoverageSource())
                        .findFirst();
                Assert.assertTrue(actualCoverageOpt.isPresent());
                Coverage actualCoverage = actualCoverageOpt.get();
                if (expectedCoverage.getRiCoverageSource() == RICoverageSource.SUPPLEMENTAL_COVERAGE_ALLOCATION) {
                    Assert.assertTrue(actualCoverage.hasUsageStartTimestamp());
                    Assert.assertTrue(actualCoverage.hasUsageEndTimestamp());
                }
                actualCoverage = actualCoverage.toBuilder().clearUsageStartTimestamp().clearUsageEndTimestamp().build();
                Assert.assertEquals(expectedCoverage, actualCoverage);
            }
        }

    }
}
