package com.vmturbo.cost.component.reserved.instance.coverage.analysis;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

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
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.identity.IdentityProvider;
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
import com.vmturbo.reserved.instance.coverage.allocator.RICoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageAllocation;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageAllocator;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

public class SupplementalRICoverageAnalysisTest {

    private final RICoverageAllocatorFactory allocatorFactory =
            mock(RICoverageAllocatorFactory.class);

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

    private final ReservedInstanceCoverageAllocator riCoverageAllocator =
            mock(ReservedInstanceCoverageAllocator.class);
    private final CoverageTopology coverageTopology = mock(CoverageTopology.class);



    @Before
    public void setup() {
        when(allocatorFactory.createAllocator(any())).thenReturn(riCoverageAllocator);
        when(businessAccountHelper.getDiscoveredBusinessAccounts()).thenReturn(ImmutableSet.of(1L));
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
        final ReservedInstanceCoverageAllocation coverageAllocation = ReservedInstanceCoverageAllocation.from(
                // total coverage
                ImmutableTable.<Long, Long, Double>builder()
                        .put(1L, 4L, 4.0)
                        .put(2L, 5L, 4.0)
                        .put(3L, 6L, 4.0)
                        .build(),
                // supplemental allocations
                ImmutableTable.<Long, Long, Double>builder()
                        .put(1L, 4L, 2.0)
                        .put(3L, 6L, 4.0)
                        .build());
        when(riCoverageAllocator.allocateCoverage()).thenReturn(coverageAllocation);

        // setup coverage topology
        // Entity ID 3 will require resolution of the coverage capacity
        when(coverageTopology.getRICoverageCapacityForEntity(eq(3L))).thenReturn(8L);

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

        final List<ReservedInstanceBought> reservedInstances = ImmutableList.of(
                ReservedInstanceBought.newBuilder()
                        .setId(1)
                        .setReservedInstanceBoughtInfo(
                                ReservedInstanceBoughtInfo.newBuilder()
                                .setBusinessAccountId(1)
                                .setReservedInstanceBoughtCoupons(
                                        ReservedInstanceBoughtCoupons.newBuilder()
                                        .setNumberOfCoupons(10)
                                        .build())
                                .build())
                        .build(),
                ReservedInstanceBought.newBuilder()
                        .setId(2)
                        .setReservedInstanceBoughtInfo(
                                ReservedInstanceBoughtInfo.newBuilder()
                                        .setBusinessAccountId(1).build())
                        .build(),
                ReservedInstanceBought.newBuilder()
                    .setId(3)
                    .setReservedInstanceBoughtInfo(
                            ReservedInstanceBoughtInfo.newBuilder()
                                .setBusinessAccountId(2)
                                .setReservedInstanceBoughtCoupons(
                                    ReservedInstanceBoughtCoupons.newBuilder()
                                    .setNumberOfCoupons(10)
                                    .setNumberOfCouponsUsed(1)
                                    .build())
                                .build())
                    .build());

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
            .getReservedInstanceBoughtByFilter(
                eq(ReservedInstanceBoughtFilter.SELECT_ALL_FILTER));

        when(reservedInstanceSpecStore.getReservedInstanceSpecByIds(any()))
                .thenReturn(riSpecs);
        when(coverageTopologyFactory.createCoverageTopology(
                eq(cloudTopology),
                eq(riSpecs),
                argThat(new ArgumentMatcher<List<ReservedInstanceBought>>() {
                    @Override
                    public boolean matches(final Object o) {
                        List<ReservedInstanceBought> receivedList = (List<ReservedInstanceBought>)o;
                        if (receivedList.size() > 3) {
                            return false;
                        }
                        // verify there is one undiscovered RI
                        Optional<ReservedInstanceBought> unDiscoveredRIs = receivedList.stream()
                                .filter(ri -> ri.getReservedInstanceBoughtInfo().getBusinessAccountId() == unDiscoveredBA.getOid())
                                .findFirst();
                        if (!unDiscoveredRIs.isPresent()
                            ||  unDiscoveredRIs.get()
                                .getReservedInstanceBoughtInfo()
                                .getReservedInstanceBoughtCoupons().getNumberOfCoupons() != 1) {
                            return false;
                        }
                        // verify the discovered RI coupons
                        Optional<ReservedInstanceBought> discoveredRI = receivedList.stream()
                                .filter(ri -> ri.getId() == 1)
                                .findAny();
                        if (!discoveredRI.isPresent()
                            || discoveredRI.get()
                                .getReservedInstanceBoughtInfo()
                                .getReservedInstanceBoughtCoupons().getNumberOfCoupons() != 8) {
                            return false;
                        }
                        return true;
                    }
                }))).thenReturn(coverageTopology);

        /*
        Setup SUT
         */
        final SupplementalRICoverageAnalysisFactory factory =
                new SupplementalRICoverageAnalysisFactory(
                        allocatorFactory,
                        coverageTopologyFactory,
                        reservedInstanceBoughtStore,
                        reservedInstanceSpecStore,
                        true,
                        false,
                        accountMappingStore);

        /*
        Invoke SUT
         */
        final SupplementalRICoverageAnalysis coverageAnalysis = factory.createCoverageAnalysis(
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
        assertThat(aggregateRICoverages, containsInAnyOrder(expectedAggregateRICoverages.toArray()));

    }
}
