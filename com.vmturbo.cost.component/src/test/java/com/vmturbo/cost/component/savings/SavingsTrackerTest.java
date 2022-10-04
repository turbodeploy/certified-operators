package com.vmturbo.cost.component.savings;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;

import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.savings.bottomup.SqlEntitySavingsStore;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Test cases for SavingsTracker.
 */
public class SavingsTrackerTest {

    private final SearchServiceMole searchServiceMole = spy(new SearchServiceMole());

    /**
     * GRPC server.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(searchServiceMole);

    private static final long CSP_AZURE_OID = 55555555L;

    private final Set<EntityType> supportedEntityTypes = ImmutableSet.of(EntityType.VIRTUAL_VOLUME, EntityType.DATABASE);

    /**
     * Test the initialization of the supported CSP OID list is called when isSupportedCSP is called
     * for the first time. Also test the return value of the isSupportedCSP method.
     *
     * @throws IOException IO Exception
     */
    @Test
    public void testCspCheck() throws IOException {
        grpcServer.start();
        SearchEntitiesResponse response = SearchEntitiesResponse.newBuilder().addEntities(PartialEntity.newBuilder().setMinimal(
                MinimalEntity.newBuilder().setOid(CSP_AZURE_OID).build()).build()).build();
        when(searchServiceMole.searchEntities(any(SearchEntitiesRequest.class)))
                .thenReturn(response);
        final SearchServiceBlockingStub searchService =
                SearchServiceGrpc.newBlockingStub(grpcServer.getChannel());

        ActionChainStore actionChainStore = mock(GrpcActionChainStore.class);
        SavingsStore savingsStore = mock(SqlEntitySavingsStore.class);
        DSLContext dsl = mock(DSLContext.class);
        RepositoryClient repositoryClient = mock(RepositoryClient.class);
        Clock clock = Clock.systemUTC();
        SavingsTracker tracker = spy(new SavingsTracker(
                new SqlBillingRecordStore(dsl),
                actionChainStore,
                savingsStore,
                supportedEntityTypes,
                TimeUnit.DAYS.toMillis(365),
                clock, mock(TopologyEntityCloudTopologyFactory.class),
                repositoryClient, dsl, mock(BusinessAccountPriceTableKeyStore.class),
                mock(PriceTableStore.class), searchService, 777777, 100));

        BillingRecord record = createBillingRecord(1234123414314L);

        boolean isSupported = tracker.isSupportedCSP(record);
        // The supported CSP OIDs have not been initialized. So expect the populateSupportedCspOids
        // method to be invoked.
        verify(tracker).populateSupportedCspOids();

        // Reset the invocation counts.
        reset(tracker);

        // The service Provider is not the supported one. Expect the boolean returned to be false.
        Assert.assertFalse(isSupported);

        BillingRecord record2 = createBillingRecord(CSP_AZURE_OID);
        // This time the provider is the expected one. Expect the method to return true.
        isSupported = tracker.isSupportedCSP(record2);
        // Since the supported CSP OID cache is already initialized. So the populate method should not be called.
        verify(tracker, never()).populateSupportedCspOids();
        Assert.assertTrue(isSupported);
    }

    private BillingRecord createBillingRecord(long serviceProviderId) {
        return new BillingRecord.Builder()
                .sampleTime(LocalDateTime.now())
                .entityId(22222L)
                .entityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .accountId(1L)
                .regionId(2L)
                .priceModel(PriceModel.ON_DEMAND)
                .costCategory(CostCategory.COMPUTE)
                .providerId(3333L)
                .providerType(EntityType.COMPUTE_TIER_VALUE)
                .commodityType(CommodityType.UNKNOWN_VALUE)
                .usageAmount(10.0)
                .cost(10.0)
                .serviceProviderId(serviceProviderId)
                .build();
    }
}
