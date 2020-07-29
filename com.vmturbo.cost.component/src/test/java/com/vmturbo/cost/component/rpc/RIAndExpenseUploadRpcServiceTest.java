package com.vmturbo.cost.component.rpc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.grpc.stub.StreamObserver;

import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.AccountRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataResponse;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.expenses.AccountExpensesStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceCoverageUpdate;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * This class tests methods in the RIAndExpenseUploadRpcService class.
 */
public class RIAndExpenseUploadRpcServiceTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Cost.COST);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private final DSLContext dsl = dbConfig.getDslContext();

    private final AccountExpensesStore accountExpensesStore =
            mock(AccountExpensesStore.class);

    @Nonnull
    private final ReservedInstanceSpecStore reservedInstanceSpecStore =
            mock(ReservedInstanceSpecStore.class);

    @Nonnull
    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore =
            mock(ReservedInstanceBoughtStore.class);

    private final ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate =
            mock(ReservedInstanceCoverageUpdate.class);

    private RIAndExpenseUploadRpcService riAndExpenseUploadRpcService;

    @Captor
    private ArgumentCaptor<List<ReservedInstanceSpec>> reservedInstanceSpecListCaptor;

    @Captor
    private ArgumentCaptor<List<ReservedInstanceBoughtInfo>> reservedInstanceBoughtInfoListCaptor;

    @Captor
    private ArgumentCaptor<List<EntityRICoverageUpload>> entityRICoverageListCaptor;

    @Captor
    private ArgumentCaptor<List<AccountRICoverageUpload>> accountRICoverageListCaptor;



    /**
     * Set up before a test.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        riAndExpenseUploadRpcService = new RIAndExpenseUploadRpcService(dsl, accountExpensesStore,
                reservedInstanceSpecStore, reservedInstanceBoughtStore,
                reservedInstanceCoverageUpdate, true);
    }

    /**
     * Test uploadRIData method.
     */
    @Test
    public void testUploadRIData() {

        // setup input
        final long topologyId = 1234567L;
        final String probeReservedInstanceId = "probe_reserved_instance_id";
        final ReservedInstanceSpec reservedInstanceSpec = ReservedInstanceSpec.getDefaultInstance();
        final ReservedInstanceBought reservedInstanceBought = ReservedInstanceBought.newBuilder()
                .setId(1L)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceSpec(2L)
                        .setProbeReservedInstanceId(probeReservedInstanceId))
                .build();
        final EntityRICoverageUpload entityRICoverageUpload =
                EntityRICoverageUpload.newBuilder().setEntityId(4L).addCoverage(
                        Coverage.newBuilder()
                                .setProbeReservedInstanceId(probeReservedInstanceId)
                                .setCoveredCoupons(96D)
                                .setUsageStartTimestamp(0)
                                .setUsageEndTimestamp(TimeUnit.DAYS.toMillis(1))).addCoverage(
                        Coverage.newBuilder()
                                .setProbeReservedInstanceId(probeReservedInstanceId)
                                .setCoveredCoupons(48D)).build();
        final AccountRICoverageUpload accountCoverage =
                AccountRICoverageUpload.newBuilder()
                        .setAccountId(10L)
                        .addCoverage(Coverage.newBuilder()
                                .setProbeReservedInstanceId(probeReservedInstanceId)
                                .setCoveredCoupons(384)
                                .setUsageStartTimestamp(1591574400000L)
                                .setUsageEndTimestamp(1591574400000L + TimeUnit.DAYS.toMillis(2)))
                        .build();
        final ReservedInstanceBought undiscoveredRIBought = ReservedInstanceBought.newBuilder()
                .setId(1L)
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceSpec(2L)
                        .setProbeReservedInstanceId(probeReservedInstanceId))
                .build();

        final UploadRIDataRequest uploadRIDataRequest = UploadRIDataRequest.newBuilder()
                .setTopologyContextId(topologyId)
                .addReservedInstanceSpecs(reservedInstanceSpec)
                .addReservedInstanceBought(reservedInstanceBought)
                .addReservedInstanceBought(undiscoveredRIBought)
                .addReservedInstanceCoverage(entityRICoverageUpload)
                .addAccountLevelReservedInstanceCoverage(accountCoverage)
                .build();
        final StreamObserver<UploadRIDataResponse> mockResponseObserver = mock(StreamObserver.class);

        // setup mocks
        final ReservedInstanceBought storedReservedInstanceBought = ReservedInstanceBought
                .newBuilder(reservedInstanceBought)
                .setId(3L)
                .build();
        when(reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
                .thenReturn(ImmutableList.of(storedReservedInstanceBought));
        when(reservedInstanceSpecStore.updateReservedInstanceSpec(any(), any()))
                .thenReturn(ImmutableMap.of(2L, 7L));

        // invoke SUT
        riAndExpenseUploadRpcService.uploadRIData(uploadRIDataRequest, mockResponseObserver);

        // setup captors
        verify(reservedInstanceSpecStore).updateReservedInstanceSpec(
                any(), reservedInstanceSpecListCaptor.capture());
        verify(reservedInstanceBoughtStore).updateReservedInstanceBought(
                any(), reservedInstanceBoughtInfoListCaptor.capture());
        verify(reservedInstanceCoverageUpdate).storeEntityRICoverageOnlyIntoCache(
                eq(topologyId), entityRICoverageListCaptor.capture());
        verify(reservedInstanceCoverageUpdate).cacheAccountRICoverageData(
                eq(topologyId), accountRICoverageListCaptor.capture());

        // assertions
        final List<ReservedInstanceSpec> actualReservedInstanceSpecs =
                reservedInstanceSpecListCaptor.getValue();
        assertThat(actualReservedInstanceSpecs.size(), equalTo(1));
        final List<ReservedInstanceBoughtInfo> actualReservedInstanceBoughtInfoList =
                reservedInstanceBoughtInfoListCaptor.getValue();
        assertThat(actualReservedInstanceBoughtInfoList.size(), equalTo(2));
        final ReservedInstanceBoughtInfo actualRIBoughtInfo =
                actualReservedInstanceBoughtInfoList.get(0);
        // verify the spec ID was updated based on the request spec ID -> local OID mapping
        assertThat(actualRIBoughtInfo.getReservedInstanceSpec(), equalTo(7L));
        // verify the Coverage::reservedInstanceId is updated based on the ID assigned
        // in the ReservedInstanceBoughtStore
        Assert.assertEquals(Collections.singletonList(EntityRICoverageUpload.newBuilder()
                .setEntityId(4)
                .addCoverage(Coverage.newBuilder()
                        .setReservedInstanceId(3)
                        .setProbeReservedInstanceId(probeReservedInstanceId)
                        .setCoveredCoupons(4D)
                        .setUsageStartTimestamp(0)
                        .setUsageEndTimestamp(86400000))
                .addCoverage(Coverage.newBuilder()
                        .setReservedInstanceId(3)
                        .setProbeReservedInstanceId(probeReservedInstanceId)
                        .setCoveredCoupons(48D))
                .build()), entityRICoverageListCaptor.getValue());
        // verify account coverage
        Assert.assertEquals(Collections.singletonList(AccountRICoverageUpload.newBuilder()
                .setAccountId(10)
                .addCoverage(Coverage.newBuilder()
                        .setReservedInstanceId(3)
                        .setProbeReservedInstanceId(probeReservedInstanceId)
                        .setCoveredCoupons(8.0)
                        .setUsageStartTimestamp(1591574400000L)
                        .setUsageEndTimestamp(1591747200000L))
                .build()), accountRICoverageListCaptor.getValue());
    }
}
