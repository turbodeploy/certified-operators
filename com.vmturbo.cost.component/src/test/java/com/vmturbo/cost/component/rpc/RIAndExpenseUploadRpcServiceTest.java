package com.vmturbo.cost.component.rpc;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataResponse;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.cost.component.expenses.AccountExpensesStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceCoverageUpdate;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class RIAndExpenseUploadRpcServiceTest {

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private DSLContext dsl;

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


    @Before
    public void setup() throws CommunicationException {

        MockitoAnnotations.initMocks(this);

        // we just need the DSL setup
        dsl = dbConfig.dsl();

        riAndExpenseUploadRpcService = new RIAndExpenseUploadRpcService(
                dsl, accountExpensesStore, reservedInstanceSpecStore,
                reservedInstanceBoughtStore, reservedInstanceCoverageUpdate);
    }

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
        final EntityRICoverageUpload entityRICoverageUpload = EntityRICoverageUpload.newBuilder()
                .setEntityId(4L)
                .addCoverage(Coverage.newBuilder()
                        .setProbeReservedInstanceId(probeReservedInstanceId))
                .build();

        final UploadRIDataRequest uploadRIDataRequest = UploadRIDataRequest.newBuilder()
                .setTopologyId(topologyId)
                .addReservedInstanceSpecs(reservedInstanceSpec)
                .addReservedInstanceBought(reservedInstanceBought)
                .addReservedInstanceCoverage(entityRICoverageUpload)
                .build();
        final StreamObserver<UploadRIDataResponse> mockResponseObserver = mock(StreamObserver.class);

        // setup mocks
        final ReservedInstanceBought storedReservedInstanceBought = ReservedInstanceBought
                .newBuilder(reservedInstanceBought)
                .setId(3L)
                .build();
        when(reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(any()))
                .thenReturn(ImmutableList.of(storedReservedInstanceBought));
        when(reservedInstanceSpecStore.updateReservedInstanceBoughtSpec(any(), any()))
                .thenReturn(ImmutableMap.of(2L, 7L));

        // invoke SUT
        riAndExpenseUploadRpcService.uploadRIData(uploadRIDataRequest, mockResponseObserver);

        // setup captors
        verify(reservedInstanceSpecStore).updateReservedInstanceBoughtSpec(
                any(), reservedInstanceSpecListCaptor.capture());
        verify(reservedInstanceBoughtStore).updateReservedInstanceBought(
                any(), reservedInstanceBoughtInfoListCaptor.capture());
        verify(reservedInstanceCoverageUpdate).storeEntityRICoverageOnlyIntoCache(
                eq(topologyId), entityRICoverageListCaptor.capture());

        // assertions
        final List<ReservedInstanceSpec> actualReservedInstanceSpecs =
                reservedInstanceSpecListCaptor.getValue();
        assertThat(actualReservedInstanceSpecs.size(), equalTo(1));
        final List<ReservedInstanceBoughtInfo> actualReservedInstanceBoughtInfoList =
                reservedInstanceBoughtInfoListCaptor.getValue();
        assertThat(actualReservedInstanceBoughtInfoList.size(), equalTo(1));
        final ReservedInstanceBoughtInfo actualRIBoughtInfo =
                actualReservedInstanceBoughtInfoList.get(0);
        // verify the spec ID was updated based on the request spec ID -> local OID mapping
        assertThat(actualRIBoughtInfo.getReservedInstanceSpec(), equalTo(7L));
        final List<EntityRICoverageUpload> actualEntityRICoverageList =
                entityRICoverageListCaptor.getValue();
        assertThat(actualEntityRICoverageList.size(), equalTo(1));
        // verify the Coverage::reservedInstanceId is updated based on the ID assigned
        // in the ReservedInstanceBoughtStore
        final EntityRICoverageUpload actualEntityRICoverage =
                actualEntityRICoverageList.get(0);
        assertThat(actualEntityRICoverage.getCoverageCount(), equalTo(1));
        final Coverage actualCoverage = actualEntityRICoverage.getCoverage(0);
        // should match ID of storedReservedInstanceBought
        assertThat(actualCoverage.getReservedInstanceId(), equalTo(3L));
    }
}
