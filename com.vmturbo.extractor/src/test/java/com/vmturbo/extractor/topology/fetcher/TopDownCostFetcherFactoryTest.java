package com.vmturbo.extractor.topology.fetcher;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.TierExpenses;
import com.vmturbo.common.protobuf.cost.Cost.ChecksumResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesResponse;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostMoles.RIAndExpenseUploadServiceMole;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.topology.fetcher.TopDownCostFetcherFactory.TopDownCostData;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Unit tests for {@link TopDownCostFetcherFactory}.
 */
public class TopDownCostFetcherFactoryTest {

    private CostServiceMole costBackend = spy(CostServiceMole.class);

    private RIAndExpenseUploadServiceMole expenseUploadBackend = spy(RIAndExpenseUploadServiceMole.class);

    /**
     * Mock tests for gRPC services.
     */
    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(costBackend, expenseUploadBackend);

    private TopDownCostFetcherFactory fetcherFactory;

    private final long serviceId = 1L;

    private final double serviceAccount1Amount = 1;

    private final double serviceAccount2Amount = 2;

    private final long tierId = 2L;

    private final double tierAccount1Amount = 10;

    private final double tierAccount2Amount = 20;

    private final long account1Id = 11;
    private final long account1Date = 1_000_000;

    private final long account2Id = 22;
    private final long account2Date = 2_000_000;

    private final AccountExpenses account1Expenses = AccountExpenses.newBuilder()
            .setAssociatedAccountId(account1Id)
            .setExpensesDate(account1Date)
            .setAccountExpensesInfo(AccountExpensesInfo.newBuilder()
                    .addTierExpenses(TierExpenses.newBuilder()
                            .setAssociatedTierId(tierId)
                            .setExpenses(CurrencyAmount.newBuilder()
                                    .setAmount(tierAccount1Amount)))
                    .addServiceExpenses(ServiceExpenses.newBuilder()
                            .setAssociatedServiceId(serviceId)
                            .setExpenses(CurrencyAmount.newBuilder()
                                    .setAmount(serviceAccount1Amount))))
            .build();

    private final AccountExpenses account2Expenses = AccountExpenses.newBuilder()
            .setAssociatedAccountId(account2Id)
            .setExpensesDate(account2Date)
            .setAccountExpensesInfo(AccountExpensesInfo.newBuilder()
                    .addTierExpenses(TierExpenses.newBuilder()
                            .setAssociatedTierId(tierId)
                            .setExpenses(CurrencyAmount.newBuilder()
                                    .setAmount(tierAccount2Amount)))
                    .addServiceExpenses(ServiceExpenses.newBuilder()
                            .setAssociatedServiceId(serviceId)
                            .setExpenses(CurrencyAmount.newBuilder()
                                    .setAmount(serviceAccount2Amount))))
            .build();

    /**
     * Common code before every test.
     */
    @Before
    public void setup() {
        fetcherFactory = new TopDownCostFetcherFactory(
            CostServiceGrpc.newBlockingStub(server.getChannel()),
            RIAndExpenseUploadServiceGrpc.newBlockingStub(server.getChannel()));
        doReturn(GetCurrentAccountExpensesResponse.newBuilder()
            .addAccountExpense(account1Expenses)
            .addAccountExpense(account2Expenses)
            .build()).when(costBackend).getCurrentAccountExpenses(any());
        setMockChecksum(1L);
    }

    /**
     * Test and validate that the retrieved top-down cost data is correct.
     */
    @Test
    public void testGetMostRecentData() {
        // ACT
        TopDownCostData costData = fetcherFactory.newFetcher(new MultiStageTimer(null),
                (data) -> { }).fetch();

        // Check that the expense dates are correct.
        assertThat(costData.getAccountExpenses(account1Id).get(), is(account1Expenses));
        assertThat(costData.getAccountExpenses(account2Id).get(), is(account2Expenses));

        // Check that the backend request is correct
        ArgumentCaptor<GetCurrentAccountExpensesRequest> reqCaptor = ArgumentCaptor.forClass(GetCurrentAccountExpensesRequest.class);
        verify(costBackend).getCurrentAccountExpenses(reqCaptor.capture());
        assertTrue(reqCaptor.getValue().getScope().getAllAccounts());
    }

    /**
     * Test that the checksum retrieved from the cost component is cached, and used to avoid
     * unnecessary RPCs for top down cost data.
     */
    @Test
    public void testChecksum() {
        setMockChecksum(1L);

        TopDownCostData data = fetcherFactory.getMostRecentData();
        // The first call should result in a backend request
        verify(costBackend, times(1)).getCurrentAccountExpenses(any());

        TopDownCostData secondCallData = fetcherFactory.getMostRecentData();

        // No more backend requests.
        verify(costBackend, times(1)).getCurrentAccountExpenses(any());

        // Should be the same object.
        assertThat(data, is(secondCallData));

        // Change the checksum
        setMockChecksum(2L);

        fetcherFactory.getMostRecentData();

        // Since checksum is different, there is another RPC
        verify(costBackend, times(2)).getCurrentAccountExpenses(any());
    }

    private void setMockChecksum(final long checksum) {
        doReturn(ChecksumResponse.newBuilder()
            .setChecksum(checksum)
            .build()).when(expenseUploadBackend).getAccountExpensesChecksum(any());
    }

}