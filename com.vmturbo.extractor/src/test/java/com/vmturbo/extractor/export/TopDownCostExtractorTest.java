package com.vmturbo.extractor.export;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.extractor.schema.json.export.AccountExpenses;
import com.vmturbo.extractor.schema.json.export.CostAmount;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.TopDownCostFetcherFactory.TopDownCostData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Unit tests for {@link TopDownCostExtractor}.
 */
public class TopDownCostExtractorTest {

    private TopDownCostData topDownCostData = mock(TopDownCostData.class);
    private TopologyGraph<SupplyChainEntity> supplyChainEntityTopologyGraph = mock(TopologyGraph.class);

    private TopDownCostExtractor extractor = new TopDownCostExtractor(topDownCostData, supplyChainEntityTopologyGraph);

    private final long serviceId1 = 1;
    private final long serviceId2 = 2;
    private final long accountId = 5;
    private final long expenseDate = 1_000_000;
    private final SupplyChainEntity svc1 = new SupplyChainEntity(TopologyEntityDTO.newBuilder()
            .setDisplayName("Amazon EC2")
            .setOid(serviceId1)
            .setEntityType(EntityType.SERVICE_VALUE)
            .build());
    private final SupplyChainEntity svc2 = new SupplyChainEntity(TopologyEntityDTO.newBuilder()
            .setDisplayName("Amazon RDS")
            .setOid(serviceId2)
            .setEntityType(EntityType.SERVICE_VALUE)
            .build());
    private final CurrencyAmount service1Cost = CurrencyAmount.newBuilder()
            .setAmount(1)
            .setCurrency(1)
            .build();
    private final CurrencyAmount service2Cost = CurrencyAmount.newBuilder()
            .setAmount(2)
            .setCurrency(1)
            .build();

    private final Cost.AccountExpenses accountExpenses = Cost.AccountExpenses.newBuilder()
            .setAssociatedAccountId(accountId)
            .setExpensesDate(expenseDate)
            .setAccountExpensesInfo(AccountExpensesInfo.newBuilder()
                    .addServiceExpenses(ServiceExpenses.newBuilder()
                        .setAssociatedServiceId(serviceId1)
                        .setExpenses(service1Cost))
                    .addServiceExpenses(ServiceExpenses.newBuilder()
                            .setAssociatedServiceId(serviceId2)
                            .setExpenses(service2Cost)))
            .build();

    /**
     * Test successfully extracting expenses.
     */
    @Test
    public void testExtractExpenses() {
        when(topDownCostData.getAccountExpenses(accountId)).thenReturn(Optional.of(accountExpenses));
        when(supplyChainEntityTopologyGraph.getEntity(serviceId1)).thenReturn(Optional.of(svc1));
        when(supplyChainEntityTopologyGraph.getEntity(serviceId2)).thenReturn(Optional.of(svc2));

        // ACT
        AccountExpenses expenses = extractor.getExpenses(accountId).get();

        assertThat(expenses.getExpenseDate(), is(ExportUtils.getFormattedDate(expenseDate)));
        assertThat(expenses.getServiceList(), containsInAnyOrder(svc1.getDisplayName(), svc2.getDisplayName()));
        assertThat(expenses.getServiceExpenses().size(), is(2));
        assertThat(expenses.getServiceExpenses().get(svc1.getDisplayName()), is(CostAmount.newAmount(service1Cost)));
        assertThat(expenses.getServiceExpenses().get(svc2.getDisplayName()), is(CostAmount.newAmount(service2Cost)));
    }

    /**
     * Test the case when an entity has no associated expenses.
     */
    @Test
    public void testExtractExpensesNotFound() {
        when(topDownCostData.getAccountExpenses(accountId)).thenReturn(Optional.empty());

        assertFalse(extractor.getExpenses(accountId).isPresent());
    }

    /**
     * Test the case when the account for an expense is not found in the topology.
     */
    @Test
    public void testExtractExpensesEntityNotFound() {
        when(topDownCostData.getAccountExpenses(accountId)).thenReturn(Optional.of(accountExpenses));
        // No entity
        when(supplyChainEntityTopologyGraph.getEntity(serviceId1)).thenReturn(Optional.empty());
        when(supplyChainEntityTopologyGraph.getEntity(serviceId2)).thenReturn(Optional.empty());

        AccountExpenses expenses = extractor.getExpenses(accountId).get();

        assertThat(expenses.getExpenseDate(), is(ExportUtils.getFormattedDate(expenseDate)));

        // If there are expenses but we don't map them successfully, the EntityExpenses should not
        // appear in the returned list.
        assertNull(expenses.getServiceList());
        assertNull(expenses.getServiceExpenses());
    }

    /**
     * Test the case when some of the services are not found.
     */
    @Test
    public void testExtractExpensesExpenseDateNotFound() {
        when(topDownCostData.getAccountExpenses(accountId)).thenReturn(Optional.of(accountExpenses));
        // No entity
        when(supplyChainEntityTopologyGraph.getEntity(serviceId1)).thenReturn(Optional.of(svc1));
        when(supplyChainEntityTopologyGraph.getEntity(serviceId2)).thenReturn(Optional.empty());

        AccountExpenses expenses = extractor.getExpenses(accountId).get();

        assertThat(expenses.getExpenseDate(), is(ExportUtils.getFormattedDate(expenseDate)));

        // If there are expenses but we don't map them successfully, the EntityExpenses should not
        // appear in the returned list.
        assertThat(expenses.getServiceList(), containsInAnyOrder(svc1.getDisplayName()));
        assertThat(expenses.getServiceExpenses(),
            is(Collections.singletonMap(svc1.getDisplayName(), CostAmount.newAmount(service1Cost))));
    }
}