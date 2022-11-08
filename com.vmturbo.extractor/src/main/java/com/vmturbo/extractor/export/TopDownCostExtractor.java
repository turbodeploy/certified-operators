package com.vmturbo.extractor.export;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.extractor.ExtractorGlobalConfig.ExtractorFeatureFlags;
import com.vmturbo.extractor.schema.json.export.AccountExpenses;
import com.vmturbo.extractor.schema.json.export.CostAmount;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.fetcher.TopDownCostFetcherFactory.TopDownCostData;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Extracts {@link AccountExpenses} from account entities in a topology.
 */
public class TopDownCostExtractor {

    private final TopDownCostData topDownCostData;

    private final TopologyGraph<SupplyChainEntity> topologyGraph;

    private final ExtractorFeatureFlags featureFlags;

    /**
     * Create a new extractor. There is a new one created every time we process a topology.
     *
     * @param topDownCostData The {@link TopDownCostData}.
     * @param topologyGraph The {@link TopologyGraph}.
     * @param featureFlags providing access to extractor's feature flags
     */
    public TopDownCostExtractor(@Nonnull final TopDownCostData topDownCostData,
            @Nonnull final TopologyGraph<SupplyChainEntity> topologyGraph,
            @Nonnull final ExtractorFeatureFlags featureFlags) {
        this.topDownCostData = topDownCostData;
        this.topologyGraph = topologyGraph;
        this.featureFlags = featureFlags;
    }

    /**
     * Get the expenses associated with an entity.
     *
     * @param entityId The entity id.
     * @return The {@link AccountExpenses} for the entity.
     */
    public Optional<AccountExpenses> getExpenses(final long entityId) {
        return topDownCostData.getAccountExpenses(entityId)
            .map(accountExpenses -> {
                final AccountExpenses exportExpenses = new AccountExpenses();
                exportExpenses.setExpenseDate(ExportUtils.getFormattedDate(accountExpenses.getExpensesDate()));
                final Map<String, CostAmount> expensesBySvc = new HashMap<>();
                accountExpenses.getAccountExpensesInfo().getServiceExpensesList()
                    .forEach(serviceExpenses -> topologyGraph.getEntity(serviceExpenses.getAssociatedServiceId())
                        .ifPresent(svc -> {
                            expensesBySvc.put(svc.getDisplayName(),
                                    CostAmount.newAmount(serviceExpenses.getExpenses()));
                        }));
                if (!expensesBySvc.isEmpty()) {
                    // Flatten the service expenses mapping into a list containing the cost amount
                    // type for each entry to be used for creating MVs in redshift.
                    if (featureFlags.enableKeysAsValues()) {
                        expensesBySvc.forEach((serviceExpense, costAmount) -> {
                            costAmount.setType(serviceExpense);
                        });
                        exportExpenses.setNewServiceExpenses(expensesBySvc);
                    } else {
                        exportExpenses.setServiceExpenses(expensesBySvc);
                    }
                }
                return exportExpenses;
            });
    }
}
