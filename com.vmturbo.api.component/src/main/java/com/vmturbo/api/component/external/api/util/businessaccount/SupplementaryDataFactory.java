package com.vmturbo.api.component.external.api.util.businessaccount;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenseQueryScope;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenseQueryScope.IdList;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;

/**
 * Factory class for {@link SupplementaryData}, responsible for the heavy lifting of
 * actually doing the bulk fetches of data from other components (e.g. costs from the cost
 * component, severities from the action orchestrator, maybe resource groups from the
 * group component).
 *
 * <p>Also helps unit test the {@link BusinessAccountRetriever} more manageably.
 */
public class SupplementaryDataFactory {
    private final Logger logger = LogManager.getLogger(getClass());
    private final CostServiceBlockingStub costService;
    private final GroupServiceBlockingStub groupService;

    /**
     * Constructs supplementary data factory.
     *
     * @param costServiceBlockingStub cost gRPC service to use
     * @param groupServiceBlockingStub group gRPC service to use
     */
    public SupplementaryDataFactory(@Nonnull CostServiceBlockingStub costServiceBlockingStub,
            @Nonnull GroupServiceBlockingStub groupServiceBlockingStub) {
        this.costService = Objects.requireNonNull(costServiceBlockingStub);
        this.groupService = Objects.requireNonNull(groupServiceBlockingStub);
    }

    /**
     * Create a new {@link SupplementaryData} instance.
     *
     * @param accountIds set of accountIds
     * @param allAccounts A hint to say whether these accounts represent ALL accounts. This
     *         allows us to optimize queries for related data, if necessary.
     * @return The {@link SupplementaryData}.
     */
    @Nonnull
    public SupplementaryData newSupplementaryData(@Nonnull Set<Long> accountIds, boolean allAccounts) {
        Map<Long, Float> costsByAccount;
        try {
            costsByAccount = getCostsByAccount(accountIds, allAccounts);
        } catch (StatusRuntimeException e) {
            if (Code.UNAVAILABLE == e.getStatus().getCode()) {
                // Any component may be down at any time. APIs like search should not fail
                // when the cost component is down. We must log a warning when this happens,
                // or else it will be difficult for someone to explain why search does not
                // have cost data.
                logger.warn(
                        "The cost component is not available. As a result, we will not fill in the response with cost details for accountIds={} and allAccounts={}",
                        () -> accountIds, () -> allAccounts);
                costsByAccount = null;
            } else {
                // Cost component responded, so it's up an running. We need to make this
                // exception visible because there might be a bug in the cost component.
                throw e;
            }
        }
        final Map<Long, Integer> groupsCountOwnedByAccount =
                getResourceGroupsCountOwnedByAccount(accountIds);
        return new SupplementaryData(costsByAccount, groupsCountOwnedByAccount);
    }

    @Nonnull
    private Map<Long, Integer> getResourceGroupsCountOwnedByAccount(@Nonnull Set<Long> accountIds) {
        final Map<Long, Integer> groupsCountOwnedByAccount = new HashMap<>(accountIds.size());
        accountIds.forEach(account -> {
            final Integer groupsCount = groupService.countGroups(GetGroupsRequest.newBuilder()
                    .setGroupFilter(GroupFilter.newBuilder()
                            .addPropertyFilters(SearchProtoUtil.stringPropertyFilterExact(
                                    SearchableProperties.ACCOUNT_ID,
                                    Collections.singletonList(account.toString())))
                            .build())
                    .build()).getCount();
            groupsCountOwnedByAccount.put(account, groupsCount);
        });
        return groupsCountOwnedByAccount;
    }

    @Nonnull
    private Map<Long, Float> getCostsByAccount(@Nonnull Set<Long> specificAccountIds,
            boolean allAccounts) {
        final AccountExpenseQueryScope.Builder scopeBldr = AccountExpenseQueryScope.newBuilder();
        if (allAccounts) {
            scopeBldr.setAllAccounts(true);
        } else {
            scopeBldr.setSpecificAccounts(IdList.newBuilder().addAllAccountIds(specificAccountIds));
        }

        final GetCurrentAccountExpensesResponse response = costService.getCurrentAccountExpenses(
                GetCurrentAccountExpensesRequest.newBuilder().setScope(scopeBldr).build());

        // Sum the expenses across services for each account.
        //
        // It's not clear whether we should also be adding the per-tier expenses, or if
        // the per-tier expenses are part of the per-service expenses.
        //
        // As this logic gets more complex we should move it out to a separate calculator class.
        final Map<Long, Float> costsByAccount = new HashMap<>();
        for (AccountExpenses expense : response.getAccountExpenseList()) {
            final long accountId = expense.getAssociatedAccountId();
            float totalExpense = 0.0f;
            for (ServiceExpenses svcExpense : expense.getAccountExpensesInfo()
                    .getServiceExpensesList()) {
                totalExpense += svcExpense.getExpenses().getAmount();
            }
            costsByAccount.put(accountId, totalExpense);
        }
        return costsByAccount;
    }
}
