package com.vmturbo.api.component.external.api.util.businessaccount;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.businessunit.BusinessUnitApiDTO;

/**
 * Container class for all data from other components required to decorate the output
 * {@link BusinessUnitApiDTO}s.
 */
public class SupplementaryData {

    private final Map<Long, Float> costsByAccountId;
    private final Map<Long, Integer> countOfGroupsOwnedByAccount;

    /**
     * Constructs supplementary data.
     *
     * @param costsByAccountId costs by account id
     * @param countOfGroupsOwnedByAccount count of groups owned by account id
     */
    public SupplementaryData(@Nullable final Map<Long, Float> costsByAccountId,
            @Nonnull Map<Long, Integer> countOfGroupsOwnedByAccount) {
        this.costsByAccountId = costsByAccountId;
        this.countOfGroupsOwnedByAccount = Objects.requireNonNull(countOfGroupsOwnedByAccount);
    }

    /**
     * Returns the cost price for the specified account. Account is not known, returns 0.
     *
     * @param accountId account to get cost data for
     * @return cost price for the account
     */
    @Nonnull
    public Optional<Float> getCostPrice(@Nonnull final Long accountId) {
        if (costsByAccountId == null) {
            return Optional.empty();
        }
        return Optional.of(costsByAccountId.getOrDefault(accountId, 0.0f));
    }

    /**
     * Returns number of resource groups for a specified account.
     *
     * @param accountId account to retrieve RG count for
     * @return number of resource groups
     */
    @Nonnull
    public Integer getResourceGroupCount(@Nonnull final Long accountId) {
        return countOfGroupsOwnedByAccount.getOrDefault(accountId, 0);
    }
}
