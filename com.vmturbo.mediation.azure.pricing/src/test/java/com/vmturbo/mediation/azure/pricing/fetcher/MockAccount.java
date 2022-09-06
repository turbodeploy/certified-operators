package com.vmturbo.mediation.azure.pricing.fetcher;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.hybrid.cloud.common.CloudProxyAwareAccount;
import com.vmturbo.platform.sdk.probe.AccountValue;

/**
 * Mock account for a target. This is intended to parallel Azure MCA, where a single pricing file
 * is used for all pricing plans in the account, so the key should not be all the account fields,
 * but only the account id.
 */
public class MockAccount extends CloudProxyAwareAccount {
    /**
     * Some kind of account identifier.
     */
    @AccountValue(displayName = "Account ID", description = "Account ID string",
            targetDisplayName = true)
    public final String accountId;

    /**
     * Identifier for a pricing plan within that account (eg regular vs DevTest).
     */
    @AccountValue(displayName = "Pricing Plan ID", description = "Pricing Plan ID",
            targetDisplayName = true)
    public final String planId;

    /**
     * Create a mock account for tests.
     *
     * @param accountId some kind of account identifier
     * @param planId identifier for a pricing plan within that account (eg regular vs DevTest)
     */
    public MockAccount(@Nonnull final String accountId, @Nonnull final String planId) {
        super(null, 0, null, null, false);
        this.accountId = accountId;
        this.planId = planId;
    }

    /**
     * Get the account ID.
     *
     * @return the account ID.
     */
    public String getAccountId() {
        return accountId;
    }

    /**
     * Get the plan ID.
     *
     * @return the plan ID.
     */
    public String getPlanId() {
        return planId;
    }
}
