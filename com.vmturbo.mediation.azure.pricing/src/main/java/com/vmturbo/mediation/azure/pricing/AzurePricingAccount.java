package com.vmturbo.mediation.azure.pricing;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.mediation.azure.BaseAzureSubscriptionAccount;
import com.vmturbo.platform.sdk.probe.AccountDefinition;
import com.vmturbo.platform.sdk.probe.AccountValue;

/**
 * Azure Pricing Account Definition.
 */
@AccountDefinition
public class AzurePricingAccount extends BaseAzureSubscriptionAccount {
    @AccountValue(
        displayName = "Azure MCA Billing Account ID",
        description = "Azure MCA Billing Account ID",
        targetId = true)
    private final String mcaBillingAccountId;

    @AccountValue(
        displayName = "Azure MCA Billing Profile ID",
        description = "Azure MCA Billing Profile ID",
        targetId = true)
    private final String mcaBillingProfileId;

    @AccountValue(
            displayName = "Azure Plan ID",
            description = "Azure Plan ID",
            targetId = true)
    private final String planId;

    /**
     * Constructor.
     *
     * @param mcaBillingAccountId MCA Billing Account ID.
     * @param mcaBillingProfileId MCA Billing Profile ID.
     * @param planId        Azure Plan ID.
     * @param name          Target name.
     * @param tenant        Tenant ID.
     * @param client        Client ID.
     * @param key           Secret Access Key.
     * @param cloudType     Azure CLoud type.
     * @param proxyHost     - proxy host.
     * @param proxyPort     - proxy port.
     * @param proxyUser     - proxy user.
     * @param proxyPassword - proxy password
     * @param secureProxy   - a flag to use SSL for proxy.
     */
    public AzurePricingAccount(
            @Nonnull final String mcaBillingAccountId,
            @Nonnull final String mcaBillingProfileId,
            @Nonnull final String planId,
            @Nonnull final String name,
            @Nonnull final String tenant,
            @Nonnull final String client,
            @Nonnull final String key,
            @Nonnull final String cloudType,
            @Nullable final String proxyHost, final int proxyPort,
            @Nullable final String proxyUser,
            @Nullable final String proxyPassword,
            final boolean secureProxy) {
        super(name, tenant, client, key, cloudType, proxyHost, proxyPort, proxyUser, proxyPassword, secureProxy);
        this.mcaBillingAccountId = mcaBillingAccountId;
        this.mcaBillingProfileId = mcaBillingProfileId;
        this.planId = planId;
    }

    /**
     * Default constructor for probe-sdk.
     */
    @SuppressWarnings("unused")
    private AzurePricingAccount() {
        this("", "", "", "", "", "", "", "", null, 0, null, null, false);
    }


    /**
     * Get MCA Billing Account ID.
     *
     * @return MCA Billing Account ID.
     */
    public String getMcaBillingAccountId() {
        return mcaBillingAccountId;
    }

    /**
     * Get MCA Billing Profile ID.
     *
     * @return MCA Billing Profile ID.
     */
    public String getMcaBillingProfileId() {
        return mcaBillingProfileId;
    }

    /**
     * Get the Azure Pricing Plan ID.
     *
     * @return the Azure Pricing Plan ID.
     */
    public String getPlanId() {
        return planId;
    }

    @Override
    public String getUsername() {
        return String.format("%s::%s", mcaBillingAccountId, mcaBillingProfileId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMcaBillingAccountId(), getMcaBillingProfileId());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AzurePricingAccount that = (AzurePricingAccount)o;
        return Objects.equals(mcaBillingAccountId, that.mcaBillingAccountId)
            && Objects.equals(mcaBillingProfileId, that.mcaBillingProfileId);
    }
}