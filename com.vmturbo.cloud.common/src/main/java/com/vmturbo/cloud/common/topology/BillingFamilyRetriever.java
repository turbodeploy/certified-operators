package com.vmturbo.cloud.common.topology;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.group.api.GroupAndMembers;

/**
 * A utility class for querying the billing family associated with an account.
 */
public interface BillingFamilyRetriever {

    /**
     * Queries the available billing family groups from the group component and checks for the
     * given {@code accountOid} in any of the groups. If the billing families are not well formed,
     * including the same account in multiple billing families, there is not guarantee on which
     * billing family will be returned. The retriever will only query and process the billing family
     * groups on first request. All subsequent requests will use the cached groups.
     *
     * @param accountOid The target account OID, which will be checked against the members of the
     *                   billing family groups.
     * @return The {@link GroupAndMembers} representing the billing family, if one is found.
     */
    @Nonnull
    Optional<GroupAndMembers> getBillingFamilyForAccount(long accountOid);
}
