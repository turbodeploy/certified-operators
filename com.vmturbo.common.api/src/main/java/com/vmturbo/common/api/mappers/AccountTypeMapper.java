package com.vmturbo.common.api.mappers;

import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessAccountData.AccountType;

/**
 * Utility for mapping ENUMs {@link com.vmturbo.api.enums.AccountType} and {@link AccountType}.
 */
public class AccountTypeMapper {

    /**
     * Mappings between {@link AccountType} and {@link com.vmturbo.api.enums.AccountType}.
     */
    private static final BiMap<AccountType, com.vmturbo.api.enums.AccountType> ACCOUNT_TYPE_MAPPINGS =
                    new ImmutableBiMap.Builder()
                                    .put(AccountType.Standard,  com.vmturbo.api.enums.AccountType.STANDARD)
                                    .put(AccountType.Government_US,  com.vmturbo.api.enums.AccountType.GOVERNMENT_US)
                                    .build();

    /**
     * Private constructor, never initialized, pattern for a utility class.
     */
    private AccountTypeMapper(){}

    /**
     * Get the {@link com.vmturbo.api.enums.AccountType} associated with a {@link AccountType}.
     *
     * @param accountType The {@link AccountType}.
     * @return The associated {@link com.vmturbo.api.enums.AccountType},
     *         or null
     */
    @Nullable
    public static com.vmturbo.api.enums.AccountType fromXLToApi(@Nullable final AccountType accountType) {
        return ACCOUNT_TYPE_MAPPINGS.getOrDefault(accountType, null);
    }

    /**
     * Get the {@link AccountType} associated with a {@link com.vmturbo.api.enums.AccountType}.
     *
     * @param accountType The {@link com.vmturbo.api.enums.AccountType}.
     * @return The associated {@link AccountType}
     */
    @Nullable
    public static AccountType fromApiToXL(@Nullable final com.vmturbo.api.enums.AccountType accountType) {
        return ACCOUNT_TYPE_MAPPINGS.inverse().getOrDefault(accountType, null);

    }
}
