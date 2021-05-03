package com.vmturbo.common.api.mappers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessAccountData.AccountType;

/**
 * {@link AccountTypeMapper} tests.
 */
public class AccountTypeMapperTest {

    /**
     * Testing converting AccountType XL Enum to API Enum.
     */
    @Test
    public void fromXLToApi() {
        assertEquals(AccountTypeMapper.fromXLToApi(AccountType.Standard), com.vmturbo.api.enums.AccountType.STANDARD);
        assertEquals(AccountTypeMapper.fromXLToApi(AccountType.Government_US), com.vmturbo.api.enums.AccountType.GOVERNMENT_US);
        assertNull(AccountTypeMapper.fromXLToApi(null));
    }

    /**
     * Tests all {@link AccountType} enums map to non null values.
     */
    @Test
    public void fromXLToApiAllEnumsMapToNonNull() {
        Arrays.stream(AccountType.values()).forEach(accountType ->
            assertNotNull(AccountTypeMapper.fromXLToApi(accountType)));
    }

    /**
     * Tests all {@link com.vmturbo.api.enums.AccountType} enums map to non null values.
     */
    @Test
    public void fromApiToXlAllEnumsMapToNonNull() {
        Arrays.stream(com.vmturbo.api.enums.AccountType.values()).forEach(accountType ->
            assertNotNull(AccountTypeMapper.fromApiToXL(accountType)));
    }

    /**
     * Testing converting AccountType API Enum to XL Enum.
     */
    @Test
    public void fromApiToXl() {
        assertEquals(AccountTypeMapper.fromApiToXL(com.vmturbo.api.enums.AccountType.STANDARD), AccountType.Standard);
        assertEquals(AccountTypeMapper.fromApiToXL(com.vmturbo.api.enums.AccountType.GOVERNMENT_US), AccountType.Government_US);
        assertNull(AccountTypeMapper.fromApiToXL(null));
    }
}
