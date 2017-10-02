package com.vmturbo.api.component.external.api.service;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.api.enums.InputValueType;
import com.vmturbo.topology.processor.api.AccountFieldValueType;

/**
 * Unit test for {@link AccountFieldValueType} class.
 */
public class AccountFieldValyeTypeTest {
    /**
     * Tests, that enums in TopologyProcessor and in API are the same by content.
     */
    @Test
    public void testEnumsEquality() {
        final Set<String> tpEnums = getNames(AccountFieldValueType.class);
        final Set<String> apiEnums = getNames(InputValueType.class);
        Assert.assertEquals("Enums should have the same values inside", tpEnums, apiEnums);
    }

    private static Set<String> getNames(Class<? extends Enum<?>> clazz) {
        return Arrays.asList(clazz.getEnumConstants()).stream().map(Enum::name)
                        .collect(Collectors.toSet());
    }
}
