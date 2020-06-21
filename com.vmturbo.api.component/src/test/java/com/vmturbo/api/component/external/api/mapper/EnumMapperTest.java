package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.*;

import org.junit.Test;

import com.vmturbo.components.common.setting.RISettingsEnum.PreferredOfferingClass;
import com.vmturbo.components.common.setting.RISettingsEnum.PreferredPaymentOption;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.ReservedInstanceData.OfferingType;
import com.vmturbo.platform.sdk.common.CloudCostDTOREST.ReservedInstanceType.PaymentOption;

public class EnumMapperTest {

    private final EnumMapper<PreferredPaymentOption> enumMapper = EnumMapper.of(PreferredPaymentOption.class);



    //Tests valueOf method using different acceptable options for description
    @Test
    public void valueOfShouldReturnTheEnumValueFromStrings() {
        assertEquals(PreferredPaymentOption.ALL_UPFRONT, enumMapper.valueOf("ALL_UPFRONT").orElse(null));
        assertEquals(PreferredPaymentOption.ALL_UPFRONT, enumMapper.valueOf("ALL UPFRONT").orElse(null));
        assertEquals(PreferredPaymentOption.ALL_UPFRONT, enumMapper.valueOf("All Upfront").orElse(null));
        assertEquals(PreferredPaymentOption.ALL_UPFRONT, enumMapper.valueOf("   All Upfront   ").orElse(null));
    }


     //Tests correct Enum const is returned using valueOf
    @Test
    public void valueOfShouldReturnTheEnumValueSimilarEnums() {
        assertEquals(PreferredPaymentOption.ALL_UPFRONT, enumMapper.valueOf(PaymentOption.ALL_UPFRONT).orElse(null));
        assertEquals(PreferredPaymentOption.ALL_UPFRONT, enumMapper.valueOf(OfferingType.ALL_UPFRONT).orElse(null));
        assertEquals(PreferredPaymentOption.ALL_UPFRONT, enumMapper.valueOf(com.vmturbo.api.enums.PaymentOption.ALL_UPFRONT).orElse(null));
    }

    //Tests empty optional returned for unmatched descriptors
    @Test
    public void valueOfShouldReturnEmptyOptionalForUnmatchedDescriptor() {
        assertFalse(enumMapper.valueOf("foo bar").isPresent());
        assertFalse(enumMapper.valueOf(PreferredOfferingClass.CONVERTIBLE).isPresent());
    }


    //Tests null is safely passed to valueOf
    @Test
    public void valueOfShouldBeNullSafe() {
        assertFalse(enumMapper.valueOf((String)null).isPresent());
        assertFalse(enumMapper.valueOf((PreferredOfferingClass)null).isPresent());
    }
}