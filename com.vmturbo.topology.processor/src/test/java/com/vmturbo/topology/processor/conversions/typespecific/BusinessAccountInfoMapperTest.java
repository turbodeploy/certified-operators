package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessAccountData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessAccountData.PricingIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessAccountData.PricingIdentifier.PricingIdentifierName;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Class to test the functionality of {@link BusinessAccountInfoMapper}.
 */
public class BusinessAccountInfoMapperTest {
    private static final Boolean DATA_DISCOVERED_FLAG = Boolean.TRUE;
    private static final String SUBSCRIPTION_ID = "subId";
    private static final String OFFER_ID = "Offer ID";
    private static final String ENROLLMENT_NUMBER = "Enrollment Number";

    /**
     * Test that we can properly extract all fields from business account EntityDTO.
     */
    @Test
    public void testExtractAllTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder businessAccountEntityDTO = EntityDTO.newBuilder()
            .setBusinessAccountData(BusinessAccountData.newBuilder()
                .setDataDiscovered(DATA_DISCOVERED_FLAG)
                .setAccountId(SUBSCRIPTION_ID)
                .addPricingIdentifiers(PricingIdentifier.newBuilder()
                    .setIdentifierName(PricingIdentifierName.OFFER_ID)
                    .setIdentifierValue(OFFER_ID)
                    .build())
                .addPricingIdentifiers(PricingIdentifier.newBuilder()
                    .setIdentifierName(PricingIdentifierName.ENROLLMENT_NUMBER)
                    .setIdentifierValue(ENROLLMENT_NUMBER)
                    .build())
                .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
            .setBusinessAccount(BusinessAccountInfo.newBuilder()
                .setHasAssociatedTarget(DATA_DISCOVERED_FLAG)
                .setAccountId(SUBSCRIPTION_ID)
                .addAllPricingIdentifiers(businessAccountEntityDTO.getBusinessAccountData()
                    .getPricingIdentifiersList())
                .build())
            .build();
        final BusinessAccountInfoMapper testBuilder = new BusinessAccountInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder
            .mapEntityDtoToTypeSpecificInfo(businessAccountEntityDTO, Collections.emptyMap());
        // assert
        assertThat(result, equalTo(expected));
    }

    /**
     * Test the case where the EntityDTO for a business account only has subscription ID and
     * data discovered set.
     */
    @Test
    public void testExtractPartialTypeSpecificInfo() {
        // arrange
        final EntityDTOOrBuilder businessAccountEntityDTO = EntityDTO.newBuilder()
            .setBusinessAccountData(BusinessAccountData.newBuilder()
                .setDataDiscovered(DATA_DISCOVERED_FLAG)
                .setAccountId(SUBSCRIPTION_ID)
                .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
            .setBusinessAccount(BusinessAccountInfo.newBuilder()
                .setHasAssociatedTarget(DATA_DISCOVERED_FLAG)
                .setAccountId(SUBSCRIPTION_ID)
                .build())
            .build();
        final BusinessAccountInfoMapper testBuilder = new BusinessAccountInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder
            .mapEntityDtoToTypeSpecificInfo(businessAccountEntityDTO, Collections.emptyMap());
        // assert
        assertThat(result, equalTo(expected));
    }
}
