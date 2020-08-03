package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessAccountData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier.PricingIdentifierName;
import com.vmturbo.topology.processor.conversions.SdkToTopologyEntityConverter;

/**
 * Class to test the functionality of {@link BusinessAccountInfoMapper}.
 */
public class BusinessAccountInfoMapperTest {
    private static final Boolean DATA_DISCOVERED_FLAG = Boolean.TRUE;
    private static final long DISCOVERING_TARGET_ID = 1L;
    private static final String SUBSCRIPTION_ID = "subId";
    private static final String OFFER_ID = "Offer ID";
    private static final String ENROLLMENT_NUMBER = "Enrollment Number";
    private static final Map<String, String> ENTITY_PROP_MAP =
        ImmutableMap.of(SdkToTopologyEntityConverter.DISCOVERING_TARGET_ID,
            String.valueOf(DISCOVERING_TARGET_ID));

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
                .setAssociatedTargetId(DISCOVERING_TARGET_ID)
                .setAccountId(SUBSCRIPTION_ID)
                .setRiSupported(true)
                .addAllPricingIdentifiers(businessAccountEntityDTO.getBusinessAccountData()
                    .getPricingIdentifiersList())
                .build())
            .build();
        final BusinessAccountInfoMapper testBuilder = new BusinessAccountInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder
            .mapEntityDtoToTypeSpecificInfo(businessAccountEntityDTO, ENTITY_PROP_MAP);
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
                .setAssociatedTargetId(DISCOVERING_TARGET_ID)
                .setRiSupported(true)
                .setAccountId(SUBSCRIPTION_ID)
                .build())
            .build();
        final BusinessAccountInfoMapper testBuilder = new BusinessAccountInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder
            .mapEntityDtoToTypeSpecificInfo(businessAccountEntityDTO, ENTITY_PROP_MAP);
        // assert
        assertThat(result, equalTo(expected));
    }

    /**
     * Test the case where DATA_DISCOVERED_FLAG is not set.
     */
    @Test
    public void testExtractTypeSpecificInfoWithoutDataDiscovered() {
        // arrange
        final EntityDTOOrBuilder businessAccountEntityDTO = EntityDTO.newBuilder()
            .setBusinessAccountData(BusinessAccountData.newBuilder()
                .setAccountId(SUBSCRIPTION_ID)
                .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
            .setBusinessAccount(BusinessAccountInfo.newBuilder()
                .setAccountId(SUBSCRIPTION_ID)
                .setRiSupported(true)
                .build())
            .build();
        final BusinessAccountInfoMapper testBuilder = new BusinessAccountInfoMapper();
        // act
        TypeSpecificInfo result = testBuilder
            .mapEntityDtoToTypeSpecificInfo(businessAccountEntityDTO, ENTITY_PROP_MAP);
        // assert
        assertThat(result, equalTo(expected));
    }
}
