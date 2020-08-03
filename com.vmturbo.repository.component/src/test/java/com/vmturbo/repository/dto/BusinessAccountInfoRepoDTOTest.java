package com.vmturbo.repository.dto;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.PricingIdentifier.PricingIdentifierName;

/**
 * Class to test BusinessAccountInfoRepo conversion to and from TypeSpecificInfo.
 */
public class BusinessAccountInfoRepoDTOTest {
    private static final String SUBSCRIPTION_ID = "sub_id";
    private static final String OFFER_ID = "offer_id";
    private static final String ENROLLMENT_NUMBER = "enrollment num";
    private static final Long ASSOCIATED_TARGET_ID = 1L;

    /**
     * Test filling a RepoDTO from a TypeSpecificInfo with data fields populated.
     */
    @Test
    public void testFillFromTypeSpecificInfo() {
        // arrange
        final TypeSpecificInfo testInfo = TypeSpecificInfo.newBuilder()
            .setBusinessAccount(
                BusinessAccountInfo.newBuilder()
                    .setAssociatedTargetId(ASSOCIATED_TARGET_ID)
                    .setAccountId(SUBSCRIPTION_ID)
                    .addPricingIdentifiers(PricingIdentifier.newBuilder()
                        .setIdentifierName(PricingIdentifierName.OFFER_ID)
                        .setIdentifierValue(OFFER_ID)
                        .build())
                    .addPricingIdentifiers(PricingIdentifier.newBuilder()
                        .setIdentifierName(PricingIdentifierName.ENROLLMENT_NUMBER)
                        .setIdentifierValue(ENROLLMENT_NUMBER)
                        .build())
                    .build())
            .build();
        final ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
        final BusinessAccountInfoRepoDTO testBusinessAccountRepoDTO =
            new BusinessAccountInfoRepoDTO();
        // act
        testBusinessAccountRepoDTO.fillFromTypeSpecificInfo(testInfo, serviceEntityRepoDTO);
        // assert
        assertEquals(SUBSCRIPTION_ID, testBusinessAccountRepoDTO.getAccountId());
        assertEquals(OFFER_ID, testBusinessAccountRepoDTO.getPricingIdentifiers().stream()
            .filter(pricingId -> pricingId.getIdentifierName() == PricingIdentifierName.OFFER_ID)
            .map(PricingIdentifierRepoDTO::getIdentifierValue)
            .findFirst()
            .get());
        assertEquals(ENROLLMENT_NUMBER, testBusinessAccountRepoDTO.getPricingIdentifiers().stream()
            .filter(pricingId -> pricingId.getIdentifierName() == PricingIdentifierName.ENROLLMENT_NUMBER)
            .map(PricingIdentifierRepoDTO::getIdentifierValue)
            .findFirst()
            .get());
        assertEquals(ASSOCIATED_TARGET_ID, testBusinessAccountRepoDTO.getAssociatedTargetId());
    }

    /**
     * Test filling a RepoDTO from an empty TypeSpecificInfo.
     */
    @Test
    public void testFillFromEmptyTypeSpecificInfo() {
        // arrange
        TypeSpecificInfo testInfo = TypeSpecificInfo.newBuilder()
            .build();
        ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
        final BusinessAccountInfoRepoDTO testBusinessAccountRepoDTO =
            new BusinessAccountInfoRepoDTO();
        // act
        testBusinessAccountRepoDTO.fillFromTypeSpecificInfo(testInfo, serviceEntityRepoDTO);
        // assert
        assertNull(testBusinessAccountRepoDTO.getAccountId());
        assertNull(testBusinessAccountRepoDTO.getPricingIdentifiers());
        assertNull(testBusinessAccountRepoDTO.getAssociatedTargetId());
    }

    /**
     * Test extracting a TypeSpecificInfo from a RepoDTO.
     */
    @Test
    public void testCreateFromRepoDTO() {
        // arrange
        final BusinessAccountInfoRepoDTO testBusinessAccountRepoDTO =
            new BusinessAccountInfoRepoDTO();
        testBusinessAccountRepoDTO.setAccountId(SUBSCRIPTION_ID);
        List<PricingIdentifierRepoDTO> pricingIdList = Lists.newArrayList();
        pricingIdList.add(new PricingIdentifierRepoDTO(PricingIdentifierName.OFFER_ID, OFFER_ID));
        pricingIdList.add(new PricingIdentifierRepoDTO(PricingIdentifierName.ENROLLMENT_NUMBER,
            ENROLLMENT_NUMBER));
        testBusinessAccountRepoDTO.setAssociatedTargetId(ASSOCIATED_TARGET_ID);
        testBusinessAccountRepoDTO.setPricingIdentifiers(pricingIdList);
        BusinessAccountInfo expected = BusinessAccountInfo.newBuilder()
            .setAssociatedTargetId(ASSOCIATED_TARGET_ID)
            .setAccountId(SUBSCRIPTION_ID)
            .addPricingIdentifiers(PricingIdentifier.newBuilder()
                .setIdentifierName(PricingIdentifierName.OFFER_ID)
                .setIdentifierValue(OFFER_ID)
                .build())
            .addPricingIdentifiers(PricingIdentifier.newBuilder()
                .setIdentifierName(PricingIdentifierName.ENROLLMENT_NUMBER)
                .setIdentifierValue(ENROLLMENT_NUMBER)
                .build())
            .build();
        // act
        final TypeSpecificInfo result = testBusinessAccountRepoDTO.createTypeSpecificInfo();
        // assert
        assertTrue(result.hasBusinessAccount());
        final BusinessAccountInfo  businessAccountInfo = result.getBusinessAccount();
        assertThat(businessAccountInfo, equalTo(expected));
    }

    /**
     * Test extracting a TypeSpecificInfo from an empty RepoDTO.
     */
    @Test
    public void testCreateFromEmptyRepoDTO() {
        // arrange
        final BusinessAccountInfoRepoDTO testBusinessAccountRepoDTO =
            new BusinessAccountInfoRepoDTO();
        BusinessAccountInfo expected = BusinessAccountInfo.newBuilder()
            .build();
        // act
        TypeSpecificInfo result = testBusinessAccountRepoDTO.createTypeSpecificInfo();
        // assert
        assertTrue(result.hasBusinessAccount());
        final BusinessAccountInfo businessAccountInfo = result.getBusinessAccount();
        assertEquals(expected, businessAccountInfo);
    }
}
