package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.FamilyRestricted;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.ProviderType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.ServiceRestricted;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;

/**
 * Class for testing the cloud commitment info mapper.
 */
public class CloudCommitmentInfoMapperTest {

    private static final Long EXPIRATION_TIME_MILLIS = 1614610963L;
    private static final Long START_TIME_MILLIS = 1583074963L;
    private static final PaymentOption PAYMENT_OPTION = PaymentOption.ALL_UPFRONT;
    private static final String INSTANCE_FAMILY = "d-Series";
    private static final Map<String, String> PROPERTY_MAP = ImmutableMap.of("foo", "bar");

    /**
     * Testing service restricted cloud commitments.
     */
    @Test
    public void testExtractAllTypeSpecificServiceRestricted() {
        final EntityDTOOrBuilder cloudCommitmentEntityDTO =
                EntityDTO.newBuilder().setCloudCommitmentData(createCloudCommitmentDataBuilder()
                        .setServiceRestricted(ServiceRestricted.newBuilder())
                        .setProviderSpecificType(ProviderType.SAVINGS_PLAN));
        final TypeSpecificInfo.Builder expected =
                TypeSpecificInfo.newBuilder().setCloudCommitmentData(createCloudCommitmentInfo()
                        .setServiceRestricted(ServiceRestricted.getDefaultInstance())
                        .setProviderSpecificType(ProviderType.SAVINGS_PLAN));
        final CloudCommitmentInfoMapper cloudCommitmentInfoMapper = new CloudCommitmentInfoMapper();
        final TypeSpecificInfo result = cloudCommitmentInfoMapper.mapEntityDtoToTypeSpecificInfo(
                cloudCommitmentEntityDTO, PROPERTY_MAP);
        assertThat(result, equalTo(expected.build()));
    }

    /**
     * Testing family restricted cloud commitments.
     */
    @Test
    public void testExtractAllTypeSpecificInfoFamilyRestricted() {
        final EntityDTOOrBuilder cloudCommitmentEntityDTO =
                EntityDTO.newBuilder().setCloudCommitmentData(createCloudCommitmentDataBuilder()
                        .setFamilyRestricted(FamilyRestricted.newBuilder()
                                .setInstanceFamily(INSTANCE_FAMILY)));
        final TypeSpecificInfo.Builder expected =
                TypeSpecificInfo.newBuilder().setCloudCommitmentData(createCloudCommitmentInfo()
                        .setFamilyRestricted(FamilyRestricted.newBuilder()
                                .setInstanceFamily(INSTANCE_FAMILY)));
        final CloudCommitmentInfoMapper cloudCommitmentInfoMapper = new CloudCommitmentInfoMapper();
        final TypeSpecificInfo result = cloudCommitmentInfoMapper.mapEntityDtoToTypeSpecificInfo(
                cloudCommitmentEntityDTO, PROPERTY_MAP);
        assertThat(result, equalTo(expected.build()));
    }

    private CloudCommitmentData.Builder createCloudCommitmentDataBuilder() {
        return CloudCommitmentData.newBuilder()
                .setExpirationTimeMilliseconds(EXPIRATION_TIME_MILLIS)
                .setStartTimeMilliseconds(START_TIME_MILLIS)
                .setPayment(PAYMENT_OPTION)
                .setSpend(CurrencyAmount.newBuilder().setAmount(200000d));
    }

    private CloudCommitmentInfo.Builder createCloudCommitmentInfo() {
        return CloudCommitmentInfo.newBuilder()
                .setExpirationTimeMilliseconds(EXPIRATION_TIME_MILLIS)
                .setStartTimeMilliseconds(START_TIME_MILLIS)
                .setPayment(PAYMENT_OPTION)
                .setSpend(CurrencyAmount.newBuilder().setAmount(200000d));
    }
}
