package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.FamilyRestricted;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.ServiceRestricted;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;

/**
 * Class for testing the cloud commitment info mapper.
 */
public class CloudCommitmentInfoMapperTest {

    private final Long expirationTimeInMilis = 1614610963L;

    private final Long startTimeInMilis = 1583074963L;

    private final PaymentOption paymentOption = PaymentOption.ALL_UPFRONT;

    private final String instanceFamily = "d-Series";

    /**
     * Testing service restricted cloud commitments.
     */
    @Test
    public void testExtractAllTypeSpecificServiceRestricted() {
        final EntityDTOOrBuilder cloudCommitmentEntityDTO = createCloudCommitmentEntityDTO(false);
        TypeSpecificInfo expected = createTypeSpecificInfo(false);
        final CloudCommitmentInfoMapper cloudCommitmentInfoMapper = new CloudCommitmentInfoMapper();
        TypeSpecificInfo result = cloudCommitmentInfoMapper.mapEntityDtoToTypeSpecificInfo(cloudCommitmentEntityDTO, ImmutableMap
                .of(SupplyChainConstants.NUM_CPUS, "4"));
        assertThat(result, equalTo(expected));
    }

    /**
     * Testing family restricted cloud commitments.
     */
    @Test
    public void testExtractAllTypeSpecificInfoFamilyRestricted() {
        final EntityDTOOrBuilder cloudCommitmentEntityDTO = createCloudCommitmentEntityDTO(true);
        TypeSpecificInfo expected = createTypeSpecificInfo(true);
        final CloudCommitmentInfoMapper cloudCommitmentInfoMapper = new CloudCommitmentInfoMapper();
        TypeSpecificInfo result = cloudCommitmentInfoMapper.mapEntityDtoToTypeSpecificInfo(cloudCommitmentEntityDTO, ImmutableMap
                .of(SupplyChainConstants.NUM_CPUS, "4"));
        assertThat(result, equalTo(expected));
    }

    private EntityDTOOrBuilder createCloudCommitmentEntityDTO(boolean isFamilyRestricted) {
        CloudCommitmentData.Builder cloudCommitmentDataBuilder = CloudCommitmentData.newBuilder()
                .setExpirationTimeMilliseconds(expirationTimeInMilis).setStartTimeMilliseconds(startTimeInMilis)
                .setPayment(paymentOption).setSpend(CurrencyAmount.newBuilder().setAmount(200000d));
        if (isFamilyRestricted) {
            cloudCommitmentDataBuilder.setFamilyRestricted(FamilyRestricted.newBuilder().setInstanceFamily(instanceFamily).build());
        } else {
            cloudCommitmentDataBuilder.setServiceRestricted(ServiceRestricted.newBuilder().build());
        }
        return EntityDTO.newBuilder().setCloudCommitmentData(cloudCommitmentDataBuilder.build());
    }

    private TypeSpecificInfo createTypeSpecificInfo(boolean isFamilyRestricted) {

        CloudCommitmentInfo.Builder builder = CloudCommitmentInfo.newBuilder().setExpirationTimeMilliseconds(expirationTimeInMilis).setStartTimeMilliseconds(startTimeInMilis)
                        .setPayment(paymentOption).setSpend(CurrencyAmount.newBuilder().setAmount(200000d).build());
        if (isFamilyRestricted) {
            builder.setFamilyRestricted(FamilyRestricted.newBuilder().setInstanceFamily(instanceFamily).build());
        } else {
            builder.setServiceRestricted(ServiceRestricted.newBuilder().build());
        }
        return TypeSpecificInfo.newBuilder().setCloudCommitmentData(builder.build()).build();
    }
}
