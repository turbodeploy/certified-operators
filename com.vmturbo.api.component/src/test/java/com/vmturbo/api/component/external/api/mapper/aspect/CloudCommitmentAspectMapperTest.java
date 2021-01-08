package com.vmturbo.api.component.external.api.mapper.aspect;

import org.junit.Test;

import com.vmturbo.api.dto.entityaspect.CloudCommitmentAspectApiDTO;
import com.vmturbo.api.enums.CloudCommitmentScopeType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.FamilyRestricted;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;

/**
 * Tests the CloudCommitmentAspectMapper.
 */
public class CloudCommitmentAspectMapperTest extends BaseAspectMapperTest {
    private final long startTimeInMiliSeconds = 1609872215908L;
    private final long endTimeInMiliSeconds = 1609873415908L;

    private final long termInMiliseconds = 31540000000L;

    private final double spend = 6000d;

    private final String instanceFamily = "m4.large";

    /**
     * Tests the entity to aspect api dto conversion.
     *
     * @throws ConversionException A conversion exception.
     * @throws InterruptedException An interrupted exception.
     */
    @Test
    public void testMapEntityToAspect() throws ConversionException, InterruptedException {
        final TopologyDTO.TypeSpecificInfo typeSpecificInfo = TopologyDTO.TypeSpecificInfo.newBuilder()
                .setCloudCommitmentData(CloudCommitmentInfo.newBuilder().setStartTimeMilliseconds(startTimeInMiliSeconds)
                .setExpirationTimeMilliseconds(endTimeInMiliSeconds).setSpend(
                                CurrencyAmount.newBuilder().setAmount(spend).build()).setFamilyRestricted(
                                FamilyRestricted.newBuilder().setInstanceFamily(instanceFamily).build()).setPayment(
                                PaymentOption.ALL_UPFRONT).setTermMilliseconds(termInMiliseconds)).build();

        final CloudCommitmentAspectMapper cloudCommitmentAspectMapper = new CloudCommitmentAspectMapper();
        final TopologyEntityDTO topologyEntityDTO = topologyEntityDTOBuilder(
                EntityType.CLOUD_COMMITMENT, typeSpecificInfo).build();
        final CloudCommitmentAspectApiDTO cloudCommitmentAspectApiDTO = cloudCommitmentAspectMapper.mapEntityToAspect(topologyEntityDTO);
        assert (cloudCommitmentAspectApiDTO.getStartTimeInMilliseconds()).equals(startTimeInMiliSeconds);
        assert (cloudCommitmentAspectApiDTO.getExpirationTimeInMilliseconds()).equals(endTimeInMiliSeconds);
        assert (cloudCommitmentAspectApiDTO.getCloudCommitmentScopeType() == CloudCommitmentScopeType.FamilyScoped);
        assert (cloudCommitmentAspectApiDTO.getPayment() == com.vmturbo.api.enums.PaymentOption.ALL_UPFRONT);
        assert (cloudCommitmentAspectApiDTO.getCloudCommitmentCapacityApiDTO().getSpendCapacity() == spend);
    }
}
