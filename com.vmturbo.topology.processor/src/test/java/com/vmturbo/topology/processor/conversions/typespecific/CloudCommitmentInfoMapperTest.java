package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Collections;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CloudCommitmentInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentScope;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentStatus;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommoditiesBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.FamilyRestricted;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.ProviderType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.ServiceRestricted;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;

/**
 * Class for testing the cloud commitment info mapper.
 */
public class CloudCommitmentInfoMapperTest {

    private static final Long EXPIRATION_TIME_IN_MILLIS = 1614610963L;

    private static final Long START_TIME_IN_MILLIS = 1583074963L;

    private static final PaymentOption PAYMENT_OPTION = PaymentOption.ALL_UPFRONT;

    private static final String INSTANCE_FAMILY = "d-Series";

    private static final CurrencyAmount.Builder SPEND_COMMITMENT =
            CurrencyAmount.newBuilder().setAmount(200000d);

    private static final CommittedCommoditiesBought.Builder COMMODITIES_BOUGHT_CAPACITY =
            CommittedCommoditiesBought.newBuilder().addCommodity(
                    CommittedCommodityBought.newBuilder()
                            .setCapacity(123)
                            .setCommodityType(CommodityType.MEM_ALLOCATION));

    /**
     * Test for {@link CloudCommitmentInfoMapper#mapEntityDtoToTypeSpecificInfo}.
     */
    @Test
    public void testMapEntityDtoToTypeSpecificInfo() {
        final CloudCommitmentInfoMapper cloudCommitmentInfoMapper = new CloudCommitmentInfoMapper();
        Stream.of(ImmutablePair.of(EntityDTO.newBuilder()
                        .setCloudCommitmentData(
                                CloudCommitmentData.newBuilder()
                                        .setExpirationTimeMilliseconds(EXPIRATION_TIME_IN_MILLIS)
                                        .setStartTimeMilliseconds(START_TIME_IN_MILLIS)
                                        .setCommitmentStatus(
                                                CloudCommitmentStatus.CLOUD_COMMITMENT_STATUS_EXPIRED)
                                        .setPayment(PAYMENT_OPTION)
                                        .setProviderSpecificType(ProviderType.SAVINGS_PLAN)
                                        .setCommitmentScope(
                                                CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_RESOURCE_GROUP)
                                        .setFamilyRestricted(FamilyRestricted.newBuilder()
                                                .setInstanceFamily(INSTANCE_FAMILY))),
                TypeSpecificInfo.newBuilder()
                        .setCloudCommitmentData(
                                CloudCommitmentInfo.newBuilder()
                                        .setExpirationTimeMilliseconds(EXPIRATION_TIME_IN_MILLIS)
                                        .setStartTimeMilliseconds(START_TIME_IN_MILLIS)
                                        .setPayment(PAYMENT_OPTION)
                                        .setProviderSpecificType(ProviderType.SAVINGS_PLAN)
                                        .setCommitmentStatus(
                                                CloudCommitmentStatus.CLOUD_COMMITMENT_STATUS_EXPIRED)
                                        .setCommitmentScope(
                                                CloudCommitmentScope.CLOUD_COMMITMENT_SCOPE_RESOURCE_GROUP)
                                        .setFamilyRestricted(FamilyRestricted.newBuilder()
                                                .setInstanceFamily(INSTANCE_FAMILY)))
                        .build()), ImmutablePair.of(EntityDTO.newBuilder()
                        .setCloudCommitmentData(CloudCommitmentData.newBuilder()
                                .setSpend(SPEND_COMMITMENT)
                                .setServiceRestricted(ServiceRestricted.newBuilder())),
                TypeSpecificInfo.newBuilder()
                        .setCloudCommitmentData(CloudCommitmentInfo.newBuilder()
                                .setSpend(SPEND_COMMITMENT)
                                .setServiceRestricted(ServiceRestricted.newBuilder()))
                        .build()), ImmutablePair.of(EntityDTO.newBuilder()
                        .setCloudCommitmentData(CloudCommitmentData.newBuilder()
                                .setCommoditiesBought(COMMODITIES_BOUGHT_CAPACITY)),
                TypeSpecificInfo.newBuilder()
                        .setCloudCommitmentData(CloudCommitmentInfo.newBuilder()
                                .setCommoditiesBought(COMMODITIES_BOUGHT_CAPACITY))
                        .build())).forEach(testCase -> {
            final TypeSpecificInfo actualResult =
                    cloudCommitmentInfoMapper.mapEntityDtoToTypeSpecificInfo(testCase.getLeft(),
                            Collections.emptyMap());
            Assert.assertEquals(testCase.getRight(), actualResult);
        });
    }
}
