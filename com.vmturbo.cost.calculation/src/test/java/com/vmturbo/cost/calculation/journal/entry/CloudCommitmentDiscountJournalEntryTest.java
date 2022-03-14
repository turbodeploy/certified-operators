package com.vmturbo.cost.calculation.journal.entry;

import static com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType.COMMODITY;
import static com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType.COUPONS;
import static com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType.SPEND_COMMITMENT;
import static com.vmturbo.common.protobuf.cost.Cost.CostCategory.ON_DEMAND_COMPUTE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.CLOUD_COMMITMENT;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.vmturbo.cloud.common.commitment.TopologyCommitmentData;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageVector;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.topology.TopologyDTO;

/**
 * Test for CloudCommitmentDiscountJournalEntry.
 */
public class CloudCommitmentDiscountJournalEntryTest {

    /**
     * Test commitment type SPEND_COMMITMENT not supported.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testSpendCommitmentNotSupported() {

        TopologyDTO.TopologyEntityDTO commitmentEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setEntityType(CLOUD_COMMITMENT.getNumber())
                        .setOid(123)
                        .setTypeSpecificInfo(TopologyDTO.TypeSpecificInfo.newBuilder()
                                                             .setCloudCommitmentData(
                                                                             TopologyDTO.TypeSpecificInfo
                                                                                             .CloudCommitmentInfo
                                                                                             .newBuilder()))
                        .build();
        final TopologyCommitmentData commitmentData = TopologyCommitmentData.builder()
                        .commitment(commitmentEntity).build();


        CloudCommitmentCoverageVector coresCoverageVector = CloudCommitmentCoverageVector
                        .newBuilder().setVectorType(
                                        CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo.newBuilder()
                                                        .setCoverageType(SPEND_COMMITMENT))
                        .setCapacity(1)
                        .setUsed(1)
                        .build();

        CloudCommitmentDiscountJournalEntry subject = new CloudCommitmentDiscountJournalEntry(commitmentData, coresCoverageVector, ON_DEMAND_COMPUTE, Cost.CostSource.CLOUD_COMMITMENT_DISCOUNT);
        assertNull("Object creation should have thrown an exception", subject);
    }

    /**
     * Test commitment type COUPONS not supported.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCouponCommitmentNotSupported() {

        TopologyDTO.TopologyEntityDTO commitmentEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setEntityType(CLOUD_COMMITMENT.getNumber())
                        .setOid(123)
                        .setTypeSpecificInfo(TopologyDTO.TypeSpecificInfo.newBuilder()
                                                             .setCloudCommitmentData(
                                                                             TopologyDTO.TypeSpecificInfo
                                                                                             .CloudCommitmentInfo
                                                                                             .newBuilder()))
                        .build();
        final TopologyCommitmentData commitmentData = TopologyCommitmentData.builder()
                        .commitment(commitmentEntity).build();


        CloudCommitmentCoverageVector coresCoverageVector = CloudCommitmentCoverageVector
                        .newBuilder().setVectorType(
                                        CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo.newBuilder()
                                                        .setCoverageType(COUPONS))
                        .setCapacity(1)
                        .setUsed(1)
                        .build();

        CloudCommitmentDiscountJournalEntry subject = new CloudCommitmentDiscountJournalEntry(commitmentData, coresCoverageVector, ON_DEMAND_COMPUTE, Cost.CostSource.CLOUD_COMMITMENT_DISCOUNT);
        assertNull("Object creation should have thrown an exception", subject);
    }

    /**
     * Test Coverage Vector with zero Capacity not supported.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testZeroCoverageCapacityNotSupported() {
        TopologyDTO.TopologyEntityDTO commitmentEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setEntityType(CLOUD_COMMITMENT.getNumber())
                        .setOid(123)
                        .setTypeSpecificInfo(TopologyDTO.TypeSpecificInfo.newBuilder()
                                                             .setCloudCommitmentData(
                                                                             TopologyDTO.TypeSpecificInfo
                                                                                             .CloudCommitmentInfo
                                                                                             .newBuilder()))
                        .build();
        final TopologyCommitmentData commitmentData = TopologyCommitmentData.builder()
                        .commitment(commitmentEntity).build();


        CloudCommitmentCoverageVector coresCoverageVector = CloudCommitmentCoverageVector
                        .newBuilder().setVectorType(
                                        CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo.newBuilder()
                                                        .setCoverageType(COMMODITY))
                        .setCapacity(0)
                        .setUsed(1)
                        .build();

        CloudCommitmentDiscountJournalEntry subject = new CloudCommitmentDiscountJournalEntry(commitmentData, coresCoverageVector, ON_DEMAND_COMPUTE, Cost.CostSource.CLOUD_COMMITMENT_DISCOUNT);
        assertNull("Object creation should have thrown an exception", subject);
    }

    /**
     * Test Coverage Vector with zero Used not supported.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testZeroCoverageUsedNotSupported() {
        TopologyDTO.TopologyEntityDTO commitmentEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setEntityType(CLOUD_COMMITMENT.getNumber())
                        .setOid(123)
                        .setTypeSpecificInfo(TopologyDTO.TypeSpecificInfo.newBuilder()
                                                             .setCloudCommitmentData(
                                                                             TopologyDTO.TypeSpecificInfo
                                                                                             .CloudCommitmentInfo
                                                                                             .newBuilder()))
                        .build();
        final TopologyCommitmentData commitmentData = TopologyCommitmentData.builder()
                        .commitment(commitmentEntity).build();


        CloudCommitmentCoverageVector coresCoverageVector = CloudCommitmentCoverageVector
                        .newBuilder().setVectorType(
                                        CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo.newBuilder()
                                                        .setCoverageType(COMMODITY))
                        .setCapacity(1)
                        .setUsed(0)
                        .build();

        CloudCommitmentDiscountJournalEntry subject = new CloudCommitmentDiscountJournalEntry(commitmentData, coresCoverageVector, ON_DEMAND_COMPUTE, Cost.CostSource.CLOUD_COMMITMENT_DISCOUNT);
        assertNull("Object creation should have thrown an exception", subject);
    }
}
