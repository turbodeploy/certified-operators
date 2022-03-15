package com.vmturbo.cost.calculation;

import static com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import static com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import static com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import static com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageVector;
import static com.vmturbo.common.protobuf.cost.Cost.CostCategory.ON_DEMAND_COMPUTE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.MEM_PROVISIONED;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.NUM_VCORE;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import junit.framework.TestCase;

import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.cloud.common.commitment.CloudCommitmentTopology;
import com.vmturbo.cloud.common.commitment.CloudCommitmentTopology.CloudCommitmentTopologyFactory;
import com.vmturbo.cloud.common.commitment.TopologyCommitmentData;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentMapping;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration.Verbosity;
import com.vmturbo.cost.calculation.CloudCommitmentApplicator.CloudCommitmentApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommoditiesBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CommittedCommodityBought;
import com.vmturbo.trax.Trax;
import com.vmturbo.trax.TraxConfiguration;
import com.vmturbo.trax.TraxConfiguration.TraxContext;

/**
 * Tests for the CloudCommitmentApplicator.
 */
public class CloudCommitmentApplicatorTest extends TestCase {

    private CloudCommitmentTopologyFactory<TestEntityClass> topologyFactory = mock(CloudCommitmentTopologyFactory.class);

    private CloudCommitmentApplicatorFactory<TestEntityClass> applicatorFactory = CloudCommitmentApplicator.newFactory(topologyFactory);

    private CostJournal.Builder<TestEntityClass> costJournal = mock(CostJournal.Builder.class);

    private EntityInfoExtractor<TestEntityClass> infoExtractor = mock(EntityInfoExtractor.class);

    private CloudCostData cloudCostData = mock(CloudCostData.class);

    private CloudTopology<TestEntityClass> cloudTopology = mock(CloudTopology.class);

    private CloudCommitmentTopology commitmentTopology = mock(CloudCommitmentTopology.class);

    /**
     * Setup trax configuration.
     */
    @BeforeClass
    public static void setupTraxConfiguration() {
        // Use trax to make it easier to debug wrong values.
        TraxConfiguration.configureTopics(ReservedInstanceApplicatorTest.class.getName(), Verbosity.TRACE);
    }

    /**
     * Clean up Trax configuration.
     */
    @AfterClass
    public static void clearTraxConfiguration() {
        TraxConfiguration.clearAllConfiguration();
    }

    private TraxContext traxContext;

    /**
     * Create a {@link TraxContext}.
     */
    @Before
    public void setup() {
        traxContext = Trax.track(getClass().getName());
    }

    /**
     * Clean up the {@link TraxContext}.
     */
    @After
    public void teardown() {
        traxContext.close();
    }

    /**
     * Test that recordCloudCommitmentDiscount is called for all commitments
     * and for multiple commodities.
     */
    @Test
    public void testAllCloudCommitmentDiscountsAreRecorded() {
        // Set up subject under test
        when(topologyFactory.createTopology(any())).thenReturn(commitmentTopology);
        final CloudCommitmentApplicator<TestEntityClass> subject =
                        applicatorFactory.newCloudCommitmentApplicator(costJournal, infoExtractor,
                                                                       cloudCostData, cloudTopology);


        // Set up the commodities
        CommodityType commodityOneType = MEM_PROVISIONED;
        CloudCommitmentCoverageTypeInfo commodityOneCoverageType = CloudCommitmentCoverageTypeInfo
                        .newBuilder()
                        .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                        .setCoverageSubtype(commodityOneType.getNumber())
                        .build();
        CommodityType commodityTwoType = NUM_VCORE;
        CloudCommitmentCoverageTypeInfo commodityTwoCoverageType = CloudCommitmentCoverageTypeInfo
                        .newBuilder()
                        .setCoverageType(CloudCommitmentCoverageType.COMMODITY)
                        .setCoverageSubtype(commodityTwoType.getNumber())
                        .build();

        // Set up the entity
        long entityId = 123L;
        when(infoExtractor.getId(any())).thenReturn(entityId);
        double entityCommodityOneCapacity = 2D;
        when(commitmentTopology.getCoverageCapacityForEntity(entityId, commodityOneCoverageType))
                        .thenReturn(entityCommodityOneCapacity);
        double entityCommodityTwoCapacity = 4D;
        when(commitmentTopology.getCoverageCapacityForEntity(entityId, commodityTwoCoverageType))
                        .thenReturn(entityCommodityTwoCapacity);

        // Set up commitment one
        long commitmentOneId = 234L;
        CloudCommitmentData cloudCommitmentOneData = mock(TopologyCommitmentData.class);
        when(cloudCostData.getExistingCloudCommitmentData(commitmentOneId))
                        .thenReturn(Optional.of(cloudCommitmentOneData));
        when(cloudCommitmentOneData.asTopologyCommitment())
                        .thenReturn((TopologyCommitmentData)cloudCommitmentOneData);

        // Set up commodities for commitment one
        double commitmentOneCommodityOneCapacity = 1D;
        CloudCommitmentMapping commitmentOneMappingOne = getCloudCommitmentMapping(
                        commitmentOneId,
                        commitmentOneCommodityOneCapacity,
                        commodityOneType);
        double commitmentOneCommodityTwoCapacity = 1D;
        CloudCommitmentMapping commitmentOneMappingTwo = getCloudCommitmentMapping(
                        commitmentOneId,
                        commitmentOneCommodityTwoCapacity,
                        commodityTwoType);

        // Set up commitment two
        long commitmentTwoId = 346L;
        CloudCommitmentData cloudCommitmentTwoData = mock(TopologyCommitmentData.class);
        when(cloudCostData.getExistingCloudCommitmentData(commitmentTwoId))
                        .thenReturn(Optional.of(cloudCommitmentTwoData));
        when(cloudCommitmentTwoData.asTopologyCommitment())
                        .thenReturn((TopologyCommitmentData)cloudCommitmentTwoData);

        double commitmentTwoCommodityOneCapacity = 1D;
        CloudCommitmentMapping commitmentTwoMappingOne = getCloudCommitmentMapping(
                        commitmentTwoId,
                        commitmentTwoCommodityOneCapacity,
                        commodityOneType);

        // Set up the cloudCommitmentMapping
        SetMultimap<Long, CloudCommitmentMapping> cloudCommitmentMapping = ImmutableSetMultimap.of(
                        entityId, commitmentOneMappingOne,
                        entityId, commitmentOneMappingTwo,
                        entityId, commitmentTwoMappingOne);
        when(cloudCostData.getCloudCommitmentMappingByEntityId())
                        .thenReturn(cloudCommitmentMapping);

        // Action under test
        subject.recordCloudCommitmentCoverage();

        // Set up expected coverage vectors
        final CloudCommitmentCoverageVector commitmentOneCommodityOneCoverageVector =
                        CloudCommitmentCoverageVector
                                        .newBuilder()
                                        .setCapacity(entityCommodityOneCapacity)
                                        .setUsed(commitmentOneCommodityOneCapacity)
                                        .setVectorType(commodityOneCoverageType)
                                        .build();
        final CloudCommitmentCoverageVector commitmentOneCommodityTwoCoverageVector =
                        CloudCommitmentCoverageVector
                                        .newBuilder()
                                        .setCapacity(entityCommodityTwoCapacity)
                                        .setUsed(commitmentOneCommodityTwoCapacity)
                                        .setVectorType(commodityTwoCoverageType)
                                        .build();
        final CloudCommitmentCoverageVector commitmentTwoCommodityOneCoverageVector =
                        CloudCommitmentCoverageVector
                                        .newBuilder()
                                        .setCapacity(entityCommodityOneCapacity)
                                        .setUsed(commitmentTwoCommodityOneCapacity)
                                        .setVectorType(commodityOneCoverageType)
                                        .build();

        verify(costJournal).recordCloudCommitmentDiscount(
                        ON_DEMAND_COMPUTE,
                        (TopologyCommitmentData)cloudCommitmentOneData,
                        commitmentOneCommodityOneCoverageVector);
        verify(costJournal).recordCloudCommitmentDiscount(
                        ON_DEMAND_COMPUTE,
                        (TopologyCommitmentData)cloudCommitmentOneData,
                        commitmentOneCommodityTwoCoverageVector);
        verify(costJournal).recordCloudCommitmentDiscount(
                        ON_DEMAND_COMPUTE,
                        (TopologyCommitmentData)cloudCommitmentTwoData,
                        commitmentTwoCommodityOneCoverageVector);
    }

    @NotNull private CloudCommitmentMapping getCloudCommitmentMapping(long commitmentOid,
                                                                      double capacity,
                                                                      CommodityType commodityType) {
        CommittedCommoditiesBought commoditiesBought = CommittedCommoditiesBought
                        .newBuilder()
                        .addCommodity(CommittedCommodityBought
                                                      .newBuilder()
                                                      .setCommodityType(commodityType)
                                                      .setCapacity(capacity))
                        .build();
        CloudCommitmentAmount commitmentAmount = CloudCommitmentAmount.newBuilder()
                                        .setCommoditiesBought(commoditiesBought)
                                        .build();
        return CloudCommitmentMapping.newBuilder()
                        .setCloudCommitmentOid(commitmentOid)
                        .setCommitmentAmount(commitmentAmount)
                        .build();
    }
}
