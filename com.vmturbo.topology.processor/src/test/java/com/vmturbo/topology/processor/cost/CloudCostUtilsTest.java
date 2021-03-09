package com.vmturbo.topology.processor.cost;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DatabaseServerTierData;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;

/**
 *
 */
public class CloudCostUtilsTest {
    @Test
    public void testStorageTierLocalNameToId() {
        Assert.assertEquals("No change", CloudCostUtils.storageTierLocalNameToId("No change", null));
        Assert.assertEquals("aws::ST::ID", CloudCostUtils.storageTierLocalNameToId("id", SDKProbeType.AWS_COST));
        Assert.assertEquals("azure::ST::ID", CloudCostUtils.storageTierLocalNameToId("id", SDKProbeType.AZURE));
    }

    @Test
    public void testDatabaseTierLocalNameToId() {
        Assert.assertEquals("No change", CloudCostUtils.getEntityIdFromDBProfile(
                generateTopologyStitchingEntity("No change"), null));
        Assert.assertEquals("aws::DBSPROFILE::id", CloudCostUtils.getEntityIdFromDBProfile(
                generateTopologyStitchingEntity("id"), SDKProbeType.AWS_COST));
    }

    @Test
    public void testAzureDatabaseTierLocalNameToId() {
        Assert.assertEquals("azure::DBPROFILE::Basic", CloudCostUtils
                .getEntityIdFromDBProfile(generateTopologyStitchingEntity("Basic"), SDKProbeType.AZURE));
        Assert.assertEquals("azure::DBPROFILE::Standard_S6", CloudCostUtils
                .getEntityIdFromDBProfile(generateTopologyStitchingEntity("S6"), SDKProbeType.AZURE));
        Assert.assertEquals("azure::DBPROFILE::Premium_P11", CloudCostUtils
                .getEntityIdFromDBProfile(generateTopologyStitchingEntity("P11"), SDKProbeType.AZURE));
    }

    /**
     * Test aws data server tier name for mapping tier info from mediation and cost without template identifier.
     */
    @Test
    public void testAwsDatabaseServerTierLocalNameToId() {
        Assert.assertEquals("aws::DBSPROFILE::db.m2.medium::gp2", CloudCostUtils
                .getEntityIdFromDBProfile(generateTopologyStitchingEntity("db.m2.medium::gp2"), SDKProbeType.AWS));
    }

    /**
     * Test aws data server tier name for mapping tier info from mediation and cost.
     */
    @Test
    public void testAwsDatabaseServerTierWithTemplateIdentifier() {
        EntityDTO.Builder entityDTO = EntityBuilders.database("foo")
                .displayName("db.t2.24xlarge")
                .build().toBuilder()
                .setDatabaseServerTierData(DatabaseServerTierData
                        .newBuilder().setTemplateIdentifier("db.t2.24xlarge::io1").build());
        TopologyStitchingEntity topologyStitchingEntity = new TopologyStitchingEntity(entityDTO, 1L, 01L, 1L);
        Assert.assertEquals("aws::DBSPROFILE::db.t2.24xlarge::io1", CloudCostUtils
                .getEntityIdFromDBProfile(topologyStitchingEntity, SDKProbeType.AWS));
    }

    private static TopologyStitchingEntity generateTopologyStitchingEntity(String displayName) {
        EntityDTO.Builder entityDTO = EntityBuilders.database("foo")
                .displayName(displayName).build().toBuilder();
        return new TopologyStitchingEntity(entityDTO, 1L, 01L, 1L);
    }
}
