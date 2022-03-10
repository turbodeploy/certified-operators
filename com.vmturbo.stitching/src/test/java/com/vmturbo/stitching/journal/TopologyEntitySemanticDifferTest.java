package com.vmturbo.stitching.journal;

import static org.junit.Assert.assertEquals;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.PerTargetEntityInformationView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.DiscoveryOriginImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.OriginImpl;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation;

public class TopologyEntitySemanticDifferTest {
    private final TopologyEntity.Builder a = entity("a", 1L, 11L);
    private final TopologyEntity.Builder b = entity("b", 2L, 12L);
    private final TopologyEntity.Builder c = entity("c", 3L, 13L);
    private final TopologyEntity.Builder d = entity("d", 4L, 14L);
    private final TopologyEntity.Builder e = entity("e", 5L, 15L);
    private TopologyEntity entity;

    final TopologyEntitySemanticDiffer diff =
        new TopologyEntitySemanticDiffer(Verbosity.LOCAL_CONTEXT_VERBOSITY);

    @Before
    public void setup() {
        a.addConsumer(b);
        a.addConsumer(e);
        a.addConsumer(e);

        a.addProvider(c);
        a.addProvider(d);

        a.addConsumer(b);

        a.getTopologyEntityImpl()
            .addCommoditySoldList(new CommoditySoldImpl()
                .setCommodityType(new CommodityTypeImpl()
                    .setType(CommodityDTO.CommodityType.VSTORAGE_VALUE)
                    .setKey("storage-key"))
                .setCapacity(1000.0)
                .setUsed(123.4)
                .setPeak(400.0));

        entity = a.build();
    }

    @Test
    public void testUnchanged() {
        // When no changes, should return an empty string.
        assertEquals("", diff.semanticDiff(entity, entity, FormatRecommendation.PRETTY));
    }

    private static TopologyEntity.Builder entity(@Nonnull final String name,
                                                 final long oid,
                                                 final long targetId) {
        final TopologyEntityImpl entityImpl = new TopologyEntityImpl()
            .setOid(oid)
            .setDisplayName(name)
            .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
            .setOrigin(new OriginImpl()
                    .setDiscoveryOrigin(new DiscoveryOriginImpl()
                        .setLastUpdatedTime(123456789L)
                        .putDiscoveredTargetData(targetId, PerTargetEntityInformationView.getDefaultInstance()))
            );

        return TopologyEntity.newBuilder(entityImpl);
    }
}