package com.vmturbo.topology.processor.template;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VSTORAGE_VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.processor.topology.TopologyEntityUtils;

public class TemplatesConverterUtilsTest {

    final TopologyEntity.Builder storage =
        TopologyEntityUtils.topologyEntityBuilder(1L, EntityType.STORAGE, Collections.emptyList());
    final TopologyEntity.Builder originalHost =
        TopologyEntityUtils.topologyEntityBuilder(2L, EntityType.PHYSICAL_MACHINE, Collections.emptyList());
    final TopologyEntity.Builder replacementHost =
        TopologyEntityUtils.topologyEntityBuilder(3L, EntityType.PHYSICAL_MACHINE, Collections.emptyList());

    private final Map<Long, Builder> topology = ImmutableMap.of(storage.getOid(), storage,
            originalHost.getOid(), originalHost);

    @Before
    public void setup() {
        storage.getEntityBuilder().addCommoditySoldList(
            CommoditySoldDTO.newBuilder()
                .setAccesses(originalHost.getOid())
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommonDTO.CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)
                    .setKey("foo"))
                .build()
        );

        originalHost.getEntityBuilder().addCommoditySoldList(
            CommoditySoldDTO.newBuilder()
                .setAccesses(storage.getOid())
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.DATASTORE_VALUE)
                    .setKey("bar"))
                .build()
        );
    }

    @Test
    public void testUpdateRelatedEntityAccesses() {
        assertEquals(1, storage.getEntityBuilder().getCommoditySoldListCount());

        // Should add an extra DSPM commodity to the storage that accesses the replacementHost.
        TopologyEntityConstructor.updateRelatedEntityAccesses(originalHost.getOid(),
            replacementHost.getOid(), originalHost.getEntityBuilder().getCommoditySoldListList(), topology);

        assertEquals(2, storage.getEntityBuilder().getCommoditySoldListCount());
        assertThat(storage.getEntityBuilder().getCommoditySoldListList().stream()
            .map(CommoditySoldDTO::getCommodityType)
            .map(CommodityType::getType)
            .collect(Collectors.toList()),
            contains(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE, CommodityDTO.CommodityType.DSPM_ACCESS_VALUE));
        assertThat(storage.getEntityBuilder().getCommoditySoldListList().stream()
            .map(CommoditySoldDTO::getAccesses)
            .collect(Collectors.toList()),
            containsInAnyOrder(originalHost.getOid(), replacementHost.getOid()));
    }

    /**
     * Verify creating CommoditySoldDTO.
     */
    @Test
    public void testCreateCommoditySoldDTO() {
        final CommoditySoldDTO soldDTO = TopologyEntityConstructor
                .createCommoditySoldDTO(VSTORAGE_VALUE);
        assertFalse(soldDTO.getIsResizeable());
        assertTrue(soldDTO.getActive());
        // if capacity is not set, it will have default capacity.
        assertEquals(CommoditySoldDTO.getDefaultInstance().getCapacity(), soldDTO.getCapacity(), 0.1);
    }
}