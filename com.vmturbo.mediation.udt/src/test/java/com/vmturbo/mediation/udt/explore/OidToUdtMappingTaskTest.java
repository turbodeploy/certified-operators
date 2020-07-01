package com.vmturbo.mediation.udt.explore;

import static com.vmturbo.mediation.udt.UdtProbe.UDT_PROBE_TAG;
import static com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants.VENDOR;
import static com.vmturbo.platform.sdk.common.util.SDKUtil.VENDOR_ID;
import static java.util.Collections.emptySet;

import java.util.Collections;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.mediation.udt.TestUtils;
import com.vmturbo.mediation.udt.inventory.UdtChildEntity;
import com.vmturbo.mediation.udt.inventory.UdtEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test class for {@link OidToUdtMappingTask}.
 */
public class OidToUdtMappingTaskTest {

    /**
     * BUSINESS_TRANSACTION`s child is SERVICE. It tests that OidToUdtMappingTask correctly maps
     * UdtChildEntity.oid to UdtEntity.oid.
     */
    @Test
    public void testMapping() {
        long topologyDtoId = 1000L;
        String definitionId = "222";
        DataProvider dataProvider = Mockito.mock(DataProvider.class);
        UdtEntity udtService = new UdtEntity(EntityType.SERVICE, definitionId, "Service-B", emptySet());
        UdtChildEntity childService = new UdtChildEntity(topologyDtoId, EntityType.SERVICE);
        UdtEntity udtTransaction = new UdtEntity(EntityType.BUSINESS_TRANSACTION, "111",
                "TransactionX", Collections.singleton(childService));
        // 'udtService' EQUAL 'childService'
        TopologyEntityDTO entityDTO = TestUtils.createTopologyDto(topologyDtoId, "Service-B", EntityType.SERVICE)
                .toBuilder()
                .putEntityPropertyMap(VENDOR, UDT_PROBE_TAG)
                .putEntityPropertyMap(VENDOR_ID, definitionId)
                .build();
        Mockito.when(dataProvider.getEntitiesByOids(Mockito.anySet()))
                .thenReturn(Collections.singleton(entityDTO));
        OidToUdtMappingTask.execute(Sets.newHashSet(udtTransaction, udtService), dataProvider);
        Assert.assertEquals(String.valueOf(topologyDtoId), udtService.getDtoId());
    }
}
