package com.vmturbo.cost.component.util;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests the BusinessAccountHelper utility class.
 */
public class BusinessAccountHelperTest {

    /**
     * Tests the business account target mappjngs
     * and the discovered business account set.
     */
    @Test
    public void testTargetMappings() {
        BusinessAccountHelper helper = new BusinessAccountHelper();
        TopologyEntityDTO ba = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(1L)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setBusinessAccount(BusinessAccountInfo.newBuilder()
                                .setAccountId("One")
                                .setAssociatedTargetId(11L)
                                .build())
                        .build())
                .build();
        helper.storeTargetMapping(1L, "One", ImmutableList.of(11L));
        helper.storeTargetMapping(2L, "Bill", ImmutableList.of(12L));
        helper.storeDiscoveredBusinessAccount(ba);

        Assert.assertEquals(2, helper.getAllBusinessAccounts().size());
        Assert.assertEquals(1, helper.getDiscoveredBusinessAccounts().size());
        Assert.assertEquals(1,
                helper.getDiscoveredBusinessAccounts().iterator().next().longValue());

        helper.removeTargetForBusinessAccount(11L);
        helper.removeBusinessAccountWithNoTargets();

        Assert.assertEquals(0, helper.getDiscoveredBusinessAccounts().size());
        Assert.assertEquals(1, helper.getAllBusinessAccounts().size());
    }
}
