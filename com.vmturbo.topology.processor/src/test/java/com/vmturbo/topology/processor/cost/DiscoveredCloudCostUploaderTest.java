package com.vmturbo.topology.processor.cost;

import java.util.Collections;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingEntityData;
import com.vmturbo.topology.processor.targets.TargetStore;

public class DiscoveredCloudCostUploaderTest {

    private StitchingContext stitchingContext;

    private RICostDataUploader riCostDataUploader =  Mockito.mock(RICostDataUploader.class);
    private AccountExpensesUploader accountExpensesUploader = Mockito.mock(AccountExpensesUploader.class);
    private PriceTableUploader priceTableUploader = Mockito.mock(PriceTableUploader.class);
    private TargetStore targetStore = Mockito.mock(TargetStore.class);
    private TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(1L)
            .setTopologyId(10L)
            .build();

    private final String riLocalId1 = "aws::ap-south-1::RI::1";
    private final String riLocalId2 = "aws::ap-south-1::RI::2";
    private final StitchingEntityData ri1 = StitchingEntityData.newBuilder(
            EntityDTO.newBuilder()
                    .setEntityType(EntityType.RESERVED_INSTANCE)
                    .setId("aws::ap-south-1::RI::1"))
            .oid(1)
            .build();
    private final StitchingEntityData ri2 = StitchingEntityData.newBuilder(
            EntityDTO.newBuilder()
                    .setEntityType(EntityType.RESERVED_INSTANCE)
                    .setId("aws::ap-south-1::RI::2"))
            .oid(2)
            .build();
    private final Map<String, StitchingEntityData> localIdToEntityMap = ImmutableMap.of(
            riLocalId1, ri1,
            riLocalId2, ri2
    );

    private DiscoveredCloudCostUploader cloudCostUploader;

    @Before
    public void setup() {
        StitchingContext.Builder stitchingContextBuilder = StitchingContext.newBuilder(2)
            .setTargetStore(Mockito.mock(TargetStore.class))
            .setIdentityProvider(Mockito.mock(IdentityProviderImpl.class));
        stitchingContextBuilder.addEntity(ri1, localIdToEntityMap);
        stitchingContextBuilder.addEntity(ri2, localIdToEntityMap);
        stitchingContext = stitchingContextBuilder.build();
        cloudCostUploader = new DiscoveredCloudCostUploader(riCostDataUploader,
                accountExpensesUploader, priceTableUploader, targetStore);
    }

    @Test
    public void testReservedInstanceRemovedIfCostRunning() {
        Assert.assertEquals(2, stitchingContext.getEntitiesOfType(EntityType.RESERVED_INSTANCE).count());
        cloudCostUploader.uploadCostData(topologyInfo, stitchingContext);
        Assert.assertEquals(0, stitchingContext.getEntitiesOfType(EntityType.RESERVED_INSTANCE).count());
    }

    @Test
    public void testReservedInstanceRemovedIfCostNotRunning() {
        Assert.assertEquals(2, stitchingContext.getEntitiesOfType(EntityType.RESERVED_INSTANCE).count());
        Mockito.doThrow(new RuntimeException("cost: Name or service not known"))
                .when(accountExpensesUploader).uploadAccountExpenses(Collections.emptyMap(),
                topologyInfo, stitchingContext, Mockito.mock(CloudEntitiesMap.class));
        cloudCostUploader.uploadCostData(topologyInfo, stitchingContext);
        Assert.assertEquals(0, stitchingContext.getEntitiesOfType(EntityType.RESERVED_INSTANCE).count());
    }
}
