package com.vmturbo.topology.processor.cost;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.NonMarketDTO.CostDataDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.NonMarketEntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.TargetCostData;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.stitching.StitchingContext;

public class DiscoveredCloudCostUploaderTest {

    private StitchingContext stitchingContext;

    private RICostDataUploader riCostDataUploader =  mock(RICostDataUploader.class);
    private AccountExpensesUploader accountExpensesUploader = mock(AccountExpensesUploader.class);
    private PriceTableUploader priceTableUploader = mock(PriceTableUploader.class);
    private BusinessAccountPriceTableKeyUploader businessAccountPriceTableKeyUploader =
            mock(BusinessAccountPriceTableKeyUploader.class);
    private TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(1L)
            .setTopologyId(10L)
            .build();

    private DiscoveredCloudCostUploader cloudCostUploader;

    @Before
    public void setup() {
        TopologyProcessorCostTestUtils utils = new TopologyProcessorCostTestUtils();
        stitchingContext = utils.setupStitchingContext();
        cloudCostUploader = new DiscoveredCloudCostUploader(riCostDataUploader,
                accountExpensesUploader, priceTableUploader,
                businessAccountPriceTableKeyUploader );
    }

    @Test
    public void testReservedInstanceRemovedIfCostRunning() {
        Assert.assertEquals(2, stitchingContext.getEntitiesOfType(EntityType.RESERVED_INSTANCE).count());
        cloudCostUploader.uploadCostData(topologyInfo, stitchingContext);
        Assert.assertEquals(0, stitchingContext.getEntitiesOfType(EntityType.RESERVED_INSTANCE).count());
        verify(businessAccountPriceTableKeyUploader, times(1))
                .uploadAccountPriceTableKeys(any(), any(), any());
    }

    @Test
    public void testReservedInstanceRemovedIfCostNotRunning() {
        Assert.assertEquals(2, stitchingContext.getEntitiesOfType(EntityType.RESERVED_INSTANCE).count());
        Mockito.doThrow(new RuntimeException("cost: Name or service not known"))
                .when(accountExpensesUploader).uploadAccountExpenses(Collections.emptyMap(),
                topologyInfo, stitchingContext, mock(CloudEntitiesMap.class));
        cloudCostUploader.uploadCostData(topologyInfo, stitchingContext);
        Assert.assertEquals(0, stitchingContext.getEntitiesOfType(EntityType.RESERVED_INSTANCE).count());
    }

    @Test
    public void testDiags() throws DiagnosticsException {
        final long targetId = 1;

        IdentityProvider idProvider = mock(IdentityProvider.class);
        when(idProvider.generateOperationId()).thenReturn(1000L);
        Discovery discovery = new Discovery(2L, targetId, idProvider);
        cloudCostUploader.recordTargetCostData(1L, Optional.of(SDKProbeType.AWS),
                Optional.of(ProbeCategory.CLOUD_MANAGEMENT),
                discovery,
            Collections.singletonList(NonMarketEntityDTO.newBuilder()
                .setDisplayName("foo")
                .setId("id")
                .setEntityType(NonMarketEntityType.CLOUD_SERVICE)
                .build()),
            Collections.singletonList(CostDataDTO.newBuilder()
                .setAccountId("some account")
                .setId("some id")
                .setAppliesProfile(true)
                .setEntityType(EntityType.CLOUD_SERVICE)
                .setCost(123)
                .build()),
            null);

        final Map<Long, TargetCostData> originalMap = cloudCostUploader.getCostDataByTargetIdSnapshot();

        final DiagnosticsAppender appender = Mockito.mock(DiagnosticsAppender.class);
        cloudCostUploader.collectDiags(appender);
        final ArgumentCaptor<String> diagsCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(appender, Mockito.atLeastOnce()).appendString(diagsCaptor.capture());

        final DiscoveredCloudCostUploader newUploader =
            new DiscoveredCloudCostUploader(riCostDataUploader, accountExpensesUploader, priceTableUploader,
                    businessAccountPriceTableKeyUploader);

        newUploader.restoreDiags(diagsCaptor.getAllValues(), null);

        assertThat(newUploader.getProbeTypesForTargetId(), is(cloudCostUploader.getProbeTypesForTargetId()));

        final Map<Long, TargetCostData> newMap = newUploader.getCostDataByTargetIdSnapshot();
        assertThat(newMap.keySet(), is(originalMap.keySet()));
        assertThat(newMap.get(targetId).targetId, is(originalMap.get(targetId).targetId));
        assertThat(newMap.get(targetId).cloudServiceEntities, is(originalMap.get(targetId).cloudServiceEntities));
        assertThat(newMap.get(targetId).costDataDTOS, is(originalMap.get(targetId).costDataDTOS));
    }
}
