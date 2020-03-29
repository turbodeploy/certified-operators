package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.CategoryPathConstants;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.RISettingsEnum.PreferredTerm;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.ActionContextRIBuyStore;
import com.vmturbo.cost.component.reserved.instance.BuyReservedInstanceStore;
import com.vmturbo.cost.component.reserved.instance.action.ReservedInstanceActionsSender;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyAnalysisContextProvider;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.calculator.RIBuyDemandCalculatorFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;

/**
 * This class tests methods in the ReservedInstanceAnalyzer class.
 */
public class ReservedInstanceAnalyzerTest {

    static final long MASTER_ID = 11111;

    static final long ZONE_ID = 33333;

    static final long REGION_ID = 1234;

    static final long COMPUTE_TIER_ID = 1234;

    private SettingServiceMole settingServiceMole = spy(new SettingServiceMole());

    /**
     * Test server.
     */
    @Rule
    public GrpcTestServer settingsServer = GrpcTestServer.newServer(settingServiceMole);

     /**
     * This method tests ReservedInstanceAnalyzer::getHourlyOnDemandCost method.
     **/
    @Ignore
    public void testGetHourlyOnDemandCost() {
        //TODO use new classes
        /*TopologyEntityDTO buyComputeTier = buildComputeTierDTO();
        TopologyEntityDTO region = buildRegionDTO();
        Map<TopologyEntityDTO, float[]> templateTypeHourlyDemand = new HashMap<>();
        float[] demand = new float[ReservedInstanceDataProcessor.WEEKLY_DEMAND_DATA_SIZE];
        Arrays.fill(demand, 4);
        templateTypeHourlyDemand.put(buyComputeTier, demand);
        ReservedInstanceRegionalContext regionalContext = new ReservedInstanceRegionalContext(MASTER_ID,
                OSType.LINUX, Tenancy.DEFAULT, buyComputeTier, region);
        ReservedInstanceAnalyzerRateAndRIs priceAndRIProvider = Mockito
                .mock(ReservedInstanceAnalyzerRateAndRIs.class);
        Mockito.when(priceAndRIProvider.lookupOnDemandRate(any(), any())).thenReturn(1f);
        ReservedInstanceAnalyzer analyzer = new ReservedInstanceAnalyzer();
        final float hourlyOnDemandCost = analyzer.getHourlyOnDemandCost(templateTypeHourlyDemand,
                        regionalContext, priceAndRIProvider, "RILT0000");
        assertEquals(1f, hourlyOnDemandCost, 0.0);
        */
    }

    /**
     * Returns a compute tier DTO.
     * @return returns a compute tier DTO.
     */
    private TopologyEntityDTO buildComputeTierDTO() {
        return TopologyEntityDTO.newBuilder()
                .setOid(COMPUTE_TIER_ID)
                .setDisplayName("computeTier")
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setComputeTier(ComputeTierInfo.newBuilder()
                                .setFamily("familyA")
                                .setNumCoupons(4)))
                .build();
    }

    /**
     * Returns a region DTO.
     * @return returns a region DTO.
     */
    private TopologyEntityDTO buildRegionDTO() {
        return TopologyEntityDTO.newBuilder()
                .setOid(REGION_ID)
                .setDisplayName("region")
                .setEntityType(EntityType.REGION_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(ZONE_ID)
                        .setConnectedEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                        .setConnectionType(ConnectionType.OWNS_CONNECTION))
                .build();
    }
}
