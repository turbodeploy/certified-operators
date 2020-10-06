package com.vmturbo.market.topology.conversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.RatioDependency;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle;
import com.vmturbo.market.runner.cost.MarketPriceTable.DatabasePriceBundle;
import com.vmturbo.market.runner.cost.MarketPriceTable.DatabasePriceBundle.DatabasePrice.StorageOption;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO.ComputeResourceDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple.DependentCostTuple.DependentResourceOption;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceLimitation;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceRangeDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceRatioDependency;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.RangeDependency;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.RangeTuple;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ComputeTierData.DedicatedStorageNetworkState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;


/**
 * Test class to unit test the CostDTOCreator methods.
 */
public class CostDTOCreatorTest {

    private static final long TIER_ID = 111;
    private static final long STORAGE_TIER_ID = 123;
    private static final long REGION_ID = 222;
    private static final long BA_ID = 333;
    private static final int IOSPEC_BASE_TYPE = 1;
    private static final int IOSPEC_TYPE = 11;
    private static final int NETSPEC_BASE_TYPE = 2;
    private static final int NETSPEC_TYPE = 22;

    private static final TopologyEntityDTO BA = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setOid(BA_ID)
            .build();
    public static final String DB_TIER_LICENCE_KEY = "SqlServer:Standard:MultiAz:NoLicenseRequired";
    public static final int DB_TIER_ID = 771;
    public static final int DB_TIER_REGION_ID = 772;

    private static List<TopologyEntityDTO> REGIONS;
    private MarketPriceTable marketPriceTable;
    private CommodityConverter converter;

    /**
     * Initialization for the mocked fields.
     */
    @Before
    public void setup() {
        marketPriceTable = mock(MarketPriceTable.class);
        converter = mock(CommodityConverter.class);
    }

    /**
     * This test ensures that all the values in Enum OSType have a mapping in
     * CostDTOCreator::OSTypeMapping, except "Windows Server" and "Windows server Burst".
     */
    @Test
    public void testOSTypeMappings() {
        List<OSType> osWithoutMapping = new ArrayList<>();
        Map<OSType, String> inversedOSTypeMapping = MarketPriceTable.OS_TYPE_MAP.entrySet().stream().collect(
                Collectors.toMap(Entry::getValue, Entry::getKey));
        for (OSType os : OSType.values()) {
            if (os != OSType.WINDOWS_SERVER && os != OSType.WINDOWS_SERVER_BURST &&
                    !inversedOSTypeMapping.containsKey(os)) {
                osWithoutMapping.add(os);
            }
        }
        String error = "Operating systems " + osWithoutMapping.stream().map(os -> os.toString())
                .collect(Collectors.joining(", ")) + " do not have mapping in " +
                "CostDTOCreator::OSTypeMapping.";
        assertTrue(error, osWithoutMapping.isEmpty());
    }


    /**
     * Creates and returns a test TopologyEntityDTO.
     * @return TopologyEntityDTO
     */
    private TopologyEntityDTO getTestComputeTier() {

        // Setting up the sold commodities for a Region
        final TopologyDTO.CommoditySoldDTO soldDTO = TopologyDTO.CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.DATACENTER_VALUE)
                        .build())
                .build();
        List<TopologyDTO.CommoditySoldDTO > soldDTOS = new ArrayList<TopologyDTO.CommoditySoldDTO>();
        soldDTOS.add(soldDTO);
        TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.REGION_VALUE)
                .setOid(REGION_ID)
                .addAllCommoditySoldList(soldDTOS)
                .build();
        REGIONS = Collections.singletonList(REGION);


        CommodityType ioTpCommType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE).build();
        CommodityType netTpCommType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE).build();
        CommoditySpecificationTO ioCommSpecTO = CommoditySpecificationTO.newBuilder()
                .setBaseType(IOSPEC_BASE_TYPE)
                .setType(IOSPEC_TYPE)
                .build();
        CommoditySpecificationTO netCommSpecTO = CommoditySpecificationTO.newBuilder()
                .setBaseType(NETSPEC_BASE_TYPE)
                .setType(NETSPEC_TYPE)
                .build();
        Mockito.doReturn(ioCommSpecTO).when(converter)
                .commoditySpecification(ioTpCommType);
        Mockito.doReturn(netCommSpecTO).when(converter)
                .commoditySpecification(netTpCommType);
        AccountPricingData accountPricingData = Mockito.mock(AccountPricingData.class);
        DatabasePriceBundle databasePriceBundle = DatabasePriceBundle.newBuilder()
                .addPrice(BA_ID, DatabaseEngine.MYSQL, DatabaseEdition.STANDARD,
                        DeploymentType.MULTI_AZ, LicenseModel.BRING_YOUR_OWN_LICENSE, 0.4,
                        Collections.emptyList())
                .build();
        when(marketPriceTable.getDatabasePriceBundle(TIER_ID, REGION_ID, accountPricingData)).thenReturn(databasePriceBundle);
        ComputePriceBundle computeBundle = ComputePriceBundle.newBuilder()
                .addPrice(BA_ID, OSType.LINUX, 0.5, true)
                .build();

        final TopologyDTO.CommoditySoldDTO topologyIoTpSold =
                TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE)
                                .build())
                        .build();

        final TopologyDTO.CommoditySoldDTO topologyNetTpSold =
                TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE)
                                .build())
                        .build();

        TopologyEntityDTO tier = TopologyEntityDTO.newBuilder()
                .setOid(111)
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .addCommoditySoldList(topologyIoTpSold)
                .addCommoditySoldList(topologyNetTpSold)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setComputeTier(ComputeTierInfo.newBuilder()
                                .setDedicatedStorageNetworkState(
                                        DedicatedStorageNetworkState.NOT_SUPPORTED)
                                .build())
                        .build())
                .build();

        when(marketPriceTable.getComputePriceBundle(tier, REGION_ID, accountPricingData))
            .thenReturn(computeBundle);
        return tier;
    }

    /**
     * Tests the ComputeResourceDependency aspect of the CreateComputeTierCostDTO.
     */
    @Test
    public void testComputeResourceDependency() {
        final TopologyEntityDTO tier = getTestComputeTier();
        Set<TopologyEntityDTO> bas = new HashSet<>();
        bas.add(BA);
        Map<Long, AccountPricingData> accountPricingDatabyBusinessAccountMap = new HashMap<>();
        CostDTOCreator costDTOCreator = new CostDTOCreator(converter, marketPriceTable);
        AccountPricingData accountPricingData = mock(AccountPricingData.class);
        for (TopologyEntityDTO region: REGIONS) {
            when(marketPriceTable.getComputePriceBundle(tier, region.getOid(), accountPricingData)).thenReturn(ComputePriceBundle.newBuilder().build());
        }
        accountPricingDatabyBusinessAccountMap.put(BA_ID, accountPricingData);
        HashSet<AccountPricingData> uniqueAccountPricingData = new HashSet<>(accountPricingDatabyBusinessAccountMap.values());
        CostDTO costDTO = costDTOCreator.createComputeTierCostDTO(tier, REGIONS, uniqueAccountPricingData);
        Assert.assertEquals(1, costDTO.getComputeTierCost().getComputeResourceDepedencyCount());
        ComputeResourceDependency dependency = costDTO.getComputeTierCost().getComputeResourceDepedency(0);
        Assert.assertNotNull(dependency.getBaseResourceType());
        Assert.assertEquals(NETSPEC_BASE_TYPE, dependency.getBaseResourceType().getBaseType());
        Assert.assertEquals(NETSPEC_TYPE, dependency.getBaseResourceType().getType());
        Assert.assertNotNull(dependency.getDependentResourceType());
        Assert.assertEquals(IOSPEC_BASE_TYPE, dependency.getDependentResourceType().getBaseType());
        Assert.assertEquals(IOSPEC_TYPE, dependency.getDependentResourceType().getType());
    }

    @Test
    public void testCreateStorageTierCostDTO() {
        TopologyEntityDTO storageTier = createStorageTier();
        CostDTO costDTO = new CostDTOCreator(converter, marketPriceTable)
                .createStorageTierCostDTO(storageTier, Collections.emptyList(), Collections.emptySet());
        StorageTierCostDTO storageTierCostDTO = costDTO.getStorageTierCost();
        assertNotNull(storageTierCostDTO);

        assertEquals(1, storageTierCostDTO.getStorageResourceLimitationCount());
        StorageResourceLimitation storageResourceLimitation = storageTierCostDTO.getStorageResourceLimitation(0);
        assertEquals(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, storageResourceLimitation.getResourceType().getType());
        assertEquals(100, storageResourceLimitation.getMinCapacity(), 0);
        assertEquals(1000, storageResourceLimitation.getMaxCapacity(), 0);
        assertTrue(storageResourceLimitation.getCheckMinCapacity());

        assertEquals(1, storageTierCostDTO.getStorageResourceRatioDependencyCount());
        StorageResourceRatioDependency storageResourceDependency = storageTierCostDTO.getStorageResourceRatioDependency(0);
        assertEquals(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE, storageResourceDependency.getDependentResourceType().getType());
        assertEquals(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, storageResourceDependency.getBaseResourceType().getType());
        assertEquals(3, storageResourceDependency.getMaxRatio(), 0.01d);

        assertEquals(1, storageTierCostDTO.getStorageResourceRangeDependencyCount());
        StorageResourceRangeDependency storageResourceRange = storageTierCostDTO.getStorageResourceRangeDependency(0);
        assertEquals(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE, storageResourceRange.getDependentResourceType().getType());
        assertEquals(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, storageResourceRange.getBaseResourceType().getType());
        assertEquals(2, storageResourceRange.getRangeTupleCount());
    }

    /**
     * Tests creating a DB tier cost DTO which includes some storage options.
     */
    @Test
    public void createdDatabaseTierCostDTOTest() {
        AccountPricingData accountPricingData = Mockito.mock(AccountPricingData.class);
        List<StorageOption> storageOptions = new ArrayList<>();
        storageOptions.add(new StorageOption(250, 250, 0.0));
        storageOptions.add(new StorageOption(50, 300, 0.01));
        DatabasePriceBundle databasePriceBundle = DatabasePriceBundle.newBuilder()
                .addPrice(0, DatabaseEngine.SQLSERVER, DatabaseEdition.STANDARD,
                        DeploymentType.MULTI_AZ, LicenseModel.NO_LICENSE_REQUIRED, 0.4,
                        storageOptions)
                .build();
        when(marketPriceTable.getDatabasePriceBundle(DB_TIER_ID, DB_TIER_REGION_ID,
                accountPricingData)).thenReturn(databasePriceBundle);
        CostDTOCreator costDTOCreator = new CostDTOCreator(converter, marketPriceTable);
        Set<AccountPricingData> accountPricingDataSet = new HashSet<>();
        accountPricingDataSet.add(accountPricingData);
        List<TopologyEntityDTO> regions = new ArrayList<>();
        final CommodityType licenseType = CommodityType.newBuilder()
                .setKey(LicenseModel.NO_LICENSE_REQUIRED.name())
                .setType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                .build();
        final TopologyDTO.CommoditySoldDTO license =
                TopologyDTO.CommoditySoldDTO.newBuilder().setCommodityType(licenseType).build();
        regions.add(TopologyEntityDTO.newBuilder()
                .setOid(DB_TIER_REGION_ID)
                .setEntityType(EntityType.REGION_VALUE)
                .addCommoditySoldList(license)
                .build());
        TopologyEntityDTO dbTier = createDBTier();
        when(converter.commoditySpecification(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                .build())).thenReturn(CommoditySpecificationTO.newBuilder()
                .setType(3)
                .setBaseType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                .build());
        CostDTO costDTO =
                costDTOCreator.createDatabaseTierCostDTO(dbTier, regions, accountPricingDataSet);
        List<DependentResourceOption> dependentResourceOptions = costDTO.getDatabaseTierCost()
                .getCostTupleListList()
                .get(0)
                .getDependentCostTuplesList()
                .get(0)
                .getDependentResourceOptionsList();
        Assert.assertEquals(2, dependentResourceOptions.size());
        DependentResourceOption dependentResourceOption1 = dependentResourceOptions.get(0);
        Assert.assertEquals(250, dependentResourceOption1.getIncrement());
        Assert.assertEquals(250, dependentResourceOption1.getEndRange());
        Assert.assertEquals(0.0, dependentResourceOption1.getPrice(), 0.01);
        DependentResourceOption dependentResourceOption2 = dependentResourceOptions.get(1);
        Assert.assertEquals(50, dependentResourceOption2.getIncrement());
        Assert.assertEquals(300, dependentResourceOption2.getEndRange());
        Assert.assertEquals(0.01, dependentResourceOption2.getPrice(), 0.01);
    }

    private TopologyEntityDTO createDBTier() {
        final CommodityType licenseType = CommodityType.newBuilder()
                .setKey(DB_TIER_LICENCE_KEY)
                .setType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                .build();
        Mockito.when(converter.commoditySpecification(licenseType))
                .thenReturn(CommoditySpecificationTO.newBuilder()
                        .setBaseType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .setType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                        .build());
        final TopologyDTO.CommoditySoldDTO license =
                TopologyDTO.CommoditySoldDTO.newBuilder().setCommodityType(licenseType).build();
        return TopologyEntityDTO.newBuilder()
                .setOid(DB_TIER_ID)
                .setEntityType(EntityType.DATABASE_TIER_VALUE)
                .addCommoditySoldList(license)
                .build();
    }

    private TopologyEntityDTO createStorageTier() {
        final CommodityType storageAmountType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE).build();
        Mockito.when(converter.commoditySpecification(storageAmountType))
                .thenReturn(CommoditySpecificationTO.newBuilder().setBaseType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                        .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE).build());
        final CommodityType storageAccessType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE).build();
        Mockito.when(converter.commoditySpecification(storageAccessType))
                .thenReturn(CommoditySpecificationTO.newBuilder().setBaseType(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE)
                        .setType(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE).build());
        final CommodityType ioThroughputType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE).build();
        Mockito.when(converter.commoditySpecification(ioThroughputType))
                .thenReturn(CommoditySpecificationTO.newBuilder().setBaseType(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE)
                        .setType(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE).build());
        final TopologyDTO.CommoditySoldDTO stAmount =
                TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(storageAmountType)
                        .setMinAmountForConsumer(100)
                        .setMaxAmountForConsumer(1000)
                        .setCheckMinAmountForConsumer(true)
                        .build();

        final TopologyDTO.CommoditySoldDTO stAccess =
                TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(storageAccessType)
                        .setRatioDependency(RatioDependency.newBuilder().setBaseCommodity(storageAmountType).setMaxRatio(3))
                        .build();

        Set<RangeTuple> rangeTuples = new HashSet<>();
        for (int i = 0; i < 2; i++) {
            rangeTuples.add(CommodityDTO.RangeTuple.newBuilder()
                    .setBaseMaxAmountForConsumer(i)
                    .setDependentMaxAmountForConsumer(i)
                    .build());
        }
        final TopologyDTO.CommoditySoldDTO ioThroughput =
                TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(ioThroughputType)
                        .setRangeDependency(RangeDependency.newBuilder().setBaseCommodity(CommodityDTO.CommodityType.STORAGE_AMOUNT)
                                .addAllRangeTuple(rangeTuples).build())
                        .build();

        return TopologyEntityDTO.newBuilder()
                .setOid(STORAGE_TIER_ID)
                .setEntityType(EntityType.STORAGE_TIER_VALUE)
                .addCommoditySoldList(stAmount)
                .addCommoditySoldList(stAccess)
                .addCommoditySoldList(ioThroughput)
                .build();
    }
}
