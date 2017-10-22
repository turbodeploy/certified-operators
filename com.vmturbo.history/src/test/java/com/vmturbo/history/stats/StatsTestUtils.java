package com.vmturbo.history.stats;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.jooq.Table;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.reports.db.EntityType;

/**
 * Static utility methods to support stats class testing.
 **/
public class StatsTestUtils {
    public static final Table<?> PM_LATEST_TABLE = EntityType.PHYSICAL_MACHINE.getLatestTable();
    public static final Table<?> APP_LATEST_TABLE = EntityType.APPLICATION.getLatestTable();
    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();
    public static final String TEST_VM_PATH = "topology/tak_test1_vm_dto.json";
    public static final String TEST_APP_PATH = "topology/guestload_tak_test1_app_dto.json";
    public static final String TEST_APP_WITHOUT_PROVIDER_PATH = "topology/test2_app_without_provider_dto.json";
    public static final String TEST_PM_PATH = "topology/hp-esx_pm_dto.json";
    private static CommodityType CPU_COMMODITY_TYPE = CommodityType.newBuilder()
            .setType(CommonDTO.CommodityDTO.CommodityType.CPU_VALUE).build();
    private static CommodityType DSPMA_COMMODITY_TYPE = CommodityType.newBuilder()
            .setType(CommonDTO.CommodityDTO.CommodityType.DSPM_ACCESS_VALUE).build();
    private static CommodityType Q1_VCPU_COMMODITY_TYPE = CommodityType.newBuilder()
            .setType(CommonDTO.CommodityDTO.CommodityType.Q1_VCPU_VALUE).build();
    public static TopologyDTO.CommodityBoughtDTO cpuBought = TopologyDTO.CommodityBoughtDTO.newBuilder()
            .setCommodityType(CPU_COMMODITY_TYPE)
            .build();
    private static double CPU_CAPACITY = 111.111;
    private static double DSPMA_CAPACITY = 100000;

    public static TopologyEntityDTO vm(long oid, long providerId) {
        return TopologyEntityDTO.newBuilder()
                        .setOid(oid)
                        .setDisplayName("VM-" + oid)
                        .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                        // 999 is the provider id. Don't care that it doesn't exist.
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(providerId)
                            .addCommodityBought(cpuBought))
                        .build();
    }

    public static TopologyEntityDTO pm(long oid, double cpuUsed) {
        return TopologyEntityDTO.newBuilder()
                        .setOid(oid)
                        .setDisplayName("PM-" + oid)
                        .setEntityType(CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE)
                        .addCommoditySoldList(cpu(cpuUsed)).build();
    }

    public static TopologyEntityDTO vm(long oid, long sellerOid, double cpuUsed) {
        return TopologyEntityDTO.newBuilder()
                        .setOid(oid)
                        .setDisplayName("VM-" + oid)
                        .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(sellerOid)
                            .addCommodityBought(cpuBought(cpuUsed)))
                        .build();
    }

    public static TopologyDTO.CommoditySoldDTO cpu(double used) {
        return TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(CPU_COMMODITY_TYPE)
                        .setUsed(used)
                        .setCapacity(CPU_CAPACITY).build();
    }

    public static TopologyDTO.CommoditySoldDTO dspma(double used) {
        return TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(DSPMA_COMMODITY_TYPE)
                        .setUsed(used)
                        .setCapacity(DSPMA_CAPACITY).build();
    }

    public static TopologyDTO.CommodityBoughtDTO cpuBought(double used) {
        return TopologyDTO.CommodityBoughtDTO.newBuilder()
                        .setCommodityType(CPU_COMMODITY_TYPE)
                        .setUsed(used).build();
    }

    public static TopologyDTO.CommoditySoldDTO q1_vcpu(double used) {
        return TopologyDTO.CommoditySoldDTO.newBuilder()
                .setCommodityType(Q1_VCPU_COMMODITY_TYPE)
                .setUsed(used)
                .setCapacity(CPU_CAPACITY).build();
    }

    /**
     * Use GSON to read a single TopologyDTO from a json file.
     *
     * @param testDTOFilePath the path to the resource to read for the TopologyDTO .json
     * @return a {@link TopologyDTO}s read from the given JSON test file
     * @throws Exception not supposed to happen
     */
    static TopologyEntityDTO generateEntityDTO(String testDTOFilePath)
            throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource(testDTOFilePath).getFile());
        final InputStream dtoInputStream = new FileInputStream(file);
        InputStreamReader inputStreamReader = new InputStreamReader(dtoInputStream);
        JsonReader topologyReader = new JsonReader(inputStreamReader);
        return GSON.fromJson(topologyReader, TopologyEntityDTO.class);
    }

    /**
     * Use GSON to read the test file TopologyDTO's into a collection.
     *
     * @param testTopologyPath the path to the resource to read for the test topology .json
     * @param testTopologyFileName the test file name
     * @return a collection of {@link TopologyDTO}s read from the given JSON test file
     * @throws Exception not supposed to happen
     */
    static Collection<TopologyEntityDTO> generateEntityDTOs(String testTopologyPath,
                                                                        String testTopologyFileName)
            throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL zipUrl = classLoader.getResource(testTopologyPath);
        ZipFile zip = new ZipFile(new File(zipUrl.toURI()));
        final ZipEntry zipEntry = zip.getEntry(testTopologyFileName);
        final InputStream topologyInputStream = zip.getInputStream(zipEntry);
        InputStreamReader inputStreamReader = new InputStreamReader(topologyInputStream);
        JsonReader topologyReader = new JsonReader(inputStreamReader);
        return Arrays.asList(GSON.fromJson(topologyReader, TopologyEntityDTO[].class));
    }
}
