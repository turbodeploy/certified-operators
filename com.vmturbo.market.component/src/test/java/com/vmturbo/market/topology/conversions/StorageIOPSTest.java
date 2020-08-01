package com.vmturbo.market.topology.conversions;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.protobuf.util.JsonFormat;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Test projected IOPS calculations.
 */
public class StorageIOPSTest {

    private static CloudStorageTierIOPSCalculator iopsCalculator;

    private static TopologyEntityDTO gp2;
    private static TopologyEntityDTO io1;
    private static TopologyEntityDTO st1;
    private static TopologyEntityDTO sc1;
    private static TopologyEntityDTO standard;
    private static TopologyEntityDTO managedPremium;
    private static TopologyEntityDTO managedStandard;
    private static TopologyEntityDTO managedStandardSSD;
    private static TopologyEntityDTO managedUltraSSD;

    /**
     * Load entities from JSON files.
     */
    @BeforeClass
    public static void setup() {
        gp2 = loadTopologyBuilderDTO("gp2.json").build();
        io1 = loadTopologyBuilderDTO("io1.json").build();
        st1 = loadTopologyBuilderDTO("st1.json").build();
        sc1 = loadTopologyBuilderDTO("sc1.json").build();
        standard = loadTopologyBuilderDTO("standard.json").build();
        managedPremium = loadTopologyBuilderDTO("managedPremium.json").build();
        managedStandard = loadTopologyBuilderDTO("managedStandard.json").build();
        managedStandardSSD = loadTopologyBuilderDTO("managedStandardSSD.json").build();
        managedUltraSSD = loadTopologyBuilderDTO("managedUltraSSD.json").build();

        Map<Long, TopologyEntityDTO> topology = new HashMap<>();
        topology.put(1L, gp2);
        topology.put(2L, io1);
        topology.put(3L, st1);
        topology.put(4L, sc1);
        topology.put(5L, standard);
        topology.put(6L, managedPremium);
        topology.put(7L, managedStandard);
        topology.put(8L, managedStandardSSD);
        topology.put(9L, managedUltraSSD);

        iopsCalculator = new CloudStorageTierIOPSCalculator(topology);
    }

    /**
     * Test projected IOPS calculation for GP2.
     *
     * @throws Exception any exception
     */
    @Test
    public void testGP2() throws Exception {
        testIOPS(gp2, 30d, null, 100d);
        testIOPS(gp2, 40d, null, 120d);
        testIOPS(gp2, 6000d, null, 16000d);
    }

    /**
     * Test projected IOPS calculation for IO1.
     *
     * @throws Exception any exception
     */
    @Test
    public void testIO1() throws Exception {
        testIOPS(io1, 100d, 3000d, 3000d);
        testIOPS(io1, 50d, 3000d, 2500d);
        testIOPS(io1, 4d, 80d, 100d);
        testIOPS(io1, 4d, 300d, 200d);
        testIOPS(io1, 25.1d, 815d, 815d);
    }

    /**
     * Test projected IOPS calculation for ST1.
     *
     * @throws Exception any exception
     */
    @Test
    public void testST1() throws Exception {
        testIOPS(st1, 1024d, null, 40d);
        testIOPS(st1, 15000d, null, 500d);
    }

    /**
     * Test projected IOPS calculation for SC1.
     *
     * @throws Exception any exception
     */
    @Test
    public void testSC1() throws Exception {
        testIOPS(sc1, 1024d, null, 12d);
        testIOPS(sc1, 16384d, null, 192d);
    }

    /**
     * Test projected IOPS calculation for Standard.
     *
     * @throws Exception any exception
     */
    @Test
    public void testStandard() throws Exception {
        testIOPS(standard, 100d, null, 200d);
        testIOPS(standard, 16384d, null, 200d);
    }

    /**
     * Test projected IOPS calculation for Managed Premium.
     *
     * @throws Exception any exception
     */
    @Test
    public void testManagedPremium() throws Exception {
        testIOPS(managedPremium, 200d, null, 1100d);
    }

    /**
     * Test projected IOPS calculation for Managed Standard.
     *
     * @throws Exception any exception
     */
    @Test
    public void testManagedStandard() throws Exception {
        testIOPS(managedStandard, 5000d, null, 1300d);
    }

    /**
     * Test projected IOPS calculation for Managed Standard SSD.
     *
     * @throws Exception any exception
     */
    @Test
    public void testManagedStandardSSD() throws Exception {
        testIOPS(managedStandardSSD, 5000d, null, 2000d);
    }

    /**
     * Test projected IOPS calculation for Managed Ultra SSD.
     *
     * @throws Exception any exception
     */
    @Test
    public void testManagedUltraSSD() throws Exception {
        testIOPS(managedUltraSSD, 1000d, 100000d, 100000d);
        testIOPS(managedUltraSSD, 300d, 100000d, 90000d);
        testIOPS(managedUltraSSD, 1000d, 400000d, 160000d);
        testIOPS(managedUltraSSD, 1000d, 1000d, 2000d);
    }


    private void testIOPS(TopologyEntityDTO tier, Double storageAmountUsedInGB,
                                      Double storageAccessUsed, Double expectedIOPSCapacity) {
        Optional<Double> iopsCap = iopsCalculator.getIopsCapacity(createCommodityList(storageAmountUsedInGB, storageAccessUsed), tier);
        Assert.assertTrue(iopsCap.isPresent());
        Assert.assertEquals(expectedIOPSCapacity, iopsCap.get());
    }

    private List<CommodityBoughtDTO> createCommodityList(Double storageAmountUsed, Double storageAccessUsed) {
        List<CommodityBoughtDTO> commList = new ArrayList<>();
        if (storageAmountUsed != null) {
            CommodityBoughtDTO.Builder storageAmountBuilder = CommodityBoughtDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE))
                    .setUsed(storageAmountUsed);
            commList.add(storageAmountBuilder.build());
        }
        if (storageAccessUsed != null) {
            CommodityBoughtDTO.Builder storageAccessBuilder = CommodityBoughtDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE))
                    .setUsed(storageAccessUsed);
            commList.add(storageAccessBuilder.build());
        }
        return commList;
    }

    /**
     * Logs JSON file containing TopologyEntityDTO.Builder.
     *
     * @param fileBasename Base filename of JSON file.
     * @return TopologyEntityDTO.Builder read from file.
     * @throws IllegalArgumentException On file read error or missing file.
     */
    private static TopologyEntityDTO.Builder loadTopologyBuilderDTO(String fileBasename) {
        TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder();
        try {
            JsonFormat.parser().merge(getInputReader(fileBasename), builder);
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Bad input JSON file " + fileBasename, ioe);
        }
        return builder;
    }

    /**
     * Protobuf message JSON file reader helper.
     *
     * @param fileBasename Base filename of JSON file.
     * @return Reader to pass to JsonFormat.
     * @throws IOException Thrown on file read error.
     */
    private static InputStreamReader getInputReader(@Nonnull final String fileBasename)
            throws IOException {
        String fileName = "protobuf/messages/storageTiers/" + fileBasename;
        URL fileUrl = CloudTopologyConverter.class.getClassLoader().getResource(fileName);
        if (fileUrl == null) {
            throw new IOException("Could not locate file: " + fileName);
        }
        return new InputStreamReader(fileUrl.openStream());
    }
}
