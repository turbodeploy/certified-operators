package com.vmturbo.market.runner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.InvalidTopologyException;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.topology.conversions.TopologyConverter;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs;

public class ScopedTopologyTest {

    private static final TopologyInfo PLAN_TOPOLOGY_INFO = TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.PLAN)
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                    .setPlanType(PlanProjectType.USER))
            .build();

    public static final boolean INCLUDE_VDC = false;
    private static final long ID_GENERATOR_PREFIX = 1;
    public static final int CLUSTER_1_EXPANDED_SE_COUNT = 19;
    @SuppressWarnings("FieldCanBeLocal")
    private static String SIMPLE_TOPOLOGY_JSON_FILE = "protobuf/messages/simple-topology.json";
    private static final long HOST_11_OID = 72207427031424L;
    private static final long HOST_12_OID = 72207427031409L;
    private static final long HOST_13_OID = 72207427031408L;

    private static final Gson GSON = new Gson();
    private Set<TopologyEntityDTO> topologyDTOs;
    Analysis testAnalysis;

    /**
     * Read the test topology from a resource file (.json).
     *
     * @throws FileNotFoundException if the topology test file is not found
     * @throws InvalidProtocolBufferException if the topology is in the wrong format
     */
    @Before
    public void setup() throws FileNotFoundException, InvalidProtocolBufferException {
        topologyDTOs = readTopologyFromJsonFile();
        IdentityGenerator.initPrefix(ID_GENERATOR_PREFIX);
        TopologyDTO.TopologyInfo topoogyInfo = TopologyDTO.TopologyInfo.getDefaultInstance();
        testAnalysis = (new Analysis.AnalysisFactory()).newAnalysisBuilder()
                .setTopologyInfo(topoogyInfo)
                .setIncludeVDC(INCLUDE_VDC)
                .build();
    }

    /**
     * In this test we start with "Host #12", which hosts "VM #14" which hosts "App #15".
     * The VM buys "CLUSTER-1" and "STORAGE-CLUSTER-1". There is only one Host providing
     * "CLUSTER-1", and so the expanded scope includes the original Host, the VM and App,
     * the Datastores that provide "STORAGE-CLUSTER-1" (2,4,6,8,10), and all of the
     * DiskArrays.
     *
     * @throws InvalidTopologyException if there is a problem converting the topology to TraderTO's
     */
    @Test
    public void testScopeTopologyCluster1() throws InvalidTopologyException {

        final TopologyConverter converter = new TopologyConverter(PLAN_TOPOLOGY_INFO);
        final Set<EconomyDTOs.TraderTO> traderTOs = converter
                .convertToMarket(topologyDTOs);

        Set<EconomyDTOs.TraderTO> scopedTraderTOs = testAnalysis.scopeTopology(traderTOs,
                Sets.newHashSet(HOST_12_OID));

        // "Host #12", "VM #14", "App #15", "Datastore #2,4,6,8,10", "DiskArray #1-10", "Datacenter #0"
        assertThat(scopedTraderTOs.size(), equalTo(CLUSTER_1_EXPANDED_SE_COUNT));
    }

    /**
     * In this test, we start with "Host #11", which has no applications. Therefore there are no
     * "upwards" traders. The only other element in the Scoped Topology is the "downwards" Datacenter.
     * @throws InvalidTopologyException if there is a problem converting the topology to TraderTO's
     */
    @Test
    public void testScopeTopologyOneHost() throws InvalidTopologyException {
        final TopologyConverter converter = new TopologyConverter(PLAN_TOPOLOGY_INFO);
        final Set<EconomyDTOs.TraderTO> traderTOs = converter
                .convertToMarket(topologyDTOs);

        Set<EconomyDTOs.TraderTO> scopedTraderTOs = testAnalysis.scopeTopology(traderTOs,
                Sets.newHashSet(HOST_11_OID));

        // "Host #11", "Datacenter #0"
        assertThat(scopedTraderTOs.size(), equalTo(2));
    }

    /**
     * In this test, we start with "Host #11" and "Host #13", neither of which has any applications.
     * Therefore there are no "upwards" traders. The only other element in the Scoped Topology
     * is the "downwards" Datacenter.
     * @throws InvalidTopologyException if there is a problem converting the topology to TraderTO's
     */
    @Test
    public void testScopeTopologyTwoHosts() throws InvalidTopologyException {
        final TopologyConverter converter = new TopologyConverter(PLAN_TOPOLOGY_INFO);
        final Set<EconomyDTOs.TraderTO> traderTOs = converter
                .convertToMarket(topologyDTOs);

        Set<EconomyDTOs.TraderTO> scopedTraderTOs = testAnalysis.scopeTopology(traderTOs,
                Sets.newHashSet(HOST_11_OID, HOST_13_OID));

        // "Host #11", "Host #13", "Datacenter #0"
        assertThat(scopedTraderTOs.size(), equalTo(3));
    }

    /**
     * Test a scoped analysis run with the simple test topology.
     *
     * Scoped to testScopeTopologyCluster1, which consists of the host HOST_12_OID,
     * there should be 10 SE's in the source and projected topology (see testScopeTopologyCluster1
     * above).
     *
     * @throws InterruptedException since we use sleep(.)
     * @throws CommunicationException should never occur
     */
    @Test
    public void testScopedMarketRunner() throws InterruptedException, CommunicationException {

        // Arrange
        MarketNotificationSender serverApi = Mockito.mock(MarketNotificationSender.class);
        ExecutorService threadPool = Executors.newFixedThreadPool(2);
        Analysis.AnalysisFactory analysisFactory = new Analysis.AnalysisFactory();
        MarketRunner runner = new MarketRunner(threadPool, serverApi, analysisFactory);

        long topologyContextId = 1000;
        long topologyId = 2000;
        long creationTime = 3000;

        TopologyDTO.TopologyInfo topologyInfo = TopologyDTO.TopologyInfo.newBuilder()
                .setTopologyId(topologyId)
                .setTopologyContextId(topologyContextId)
                .setCreationTime(creationTime)
                .setTopologyType(TopologyDTO.TopologyType.PLAN)
                .addScopeSeedOids(HOST_12_OID)
                .build();

        // Act
        Analysis analysis = runner.scheduleAnalysis(topologyInfo, topologyDTOs, true);
        assertTrue(runner.getRuns().contains(analysis));
        while (!analysis.isDone()) {
            Thread.sleep(100);
        }
        assertSame("Plan completed with an error : " + analysis.getErrorMsg(),
                Analysis.AnalysisState.SUCCEEDED, analysis.getState());

        // Assert
        Mockito.verify(serverApi, Mockito.times(1)).notifyActionsRecommended(analysis.getActionPlan().get());

        // since the IDgenerator gives us a different projectedTopoID every time, we create a
        // MockitoMatcher using anyLong to represent this parameter
        Mockito.verify(serverApi, Mockito.times(1))
                .notifyProjectedTopology(eq(topologyInfo), anyLong(),
                        eq(analysis.getProjectedTopology().get()));
        assertThat(analysis.getPriceIndexMessage().isPresent(), equalTo(true));
        PriceIndexDTOs.PriceIndexMessage pim = PriceIndexDTOs.PriceIndexMessage
                .newBuilder(analysis.getPriceIndexMessage().get())
                .setTopologyContextId(analysis.getContextId())
                .build();
        Mockito.verify(serverApi).sendPriceIndex(eq(topologyInfo), eq(pim));

        // check the original topology size
        assertThat(analysis.getTopology().size(), equalTo(topologyDTOs.size()));

        // check projected topology -  "Host #12", "VM #14", "App #15", "Datastore #2,4,6,8,10",
        // "DiskArray #1-10", "Datacenter #0"
        assertThat(analysis.getProjectedTopology().isPresent(), equalTo(true));
        assertThat(analysis.getProjectedTopology().get().size(), equalTo(CLUSTER_1_EXPANDED_SE_COUNT));

        // check the output 'priceIndex' info to make sure it has the correct size
        assertThat(analysis.getPriceIndexMessage().get().getPayloadList().size(),
                equalTo(CLUSTER_1_EXPANDED_SE_COUNT));
    }


    /**
     * "App #15": 72207427031437 buys from "VM #14": 72207427031425
     * "VM #14": 72207427031425, buys from:
     * <ul>
     * <li>"Host #12": 72207427031409 - CLUSTER("CLUSTER-1"), CPU, MEM, NET_THROUGHPUT
     * <li>"Datastore #6": 72207427031433 - STORAGE_CLUSTER("STORAGE-CLUSTER-1"), STORAGE_AMOUNT, DRS_SEGMENTATION
     * </ul>
     * from "Host #12": 72207427031409, "Datastore #6": 72207427031433
     * "Host #11": 72207427031424, buys from "Datacenter #0": 72207427031436
     * "Host #12": 72207427031409, buys from "Datacenter #0": 72207427031436
     * "Host #13": 72207427031408, buys from "Datacenter #0": 72207427031436
     *
     * "Datastore #1": 72207427031430,
     * "Datastore #2": 72207427031428
     * "Datastore #3": 72207427031429
     * "Datastore #4": 72207427031426
     * "Datastore #5": 72207427031427
     * "Datastore #6": 72207427031433
     * "Datastore #7": 72207427031434
     * "Datastore #8": 72207427031431
     * "Datastore #9": 72207427031432
     * "Datastore #10": 72207427031435
     *
     * "Datacenter #0": 72207427031436
     *
     * sellers of CLUSTER("CLUSTER-0") = "Host #11", "Host #13"
     * sellers of CLUSTER("CLUSTER-1") = "Host #12"
     *
     * sellers of STORAGE_CLUSTER("STORAGE-CLUSTER-0") = Datastore #1,3,5,7,9
     * sellers of STORAGE_CLUSTER("STORAGE-CLUSTER-1") = Datastore #2,4,6,8,10
     *
     * the disk arrays are not listed here, but "Datastore #n" buys EXTENT("DiskArray #n")
     *
     * @return a Set of TopologyentityDTO Protobufs read from the file "simple-topology.json"
     * @throws FileNotFoundException if the test file is not found
     * @throws InvalidProtocolBufferException if the JSON file has the wrong format
     */
    private Set<TopologyEntityDTO> readTopologyFromJsonFile()
            throws FileNotFoundException, InvalidProtocolBufferException {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final URL topologyFileResource = classLoader.getResource(SIMPLE_TOPOLOGY_JSON_FILE);
        if (topologyFileResource == null) {
            throw new FileNotFoundException("Error reading " + SIMPLE_TOPOLOGY_JSON_FILE);
        }
        File file = new File(topologyFileResource.getFile());
        final InputStream dtoInputStream = new FileInputStream(file);
        InputStreamReader inputStreamReader = new InputStreamReader(dtoInputStream);
        JsonReader topologyReader = new JsonReader(inputStreamReader);
        List<Object> dtos = GSON.fromJson(topologyReader, List.class);

        Set<TopologyEntityDTO> topologyDTOs = Sets.newHashSet();
        for (Object dto : dtos) {
            String dtoString = GSON.toJson(dto);
            TopologyEntityDTO.Builder entityDtoBuilder =
                    TopologyEntityDTO.newBuilder();
            JsonFormat.parser().merge(dtoString, entityDtoBuilder);
            topologyDTOs.add(entityDtoBuilder.build());
        }
        return topologyDTOs;
    }

}