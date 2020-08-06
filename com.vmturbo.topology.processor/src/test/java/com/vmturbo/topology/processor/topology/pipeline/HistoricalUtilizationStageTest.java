package com.vmturbo.topology.processor.topology.pipeline;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.processor.historical.HistoricalCommodityInfo;
import com.vmturbo.topology.processor.historical.HistoricalInfo;
import com.vmturbo.topology.processor.historical.HistoricalUtilizationDatabase;
import com.vmturbo.topology.processor.topology.HistoricalEditor;
import com.vmturbo.topology.processor.topology.pipeline.Stages.HistoricalUtilizationStage;

/**
 * Unit test for {@link HistoricalUtilizationStage}.
 */
public class HistoricalUtilizationStageTest {
    private static final long CLOUD_VIRTUAL_MACHINE_ID = 1;
    private static final long ON_PREM_VIRTUAL_MACHINE_ID = 2;
    private static final long PHYSICAL_MACHINE_ID = 3;
    private static final double NET_THROUGHPUT_USED = 100;
    private static final double NET_THROUGHPUT_PEAK = 200;
    private static final double IO_THROUGHPUT_USED = 300;
    private static final double IO_THROUGHPUT_PEAK = 400;
    private static final double CPU_USED = 500;
    private static final double CPU_PEAK = 600;
    private static final double DELTA = 0.001D;

    private HistoricalUtilizationDatabase historicalUtilizationDatabase;
    private HistoricalUtilizationStage historicalUtilizationStage;
    private TopologyGraph<TopologyEntity> topologyGraph;
    private final TopologyPipelineContext context = mock(TopologyPipelineContext.class);

    /**
     * Objects initialization necessary for a unit test.
     * Building a topology consisting of 2 virtual machines and 1 physical machine.
     * The first virtual machine from {@link EnvironmentType#ON_PREM} environment.
     * The second virtual machine from {@link EnvironmentType#CLOUD} environment.
     */
    @Before
    public void setUp() {
        final CommodityBoughtDTO netThroughputCommodityBoughtDTO = CommodityBoughtDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityType.NET_THROUGHPUT_VALUE))
                .setUsed(NET_THROUGHPUT_USED)
                .setPeak(NET_THROUGHPUT_PEAK)
                .build();
        final CommodityBoughtDTO ioThroughputCommodityBoughtDTO = CommodityBoughtDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(CommodityType.IO_THROUGHPUT_VALUE))
                .setUsed(IO_THROUGHPUT_USED)
                .setPeak(IO_THROUGHPUT_PEAK)
                .build();
        final CommodityBoughtDTO cpuCommodityBoughtDTO = CommodityBoughtDTO.newBuilder()
                .setCommodityType(
                        TopologyDTO.CommodityType.newBuilder().setType(CommodityType.CPU_VALUE))
                .setUsed(CPU_USED)
                .setPeak(CPU_PEAK)
                .build();
        final CommodityBoughtDTO clusterCommodityBoughtDTO = CommodityBoughtDTO.newBuilder()
                .setCommodityType(
                        TopologyDTO.CommodityType.newBuilder().setType(CommodityType.CLUSTER_VALUE))
                .build();
        final List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProviders =
                Collections.singletonList(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .setProviderId(PHYSICAL_MACHINE_ID)
                        .addAllCommodityBought(Arrays.asList(netThroughputCommodityBoughtDTO,
                                ioThroughputCommodityBoughtDTO, cpuCommodityBoughtDTO,
                                clusterCommodityBoughtDTO))
                        .build());
        final Builder cloudVirtualMachine = TopologyEntity.newBuilder(TopologyEntityDTO.newBuilder()
                .setOid(CLOUD_VIRTUAL_MACHINE_ID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addAllCommoditiesBoughtFromProviders(commoditiesBoughtFromProviders));
        final Builder onPremVirtualMachine = TopologyEntity.newBuilder(
                TopologyEntityDTO.newBuilder()
                        .setOid(ON_PREM_VIRTUAL_MACHINE_ID)
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setEnvironmentType(EnvironmentType.ON_PREM)
                        .addAllCommoditiesBoughtFromProviders(commoditiesBoughtFromProviders));
        topologyGraph = new TopologyGraphCreator<Builder, TopologyEntity>().addEntities(
                Arrays.asList(onPremVirtualMachine, cloudVirtualMachine)).build();

        historicalUtilizationDatabase = Mockito.mock(HistoricalUtilizationDatabase.class);
        final ExecutorService executorService = Mockito.mock(ExecutorService.class);
        Mockito.when(executorService.submit(Mockito.any(Runnable.class)))
                .thenAnswer((invocation -> {
                    final Runnable runnable = invocation.getArgumentAt(0, Runnable.class);
                    runnable.run();
                    return Mockito.mock(Future.class);
                }));
        final HistoricalEditor historicalEditor =
                new HistoricalEditor(historicalUtilizationDatabase, executorService);
        historicalUtilizationStage = new HistoricalUtilizationStage(historicalEditor);
    }

    /**
     * Check that the smooth values for the bought commodities are calculated for entities
     * from {@link EnvironmentType#ON_PREM} and {@link EnvironmentType#CLOUD} environments.
     *
     * @throws PipelineStageException when failed
     */
    @Test
    public void testProcessCommodityBoughtListForCloudAndOnPremEntities()
            throws PipelineStageException {
        when(context.getTopologyInfo()).thenReturn(TopologyInfo.newBuilder().build());
        ReflectionTestUtils.setField(historicalUtilizationStage, "context", context);
        historicalUtilizationStage.passthrough(topologyGraph);

        final ArgumentCaptor<HistoricalInfo> captor = ArgumentCaptor.forClass(HistoricalInfo.class);
        Mockito.verify(historicalUtilizationDatabase).saveInfo(captor.capture());

        final Map<Long, Map<Integer, HistoricalCommodityInfo>> historicalInfo = captor.getValue()
                .getOidToHistoricalSEInfo()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Entry::getKey, (e) -> e.getValue()
                        .getHistoricalCommodityBought()
                        .stream()
                        .collect(Collectors.toMap((h) -> h.getCommodityTypeAndKey().getType(),
                                Function.identity()))));

        checkCommodities(historicalInfo.get(CLOUD_VIRTUAL_MACHINE_ID));
        checkCommodities(historicalInfo.get(ON_PREM_VIRTUAL_MACHINE_ID));
    }

    private static void checkCommodities(
            Map<Integer, HistoricalCommodityInfo> commodityNumberToHistoricalCommodityInfo) {
        final HistoricalCommodityInfo ioThroughputCommodity =
                commodityNumberToHistoricalCommodityInfo.get(CommodityType.IO_THROUGHPUT_VALUE);
        final HistoricalCommodityInfo netThroughputCommodity =
                commodityNumberToHistoricalCommodityInfo.get(CommodityType.NET_THROUGHPUT_VALUE);
        final HistoricalCommodityInfo cpu =
                commodityNumberToHistoricalCommodityInfo.get(CommodityType.CPU_VALUE);
        final HistoricalCommodityInfo clusterCommodity =
                commodityNumberToHistoricalCommodityInfo.get(CommodityType.CLUSTER_VALUE);
        Assert.assertEquals(IO_THROUGHPUT_USED, ioThroughputCommodity.getHistoricalUsed(), DELTA);
        Assert.assertEquals(IO_THROUGHPUT_PEAK, ioThroughputCommodity.getHistoricalPeak(), DELTA);
        Assert.assertEquals(NET_THROUGHPUT_USED, netThroughputCommodity.getHistoricalUsed(), DELTA);
        Assert.assertEquals(NET_THROUGHPUT_PEAK, netThroughputCommodity.getHistoricalPeak(), DELTA);
        Assert.assertEquals(CPU_USED, cpu.getHistoricalUsed(), DELTA);
        Assert.assertEquals(CPU_PEAK, cpu.getHistoricalPeak(), DELTA);
        Assert.assertNull(clusterCommodity);
    }
}
