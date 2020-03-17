package com.vmturbo.topology.graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.ApiEntityType;

@Ignore
public class TopologyEntityTopologyGraphIT {

    private static final Logger logger = LogManager.getLogger();

    private static final int APP_COUNT = 500000;
    private static final int VM_COUNT = 500000;
    private static final int HOST_COUNT = 5000;
    private static final int STORAGE_COUNT = 5000;
    private static final int DC_COUNT = 500;
    private static final int SEED = 123456;

    private final Random random  = new Random(SEED);
    private final List<Long> datacenterArray = new ArrayList<>(DC_COUNT);
    private final List<Long> hostArray = new ArrayList<>(HOST_COUNT);
    private final List<Long> storageArray = new ArrayList<>(STORAGE_COUNT);
    private final List<Long> vmArray = new ArrayList<>(VM_COUNT);
    private final List<Long> appArray = new ArrayList<>(APP_COUNT);

    // Note!!!!
    // To run with memory measurements, see https://github.com/DimitrisAndreou/memory-measurer
    // Download and add the library to your project in your IDE, add the -javaagent JVM argument
    // to your test run arguments, and uncomment the memory measurer lines.
    @Test
    public void gatherPerformanceMetrics() {
        final Map<Long, TestGraphEntity.Builder> topologyMap = buildTopologyMap();
        final TopologyGraph<TestGraphEntity> graph = benchmark("Creating graph",
            () -> TestGraphEntity.newGraph(topologyMap),
            TimeUnit.MILLISECONDS);

        final int startingDcCount = 100;
        final Collection<Long> traversedApplications = benchmark(
            "Traversal from " + startingDcCount + " datacenters to all connected applications",
            () -> testTraversal(graph,
                IntStream.range(0, 100)
                    .mapToObj(i -> datacenterArray.get(random.nextInt(datacenterArray.size())))
                    .collect(Collectors.toList())),
            TimeUnit.MILLISECONDS);

        System.out.println("Traversed to " + traversedApplications.size() + " applications.");
        printGraphMemoryUsage(graph);
    }

    @Nonnull
    public Map<Long, TestGraphEntity.Builder> buildTopologyMap() {
        System.out.println("Building topology map...");
        Map<Long, TestGraphEntity.Builder> map = new HashMap<>();
        long nextVertex = 0;
        long nextEdge = 0;

        for (int i = 0; i < DC_COUNT; i++, nextVertex++) {
            map.put(nextVertex, TestGraphEntity.newBuilder(nextVertex, ApiEntityType.DATACENTER));
            datacenterArray.add(nextVertex);
        }

        for (int i = 0; i < HOST_COUNT; i++, nextVertex++, nextEdge++) {
            long pm = nextVertex;
            long dc = datacenterArray.get(random.nextInt(datacenterArray.size()));
            map.put(pm, TestGraphEntity.newBuilder(pm, ApiEntityType.PHYSICAL_MACHINE).addProviderId(dc));
            hostArray.add(pm);
        }

        for (int i = 0; i < STORAGE_COUNT; i++, nextVertex++) {
            map.put(nextVertex, TestGraphEntity.newBuilder(nextVertex, ApiEntityType.STORAGE));
            storageArray.add(nextVertex);
        }

        for (int i = 0; i < VM_COUNT; i++, nextVertex++) {
            long vm = nextVertex;
            long pm = hostArray.get(random.nextInt(hostArray.size()));
            long st = storageArray.get(random.nextInt(storageArray.size()));

            map.put(vm, TestGraphEntity.newBuilder(vm, ApiEntityType.VIRTUAL_MACHINE)
                .addProviderId(pm)
                .addProviderId(st));
            vmArray.add(vm);
        }

        for (int i = 0; i < APP_COUNT; i++, nextVertex++, nextEdge++) {
            long app = nextVertex;
            long vm = vmArray.get(random.nextInt(vmArray.size()));

            map.put(app, TestGraphEntity.newBuilder(app, ApiEntityType.APPLICATION)
                .addProviderId(vm));
            appArray.add(app);
        }

        return map;
    }

    private Collection<Long> testTraversal(@Nonnull final TopologyGraph<TestGraphEntity> graph,
                                                        @Nonnull final Collection<Long> startingDatacenters) {
        return startingDatacenters.stream()
            .flatMap(startDc -> graph.getConsumers(startDc)
                .flatMap(pm -> graph.getConsumers(pm)
                    .flatMap(graph::getConsumers)))
            .distinct()
            .map(TestGraphEntity::getOid)
            .collect(Collectors.toList());
    }

    private <T> T benchmark(@Nonnull final String operationName,
                            @Nonnull final Supplier<T> methodToBench,
                            @Nonnull final TimeUnit units) {
        long startTime = System.currentTimeMillis();

        final T output = methodToBench.get();

        long operationDuration = units.convert(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
        System.out.println(operationName + " took " + operationDuration + " " + units.toString());

        return output;
    }

    private void printGraphMemoryUsage(@Nonnull final TopologyGraph<TestGraphEntity> graph) {
        // Comment out the below line to measure graph memory usage in megabytes
        System.out.println("Skipping memory measurement. See comments in code on how to enable locally...");

//        long graphSizeBytes = MemoryMeasurer.measureBytes(graph);
//        System.out.println("Graph size (including protobuf info) in MB: " + graphSizeBytes / (1024 * 1024));
//
//        long protobufSizeBytes = graph.entities()
//            .map(TopologyEntity::getTopologyEntityDtoBuilder)
//            .mapToLong(MemoryMeasurer::measureBytes)
//            .sum();
//        long excludingProtobufBytes = graphSizeBytes - protobufSizeBytes;
//
//        System.out.println("Graph size excluding protobuf in MB: " + excludingProtobufBytes / (1024 * 1024));
    }
}
