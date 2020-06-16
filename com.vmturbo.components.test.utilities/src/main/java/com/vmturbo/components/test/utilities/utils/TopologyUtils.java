package com.vmturbo.components.test.utilities.utils;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.sdk.examples.stressProbe.StressAccount;
import com.vmturbo.sdk.examples.stressProbe.StressProbe;

/**
 * Utilities for constructing topologies used in tests.
 */
public class TopologyUtils {
    /**
     * The ratios below are computed based on ratios to achieve a moderately high resource utilization
     * density while also being able to fit all the entities on their available suppliers.
     * See https://vmturbo.atlassian.net/wiki/spaces/Home/pages/90571158/Stress+probe for details.
     *
     * APPLICATIONS: 34,000
     * VIRTUAL_MACHINES: 34,000
     * HOSTS: 2,000
     * STORAGES: 2,500
     * CLUSTER: 125
     * DATACENTER: 1
     */
    private static final double CANONICAL_TOPOLOGY_SIZE = 75000.0;
    private static final double CANONICAL_VM_COUNT = 34000.0;
    private static final double APP_RATIO = CANONICAL_VM_COUNT / CANONICAL_TOPOLOGY_SIZE;
    private static final double VM_RATIO = CANONICAL_VM_COUNT / CANONICAL_TOPOLOGY_SIZE;
    private static final double HOST_RATIO = 2000 / CANONICAL_TOPOLOGY_SIZE;
    // Note that an equal number of disk-arrays are added per storage which are hidden from the count.
    private static final double STORAGE_RATIO = 2500 / CANONICAL_TOPOLOGY_SIZE;
    private static final double CLUSTER_RATIO = 125 / CANONICAL_VM_COUNT;

    private static final int RANDOM_SEED = 1234567;

    /**
     * Generate a stress account with a reasonable distribution of entities for a topology of
     * a given size.
     *
     * @param topologySize The number of service entities in the stress account topology.
     * @return A {@linnk StressAccount} for use with a {@link StressProbe} that will produce
     *         a topology approximately the requested size.
     */
    public static StressAccount generateStressAccount(final int topologySize) {
        final Function<Double, Integer> calculator = (ratio) -> Math.max((int)Math.round(ratio * topologySize), 1);

        final int appCount = calculator.apply(APP_RATIO);
        final int vmCount = calculator.apply(VM_RATIO);
        final int hostCount = calculator.apply(HOST_RATIO);
        final int storageCount = calculator.apply(STORAGE_RATIO);
        final int clusterCount = calculator.apply(CLUSTER_RATIO);
        final int datacenterCount = 1;

        return new StressAccount("STRESS", vmCount, appCount, hostCount,
            storageCount, clusterCount, datacenterCount, RANDOM_SEED);
    }

    /**
     * Generate a topology of a given size using the StressProbe.
     * The number of VMs, PMs, Apps, Storages, clusters, etc. are given by fixed ratios
     * applied to the input topology.
     *
     * Note that the size of the topology created may not be EXACTLY the input size specified,
     * but should be approximately the size. This is because balancing the ratios of service entities
     * may not be possible with integer-sized numbers of entities.
     *
     * The created topology will always create exactly one datacenter, and a minimum of 1 of each of the following:
     * application, virtual machine, host, storage, cluster
     *
     * Uses a fixed random seed.
     *
     * @param topologySize The number of service entities to generate in the topology.
     * @return A topology consisting of a list of service entities. The size of the list
     *         will be (approximately) the size of the input size.
     * @throws InterruptedException If topologoy generation was interrupted.
     */
    public static List<EntityDTO> generateProbeTopology(final int topologySize) throws InterruptedException {
        final StressAccount account = generateStressAccount(topologySize);

        final StressProbe stressProbe = new StressProbe();
        return stressProbe.discoverTarget(account).getEntityDTOList();
    }

    /**
     * Follows the same contract as {@link #generateProbeTopology(int)} except that it returns a list of
     * {@link TopologyEntityDTO} as converted by the TopologyProcessor's {@link SdkToTopologyEntityConverter} instead of
     * the list of discovered {@link EntityDTO}s.
     *
     * @param topologySize The size of the topology to create.
     * @return A topology consisting of a list of service entities. The size of the list
     *         will be (approximately) the size of the input size.
     * @throws InterruptedException If topologoy generation was interrupted.
     */
    public static List<TopologyEntityDTO> generateTopology(final int topologySize) throws InterruptedException {
        return Collections.emptyList();
//        final AtomicLong nextId = new AtomicLong(0);
//        final List<EntityDTO> entities = generateProbeTopology(topologySize);
//
//        final Map<Long, EntityDTO> entitiesWithOids = entities.stream()
//            .collect(Collectors.toMap(
//                entityDto -> nextId.incrementAndGet(),
//                Function.identity()));
//
//        throw
//
//        return SdkToTopologyEntityConverter.convertToTopologyEntityDTOs(entitiesWithOids).stream()
//                .map(TopologyEntityDTO.Builder::build)
//                .collect(Collectors.toList());
    }

    /**
     * How to run:
     * mvn  -q clean install exec:java  -DskipTests=true -Dcheckstyle.skip=true -Dexec.mainClass=com.vmturbo.components.test.utilities.utils.TopologyUtils -Dexec.args=<topology size>
     * where topology size is an integer or an
     *  integer optionally followed by the prefix "k" which will multiply the size by 1000.
     *  i.e. 200k will generate a topology with StressAccount recommendation for a
     *  topology of approximately 200,000 entities.
     */
    public static void main(String[] args) {
        String helpMsg =
        "args=<topology size> where topology size is an integer or an integer\n"+
        "   followed by the prefix 'k' which will multiply the size by 1000.\n" +
        "   i.e. 200k will generate a topology with StressAccount recommendation\n" +
        "   for a topology of approximately 200,000 entities.";

        if (args.length != 1 || args[0].trim().isEmpty()) {
            System.out.println("topologySize argument missing.\n" + helpMsg);
            System.exit(1);
        }
        try {
            String input = args[0].trim();
            int topologySize;
            if (input.length()>1 && input.charAt(input.length()-1) == 'k') {
                topologySize = Integer.parseInt(input.substring(0, input.length()-1));
                topologySize = topologySize * 1000;
            } else {
                topologySize = Integer.parseInt(args[0]);
            }
            System.out.println(generateStressAccount(topologySize));
        }
        catch (NumberFormatException | ArithmeticException ex) {
            System.out.println("Invalid input. Not an integer: " + args[0] + "\n" + helpMsg);
            System.exit(1);
        }
    }
}
