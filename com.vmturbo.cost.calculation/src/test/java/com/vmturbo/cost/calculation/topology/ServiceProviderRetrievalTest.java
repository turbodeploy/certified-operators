package com.vmturbo.cost.calculation.topology;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests for {@link TopologyEntityCloudTopology#getServiceProvider(TopologyEntityDTO)}.
 */
public class ServiceProviderRetrievalTest {

    private static final Set<Integer> EXCLUDED_TYPES = ImmutableSet.of(EntityType.SERVICE_PROVIDER_VALUE);

    /**
     * Test using Azure topology.
     *
     * @throws IOException in case of loading topology error
     */
    @Test
    public void testAzureTopo() throws IOException {
        final List<TopologyEntityDTO> entities = loadTopology("topology_azure.json");
        doTest(entities);
    }

    /**
     * Test using AWS topology.
     *
     * @throws IOException in case of loading topology error
     */
    @Test
    public void testAWSTopo() throws IOException {
        final List<TopologyEntityDTO> entities = loadTopology("topology_aws.json");
        doTest(entities);
    }

    /**
     * Test case where there is cycled topology graph.
     *
     * @throws IOException in case of loading topology error
     */
    @Test
    public void testLoopTopo() throws IOException {
        final List<TopologyEntityDTO> entities = loadTopology("topology_looped.json");
        TopologyEntityCloudTopology t = new TopologyEntityCloudTopology(entities.stream(),
                mock(GroupMemberRetriever.class));
        Optional<TopologyEntityDTO> sp = t.getServiceProvider(entities.iterator().next());
        Assert.assertFalse(sp.isPresent());
    }


    private static void doTest(List<TopologyEntityDTO> entities) {
        Map<Integer, TopologyEntityDTO> entityType2entity = entities.stream()
                .collect(Collectors.toMap(TopologyEntityDTO::getEntityType, x -> x));

        TopologyEntityDTO serviceProvider =
                entityType2entity.get(EntityType.SERVICE_PROVIDER_VALUE);

        TopologyEntityCloudTopology t = new TopologyEntityCloudTopology(entities.stream(),
                mock(GroupMemberRetriever.class));

        for (TopologyEntityDTO entity : entities) {
            if (!EXCLUDED_TYPES.contains(entity.getEntityType())) {
                Optional<TopologyEntityDTO> newSP = t.getServiceProvider(entity);
                EntityType eType = EntityType.forNumber(entity.getEntityType());
                Assert.assertTrue(String.format("ServiceProvider not found for entity type: %s",
                        eType), newSP.isPresent());
                Assert.assertEquals(String.format("Wrong ServiceProvider for entity type: %s",
                        eType), serviceProvider, newSP.get());
            }
        }
    }

    private static List<TopologyEntityDTO> loadTopology(@Nonnull String path) throws IOException {
        try (InputStream stream = ServiceProviderRetrievalTest.class.getClassLoader()
                .getResourceAsStream(path); Reader reader = new InputStreamReader(stream)) {
            return Arrays.asList(
                    ComponentGsonFactory.createGson().fromJson(reader, TopologyEntityDTO[].class));
        }
    }
}
