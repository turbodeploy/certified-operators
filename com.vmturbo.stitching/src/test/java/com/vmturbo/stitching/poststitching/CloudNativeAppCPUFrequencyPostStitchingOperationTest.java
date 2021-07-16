package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntity;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeTopologyEntityBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerPodInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.TopologyEntityBuilder;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * Unit tests for {@link CloudNativeAppCPUFrequencyPostStitchingOperation}.
 */
public class CloudNativeAppCPUFrequencyPostStitchingOperationTest {
    private final CloudNativeAppCPUFrequencyPostStitchingOperation operation =
            new CloudNativeAppCPUFrequencyPostStitchingOperation();
    private EntityChangesBuilder<TopologyEntity> resultBuilder;
    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);
    // Application without provider
    private final TopologyEntity appWithoutProvider =
            makeTopologyEntityBuilder(EntityType.APPLICATION_COMPONENT_VALUE,
                                      Collections.emptyList(),
                                      Collections.emptyList())
                    .build();
    // Application on container provider, and already has node CPU frequency data
    private final TopologyEntity appOnContainerWithExistingNodeCpuFrequency =
            TopologyEntityBuilder
                    .newBuilder()
                    .withEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                    .withProviders(ImmutableList.of(
                            makeTopologyEntityBuilder(EntityType.CONTAINER_VALUE,
                                                      Collections.emptyList(),
                                                      Collections.emptyList())))
                    .withTypeSpecificInfo(
                            TypeSpecificInfo.newBuilder()
                                    .setApplication(ApplicationInfo.newBuilder()
                                                            .setHostingNodeCpuFrequency(2000)
                                                            .build()))
                    .build();
    // Application on container provider, which does not have a pod provider
    private final TopologyEntity appOnContainerWithoutProvider =
            makeTopologyEntity(EntityType.APPLICATION_COMPONENT_VALUE,
                               Collections.emptyList(),
                               Collections.emptyList(),
                               ImmutableList.of(makeTopologyEntityBuilder(EntityType.CONTAINER_VALUE,
                                                                          Collections.emptyList(),
                                                                          Collections.emptyList())));
    // Application on container provider, which is on pod provider
    private final TopologyEntity.Builder pod =
            TopologyEntityBuilder
                    .newBuilder()
                    .withEntityType(EntityType.CONTAINER_POD_VALUE)
                    .withTypeSpecificInfo(
                            TypeSpecificInfo.newBuilder()
                                    .setContainerPod(ContainerPodInfo.newBuilder()
                                                             .setHostingNodeCpuFrequency(2000)
                                                             .build()))
                    .getBuilder();
    private final TopologyEntity.Builder container =
            TopologyEntityBuilder
                    .newBuilder()
                    .withEntityType(EntityType.CONTAINER_VALUE)
                    .withProviders(ImmutableList.of(pod))
                    .getBuilder();
    private final TopologyEntity appOnContainerWithPod =
            TopologyEntityBuilder
                    .newBuilder()
                    .withEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
                    .withProviders(ImmutableList.of(container))
                    .build();

    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> journal =
            (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    /**
     * Setup the tests.
     */
    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();
    }

    /**
     * Test that applications without providers don't need populate CPU frequency.
     */
    @Test
    public void testAppWithNoProvider() {
        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(appWithoutProvider), settingsMock, resultBuilder);
        assertTrue(result.getChanges().isEmpty());
    }

    /**
     * Test that applications running on container, and already have nodeCpuFrequency data don't
     * need populate CPU frequency.
     */
    @Test
    public void testAppOnContainerWithExistingNodeCpuFrequency() {
        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(appOnContainerWithExistingNodeCpuFrequency),
                                           settingsMock, resultBuilder);
        assertTrue(result.getChanges().isEmpty());
    }

    /**
     * Test that applications running on containers without pod providers don't need need populate
     * CPU frequency.
     */
    @Test
    public void testAppOnContainerWithoutProvider() {
        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(appOnContainerWithoutProvider),
                                           settingsMock, resultBuilder);
        assertTrue(result.getChanges().isEmpty());
    }

    /**
     * Test that applications running on containers with pod providers need populate CPU frequency.
     */
    @Test
    public void testAppOnContainerPod() {
        final TopologicalChangelog<TopologyEntity> result =
                operation.performOperation(Stream.of(appOnContainerWithPod),
                                           settingsMock, resultBuilder);
        assertEquals(1, result.getChanges().size());
        // apply the changes
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        assertTrue(appOnContainerWithPod.getTypeSpecificInfo().getApplication()
                            .hasHostingNodeCpuFrequency());
    }
}
