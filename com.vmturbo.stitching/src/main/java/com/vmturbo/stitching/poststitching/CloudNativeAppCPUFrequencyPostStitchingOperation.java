package com.vmturbo.stitching.poststitching;

import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ContainerPodInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Set nodeCpuFrequency in the application data for application components discovered by APM probe
 * and stitched onto a container discovered by kubeturbo.
 */
public class CloudNativeAppCPUFrequencyPostStitchingOperation implements PostStitchingOperation {

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.probeCategoryEntityTypeScope(
                ProbeCategory.GUEST_OS_PROCESSES, EntityType.APPLICATION_COMPONENT);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> entities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        entities.filter(this::isHostedOnContainerPod)
                .filter(this::hasNoNodeCpuFrequency)
                .forEach(application -> resultBuilder.queueUpdateEntityAlone(
                        application, this::addCPUFrequencyData));
        return resultBuilder.build();
    }

    private boolean isHostedOnContainerPod(@Nonnull final TopologyEntity application) {
        return application.getProviders().stream()
                .filter(provider -> provider.getEntityType() == EntityType.CONTAINER_VALUE)
                .map(TopologyEntity::getProviders)
                .flatMap(List::stream)
                .anyMatch(provider -> provider.getEntityType() == EntityType.CONTAINER_POD_VALUE);
    }

    private boolean hasNoNodeCpuFrequency(@Nonnull final TopologyEntity application) {
        return !application
                .getTopologyEntityDtoBuilder()
                .getTypeSpecificInfo()
                .getApplication()
                .hasHostingNodeCpuFrequency();
    }

    private void addCPUFrequencyData(@Nonnull final TopologyEntity application) {
        application.getProviders().stream()
                .filter(provider -> provider.getEntityType() == EntityType.CONTAINER_VALUE)
                .map(TopologyEntity::getProviders)
                .flatMap(List::stream)
                .filter(provider -> provider.getEntityType() == EntityType.CONTAINER_POD_VALUE)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .map(TopologyEntityDTO.Builder::getTypeSpecificInfo)
                .map(TypeSpecificInfo::getContainerPod)
                .filter(ContainerPodInfo::hasHostingNodeCpuFrequency)
                .map(ContainerPodInfo::getHostingNodeCpuFrequency)
                .findFirst()
                .ifPresent(nodeCpuFreq -> application
                        .getTopologyEntityDtoBuilder()
                        .getTypeSpecificInfoBuilder()
                        .getApplicationBuilder()
                        .setHostingNodeCpuFrequency(nodeCpuFreq));
    }
}
