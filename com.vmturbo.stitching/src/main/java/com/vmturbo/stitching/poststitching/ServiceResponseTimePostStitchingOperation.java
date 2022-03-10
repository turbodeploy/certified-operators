package com.vmturbo.stitching.poststitching;

import java.util.Collections;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.serviceslo.ServiceSLOStitchingOperation;

/**
 * Set response time of Service entities by averaging the value across number of replicas.
 *
 * <p>For now, we only perform the operation on Service entities coming from Custom probes, for
 * example, Prometurbo and DIF.
 * During stitching, response time of each service replica coming from Custom probes gets
 * patched and aggregated onto the real service discovered by kuberturbo in the
 * {@link ServiceSLOStitchingOperation} custom stitching operation. This post stitching
 * operation divides the aggregated response time value by the number of service replicas to
 * calculate the average response time of a service.
 *
 * <p>TODO: The simple averaging of total response time by number of service replicas is flawed.
 *     We should use weighted average to calculate the response time of a service with multiple
 *     replicas, but Platform does not have enough information to carry out this computation. We
 *     have decided that the proper fix is to make sure that any APM or Custom probe should properly
 *     aggregate the response time and transaction metrics and only send in one proxy service with
 *     aggregated metrics. See https://vmturbo.atlassian.net/browse/OM-70870 for more details.
 *
 * <p>This operation must be performed before {@link SetAutoSetCommodityCapacityPostStitchingOperation}.
 */
public class ServiceResponseTimePostStitchingOperation implements PostStitchingOperation {
    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.probeCategoryEntityTypeScope(
                ProbeCategory.CUSTOM, EntityType.SERVICE);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> entities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        entities.filter(this::serviceHasResponseTimeAndReplicas)
                .forEach(service -> resultBuilder.queueUpdateEntityAlone(
                        service, this::updateServiceResponseTime));
        return resultBuilder.build();
    }

    private boolean serviceHasResponseTimeAndReplicas(@Nonnull final TopologyEntity service) {
        return service.getProviders().size() > 0 && service.soldCommoditiesByType()
                .getOrDefault(CommodityType.RESPONSE_TIME_VALUE, Collections.emptyList())
                .stream()
                .anyMatch(commSold -> commSold.hasUsed() && commSold.getUsed() > 0);
    }

    private void updateServiceResponseTime(@Nonnull final TopologyEntity service) {
        final TopologyEntityImpl serviceBuilder = service.getTopologyEntityImpl();
        final long replicas = serviceBuilder.getCommoditiesBoughtFromProvidersList().stream()
                .filter(commsBought -> commsBought.getProviderEntityType() == EntityType.APPLICATION_COMPONENT_VALUE)
                .filter(commsBought -> commsBought.getCommodityBoughtList().stream()
                        .anyMatch(boughtComm -> boughtComm.getCommodityType().getType() == CommodityType.RESPONSE_TIME_VALUE
                                && boughtComm.hasUsed() && boughtComm.getUsed() > 0))
                .count();
        if (replicas == 0) {
            logger.warn("Service {} sells non-zero response time but does not buy response time "
                    + "from any of its providers.", service.getOid());
            return;
        }
        serviceBuilder.getCommoditySoldListImplList().stream()
                .filter(soldComm -> soldComm.getCommodityType().getType() == CommodityType.RESPONSE_TIME_VALUE)
                .forEach(responseTimeComm -> {
                    logger.debug("Setting ResponseTime used for service {} by averaging total value {} "
                            + "across {} replicas.", service.getOid(), responseTimeComm.getUsed(), replicas);
                    responseTimeComm.setUsed(responseTimeComm.getUsed() / replicas);
                });
    }
}
