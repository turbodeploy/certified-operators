package com.vmturbo.topology.processor.reservation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.grpc.StatusRuntimeException;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ReservationDTO.GetAllReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate.ReservationInstance.PlacementInfo;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateFutureAndExpiredReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateFutureAndExpiredReservationsResponse;
import com.vmturbo.common.protobuf.plan.ReservationDTO.UpdateReservationsRequest;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ReservationOrigin;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.common.utils.ReservationProtoUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.reservation.ReservationValidator.ValidationErrors;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;

/**
 * Responsible for convert Reservations to TopologyEntity and add them into live topology. And also
 * check if there are any Reservation should become active (start day is today or before and
 * status is FUTURE) and change its status and send request to update Reservation table.
 * <p>
 * Reservation entity will only buy provision commodity, for example, Virtual Machine reservation
 * entity only buy CpuProvision, MemProvision, StorageProvision commodity. And also the providers of
 * reservation entity will modify their commodity sold utilization based on reservation entity provision
 * value.
 */
public class ReservationManager {
    private static final Logger logger = LogManager.getLogger();

    static final String RESERVATION_KEY = "ReservationKey";

    /**
     * Track reservation counts every broadcast.
     */
    private static final DataMetricGauge RESERVATION_STATUS_GAUGE = DataMetricGauge.builder()
            .withName(StringConstants.METRICS_TURBO_PREFIX + "current_reservations")
            .withHelp("Reservation per status each broadcast")
            .withLabelNames("status")
            .build()
            .register();

    private final ReservationServiceBlockingStub reservationService;

    private final ReservationValidator reservationValidator;

    ReservationManager(@Nonnull final ReservationServiceBlockingStub reservationService,
                              @Nonnull final ReservationValidator reservationValidator) {
        this.reservationService = Objects.requireNonNull(reservationService);
        this.reservationValidator = Objects.requireNonNull(reservationValidator);
    }

    /**
     * Convert all active and potential active Reservations to TopologyEntity and add them into live
     * topology.
     *
     * @param topology a Map contains live discovered topologyEntity.
     * @param topologyInfo Information about the topology under construction.
     * @return The number of reservation entities
     */
    public int applyReservation(@Nonnull final Map<Long, TopologyEntity.Builder> topology,
                                TopologyInfo topologyInfo) {
        TopologyType topologyType = topologyInfo.getTopologyType();
        PlanProjectType planType  = topologyInfo.getPlanInfo().getPlanProjectType();
        // Retrieve all active reservations. The ones which has already been
        // taken care of by a previous reservation plan.
        final Map<Long, Reservation> reservedReservations = new HashMap<>();

        RESERVATION_STATUS_GAUGE.getLabeledMetrics().forEach((key, val) -> {
            val.setData(0.0);
        });
        reservationService.getAllReservations(GetAllReservationsRequest.getDefaultInstance())
                .forEachRemaining(reservation -> {
                    if (topologyType == TopologyType.REALTIME
                            || planType == PlanProjectType.CLUSTER_HEADROOM) {
                        if (reservation.getStatus() == ReservationStatus.RESERVED) {
                            reservedReservations.put(reservation.getId(), reservation);
                        }
                    }
                    RESERVATION_STATUS_GAUGE.labels(reservation.getStatus().toString()).increment();
                });

        // update the future, expired and invalid reservations.
        if (topologyType == TopologyType.REALTIME) {
            final UpdateFutureAndExpiredReservationsResponse resp = reservationService.updateFutureAndExpiredReservations(
                UpdateFutureAndExpiredReservationsRequest.newBuilder()
                .build());
            if (resp.getActivatedReservations() > 0 || resp.getExpiredReservationsRemoved() > 0) {
                logger.info("Activated {} reservations. Expired {} reservations.",
                    resp.getActivatedReservations(), resp.getExpiredReservationsRemoved());
            }
        }

        // Validate BEFORE adding entities to the topology.
        final ValidationErrors validationErrors = reservationValidator.validateReservations(
            // Concatenate the reserved and active reservations, because validation requires
            // RPC calls.
            reservedReservations.values().stream(),
            topology::containsKey);
        if (!validationErrors.isEmpty()) {
            logger.error("Invalid reservations detected! These reservations will not be in the topology: {}",
                validationErrors);
            final UpdateReservationsRequest.Builder updateReqBldr = UpdateReservationsRequest.newBuilder();
            // Remove the invalid reservations from the "reserved" and "active" sets, and
            // mark them as invalid in the reservation service.
            validationErrors.getErrorsByReservation().forEach((reservationId, errors) -> {
                final Reservation existing =
                    reservedReservations.remove(reservationId);
                if (existing != null) {
                    updateReqBldr.addReservation(
                        // We mark them as invalid here.
                        // On the next broadcast, in the ReservationManager, we will trigger a call to
                        // mark invalid reservations as unfulfilled, which will trigger another
                        // reservation plan. We don't mark them as UNFULFILLED here because that will trigger
                        // an immediate reservation plan, and if the reservation continues to be invalid
                        // we will just continue running the reservation plan ad infinitum!
                            ReservationProtoUtil.invalidateReservation(existing));
                }
            });
            final UpdateReservationsRequest req = updateReqBldr.build();
            try {
                final MutableLong updatedCount = new MutableLong(0);
                // Important to drain the iterator.
                reservationService.updateReservations(req).forEachRemaining(updated -> updatedCount.increment());
                logger.info("Marked {}/{} failed reservations as invalid.", updatedCount, req.getReservationCount());
            } catch (StatusRuntimeException e) {
                // Maybe the plan orchestrator crashed, or there was a DB error.
                // We don't want to fail the pipeline here. The reservations will continue to be
                // considered "active", and we will try to add them again on the next cycle.
                // We won't actually add the entities that represent the reservations, and that's
                // the important part.
                logger.error("Failed to mark {} failed reservations as invalid due to RPC error: {}",
                    req.getReservationCount(), e.getMessage());
            }
        }

        // create entities for the new ones and update utilization of the host/storages for the
        // old ones.
        final List<TopologyEntity.Builder> reservedReservationTopologyEntities = new ArrayList<>();

        handleReservedReservation(reservedReservations.values(), reservedReservationTopologyEntities, topology);

        //TODO: (OM-29676) update existing topologyEntity utilization based on placement information
        // and only buy provision commodities.

        int numAdded = 0;
        if (topologyType == TopologyType.REALTIME || planType == PlanProjectType.CLUSTER_HEADROOM) {
            // For real time and headroom plan we send all the reserved and placement_failed reservations.
            for (TopologyEntity.Builder reservedEntity : reservedReservationTopologyEntities) {
                final TopologyEntity.Builder existingEntity =
                        topology.putIfAbsent(reservedEntity.getOid(), reservedEntity);
                if (existingEntity == null) {
                    numAdded++;
                }
            }
        }
        return numAdded;
    }


    /**
     * Convert reserved Reservations to topology entities.
     *
     * @param reservedReservations a set of {@link Reservation}.
     * @param reservationTopologyEntities a list of {@link TopologyEntity.Builder}.
     * @param topology a Map contains live discovered topologyEntity.
     */
    private void handleReservedReservation(
            @Nonnull final Collection<Reservation> reservedReservations,
            @Nonnull final List<TopologyEntity.Builder> reservationTopologyEntities,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        for (Reservation reservation : reservedReservations) {
            final List<ReservationTemplate> reservationTemplates =
                    reservation.getReservationTemplateCollection()
                    .getReservationTemplateList();
            // give each reserved entity a ReservationOrigin
            Origin reservationOrigin = Origin.newBuilder()
                    .setReservationOrigin(ReservationOrigin.newBuilder()
                        .setReservationId(reservation.getId()))
                    .build();
            for (ReservationTemplate reservationTemplate : reservationTemplates) {
                List<TopologyEntity.Builder> entityBuilders = createTopologyEntitiesFromInstances(reservationTemplate, topology);
                entityBuilders.forEach(builder -> {
                    builder.getEntityBuilder().setOrigin(reservationOrigin);
                    reservationTopologyEntities.add(builder);
                });
            }
        }
    }

    /**
     * Create a list of topology entities based on input {@link ReservationTemplate}.
     *
     * @param reservationTemplate {@link ReservationTemplate}.
     * @param topology            a Map contains live discovered topologyEntity.
     * @return a list of topology entities.
     */
    private List<TopologyEntity.Builder> createTopologyEntitiesFromInstances(
            @Nonnull ReservationTemplate reservationTemplate,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        // normally it should not happen, if it happens, will ignore this reservation template.
        if (reservationTemplate.getReservationInstanceCount() != reservationTemplate.getCount()) {
            logger.error("Count mismatch between instance count: " +
                    reservationTemplate.getReservationInstanceCount() + " with template count: " +
                    reservationTemplate.getCount());
            return Collections.emptyList();
        }

        final List<TopologyEntity.Builder> topologyEntityBuilder = new ArrayList<>();

        for (ReservationInstance reservationInstance : reservationTemplate.getReservationInstanceList()) {
            TopologyEntityDTO.Builder topologyEntityDTO = TopologyEntityDTO.newBuilder();
            topologyEntityDTO.setOid(reservationInstance.getEntityId())
                    .setEntityState(EntityState.POWERED_ON).setAnalysisSettings(AnalysisSettings
                    .newBuilder().setIsAvailableAsProvider(true).setShopTogether(true)
                    .setControllable(false).setSuspendable(false))
                    .setDisplayName(reservationInstance.getName())
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
            for (PlacementInfo placementInfo : reservationInstance.getPlacementInfoList()) {
                CommoditiesBoughtFromProvider.Builder commoditiesBoughtFromProvider =
                        CommoditiesBoughtFromProvider.newBuilder();
                commoditiesBoughtFromProvider.addAllCommodityBought(placementInfo.getCommodityBoughtList());
                commoditiesBoughtFromProvider.setProviderEntityType(placementInfo.getProviderType());
                if (placementInfo.hasProviderId()) {
                    commoditiesBoughtFromProvider.setProviderId(placementInfo.getProviderId());
                }
                topologyEntityDTO.addCommoditiesBoughtFromProviders(commoditiesBoughtFromProvider);
            }
            topologyEntityBuilder.add(TopologyEntity.newBuilder(topologyEntityDTO));
        }
        return topologyEntityBuilder;

    }
}