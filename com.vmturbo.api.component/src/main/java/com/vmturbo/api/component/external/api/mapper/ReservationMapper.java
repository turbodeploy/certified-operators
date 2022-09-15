package com.vmturbo.api.component.external.api.mapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.util.StopWatch;

import com.vmturbo.api.ReservationNotificationDTO;
import com.vmturbo.api.ReservationNotificationDTO.ReservationNotification;
import com.vmturbo.api.ReservationNotificationDTO.ReservationStatusNotification;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservation.DemandEntityInfoDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiDTO;
import com.vmturbo.api.dto.reservation.DemandReservationApiInputDTO;
import com.vmturbo.api.dto.reservation.DemandReservationParametersDTO;
import com.vmturbo.api.dto.reservation.PlacementInfoDTO;
import com.vmturbo.api.dto.reservation.PlacementParametersDTO;
import com.vmturbo.api.dto.reservation.ReservationConstraintApiDTO;
import com.vmturbo.api.dto.reservation.ReservationFailureInfoDTO;
import com.vmturbo.api.dto.reservation.ReservationInvalidInfoApiDTO;
import com.vmturbo.api.dto.reservation.ReservationInvalidInfoApiDTO.InvalidReason;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.dto.template.ResourceApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.enums.ReservationGrouping;
import com.vmturbo.api.enums.ReservationMode;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetPartialGroupingInfoRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.PartialGroupingInfo.Type;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.market.InitialPlacement.InvalidInfo;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ConstraintInfoCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChange;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationChanges;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationStatus;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ReservationTemplateCollection.ReservationTemplate;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.GetTemplateRequest;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason.FailedResources;
import com.vmturbo.plan.orchestrator.api.NoSuchValueException;
import com.vmturbo.plan.orchestrator.api.ReservationFieldsConverter;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * A mapper class for convert Reservation related objects between Api DTO with XL DTO.
 */
public class ReservationMapper {
    private final String moreThanOneReservationFound = "Error: Found more than one value when looking for single reservation";
    private final String numberOfReservationsToMapMisMatch = "Error: number of reservations {} != reservationToPlacementInfosMap {}";
    private final String clusterIdAmountMisMatch = "clusterMap.values().size(): {} != clusterIds.size(): {} not equal";
    private final boolean enableReservationEnhancements;

    private static final EnumMap<ReservationMode, Set<ReservationGrouping>> VALID_RESERVATION_MODE_GROUPINGS
        = new EnumMap<>(ImmutableMap.<ReservationMode, Set<ReservationGrouping>>builder()
            .put(ReservationMode.NO_GROUPING, Sets.newHashSet(ReservationGrouping.NONE))
            .put(ReservationMode.AFFINITY, Sets.newHashSet(ReservationGrouping.CLUSTER))
            .build());

    private static final Map<Integer, ReservationConstraintInfo.Type> ENTITY_TYPE_TO_CONSTRAINT_TYPE =
        ImmutableMap.<Integer, ReservationConstraintInfo.Type>builder()
            .put(ApiEntityType.DATACENTER.typeNumber(), ReservationConstraintInfo.Type.DATA_CENTER)
            .put(ApiEntityType.VIRTUAL_DATACENTER.typeNumber(), ReservationConstraintInfo.Type.VIRTUAL_DATA_CENTER)
            .put(ApiEntityType.NETWORK.typeNumber(), ReservationConstraintInfo.Type.NETWORK)
            .build();

    private static final Map<Integer, Pair<String, String>> COMMODITY_TYPE_NAME_UNIT_MAP =
            ImmutableMap.<Integer, Pair<String, String>>builder()
                    .put(CommodityType.CPU_PROVISIONED_VALUE,
                            Pair.of(CommodityTypeMapping.getMixedCaseFromCommodityType(CommodityType.CPU_PROVISIONED),
                                CommodityTypeMapping.getUnitForCommodityType(CommodityType.CPU_PROVISIONED)))
                    .put(CommodityType.MEM_PROVISIONED_VALUE,
                            Pair.of(CommodityTypeMapping.getMixedCaseFromCommodityType(CommodityType.MEM_PROVISIONED),
                                CommodityTypeMapping.getUnitForCommodityType(CommodityType.MEM_PROVISIONED)))
                    .put(CommodityType.CPU_VALUE,
                            Pair.of(CommodityTypeMapping.getMixedCaseFromCommodityType(CommodityType.CPU),
                                CommodityTypeMapping.getUnitForCommodityType(CommodityType.CPU)))
                    .put(CommodityType.MEM_VALUE,
                            Pair.of(CommodityTypeMapping.getMixedCaseFromCommodityType(CommodityType.MEM),
                                CommodityTypeMapping.getUnitForCommodityType(CommodityType.MEM)))
                    .put(CommodityType.IO_THROUGHPUT_VALUE,
                            Pair.of(CommodityTypeMapping.getMixedCaseFromCommodityType(CommodityType.IO_THROUGHPUT),
                                CommodityTypeMapping.getUnitForCommodityType(CommodityType.IO_THROUGHPUT)))
                    .put(CommodityType.NET_THROUGHPUT_VALUE,
                            Pair.of(CommodityTypeMapping.getMixedCaseFromCommodityType(CommodityType.NET_THROUGHPUT),
                                CommodityTypeMapping.getUnitForCommodityType(CommodityType.NET_THROUGHPUT)))
                    .put(CommodityType.STORAGE_AMOUNT_VALUE,
                            Pair.of(CommodityTypeMapping.getMixedCaseFromCommodityType(CommodityType.STORAGE_AMOUNT),
                                CommodityTypeMapping.getUnitForCommodityType(CommodityType.STORAGE_AMOUNT)))
                    .put(CommodityType.STORAGE_ACCESS_VALUE,
                            Pair.of(CommodityTypeMapping.getMixedCaseFromCommodityType(CommodityType.STORAGE_ACCESS),
                                CommodityTypeMapping.getUnitForCommodityType(CommodityType.STORAGE_ACCESS)))
                    .put(CommodityType.STORAGE_PROVISIONED_VALUE,
                            Pair.of(CommodityTypeMapping.getMixedCaseFromCommodityType(CommodityType.STORAGE_PROVISIONED),
                                CommodityTypeMapping.getUnitForCommodityType(CommodityType.STORAGE_PROVISIONED)))
                    .put(CommodityType.SEGMENTATION_VALUE,
                            Pair.of("PlacementPolicy",
                                CommodityTypeMapping.getUnitForCommodityType(CommodityType.SEGMENTATION)))
                    .build();

    private final Logger logger = LogManager.getLogger();

    private final RepositoryApi repositoryApi;

    private final GroupServiceBlockingStub groupServiceBlockingStub;

    private final TemplateServiceBlockingStub templateService;

    private final PolicyServiceBlockingStub policyService;

    ReservationMapper(@Nonnull final RepositoryApi repositoryApi,
                      @Nonnull final TemplateServiceBlockingStub templateService,
                      @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub,
                      @Nonnull final PolicyServiceBlockingStub policyService,
                      final boolean enableReservationEnhancements) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.templateService = Objects.requireNonNull(templateService);
        this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
        this.policyService = Objects.requireNonNull(policyService);
        this.enableReservationEnhancements = enableReservationEnhancements;
    }

    /**
     * Convert {@link DemandReservationApiInputDTO} to {@link Reservation}, it will always set status
     * to FUTURE, during the next broadcast cycle, status will be changed to RESERVED if its start
     * day comes.
     *
     * @param demandApiInputDTO {@link DemandReservationApiInputDTO}
     * @return a {@link Reservation}
     * @throws InvalidOperationException if input parameter are not correct.
     * @throws NoSuchValueException if invalid reservation value is specified.
     */
    public Reservation convertToReservation(
            @Nonnull final DemandReservationApiInputDTO demandApiInputDTO)
            throws InvalidOperationException, NoSuchValueException {
        final Reservation.Builder reservationBuilder = Reservation.newBuilder();
        reservationBuilder.setName(demandApiInputDTO.getDemandName());
        convertReservationDateStatus(demandApiInputDTO.getReserveDateTime(),
                demandApiInputDTO.getExpireDateTime(), reservationBuilder);
        final List<DemandReservationParametersDTO> placementParameters =
                demandApiInputDTO.getParameters();
        final List<ReservationTemplate> reservationTemplates = placementParameters
                .stream()
                .map(DemandReservationParametersDTO::getPlacementParameters)
                .map(this::convertToReservationTemplate)
                .collect(Collectors.toList());
        reservationBuilder.setReservationTemplateCollection(ReservationTemplateCollection
                .newBuilder()
                .addAllReservationTemplate(reservationTemplates)
                .build());
        final Set<Long> constraintIds = placementParameters
                .stream()
                .map(DemandReservationParametersDTO::getPlacementParameters)
                .map(PlacementParametersDTO::getConstraintIDs)
                .filter(Objects::nonNull)
                .flatMap(Set::stream)
                .map(Long::valueOf)
                .collect(Collectors.toSet());
        final List<ReservationConstraintInfo> constraintInfos = constraintIds
                .stream()
                .map(constraintId -> ReservationConstraintInfo
                        .newBuilder()
                        .setConstraintId(constraintId)
                        .build())
                .collect(Collectors.toList());
        ConstraintInfoCollection.Builder constraintsCollectionBuilder
            = ConstraintInfoCollection.newBuilder().addAllReservationConstraintInfo(constraintInfos);
        convertScopeFromApi(demandApiInputDTO, constraintsCollectionBuilder);
        reservationBuilder.setConstraintInfoCollection(constraintsCollectionBuilder.build());
        convertReservationModeGroupingFromApi(demandApiInputDTO, reservationBuilder);
        return reservationBuilder.build();
    }

    private void convertScopeFromApi(@NotNull DemandReservationApiInputDTO demandApiInputDTO,
            ConstraintInfoCollection.Builder constraintsCollectionBuilder) throws InvalidOperationException {
        // If scope is not set there is nothing to do
        if (Objects.isNull(demandApiInputDTO.getScope())) {
            return;
        }

        // An empty scope is equivalent to no scope being set, so there is nothing to do
        if (demandApiInputDTO.getScope().isEmpty()) {
            return;
        }

        final Pattern regex = Pattern.compile("[0-9]+");
        Optional<String> invalidScopeUuid = demandApiInputDTO
                .getScope()
                .stream()
                .filter(scopeId -> !regex.matcher(scopeId).matches())
                .findAny();
        if (invalidScopeUuid.isPresent()) {
            throw new InvalidOperationException(invalidScopeUuid.get() + " is not a valid uuid");
        }

        List<Long> scope = demandApiInputDTO
                .getScope()
                .stream()
                .map(Long::parseLong)
                .collect(Collectors.toList());

        GetGroupsRequest request = GetGroupsRequest
                .newBuilder()
                .setGroupFilter(GroupFilter
                        .newBuilder()
                        .addAllId(scope)
                        .addAllDirectMemberTypes(Arrays.asList(MemberType
                                .newBuilder()
                                .setEntity(
                                        EntityType.PHYSICAL_MACHINE_VALUE) // group of physical machines
                                .build(), MemberType
                                .newBuilder()
                                .setGroup(GroupType.COMPUTE_HOST_CLUSTER) // or group of clusters
                                .build()))
                        .build())
                .build();

        int numberOfGroupsThatAreAllowed = groupServiceBlockingStub.countGroups(request).getCount();

        if (numberOfGroupsThatAreAllowed != scope.size()) {
            throw new InvalidOperationException("Provided scopes are not valid");
        }
        constraintsCollectionBuilder.addAllScopeIds(scope);
    }

    /**
     * Convert {@link Reservation} to {@link DemandReservationApiDTO}. Right now, it only pick the
     * first ReservationTemplate, because currently Reservation only support one type of template.
     *
     * @param reservation {@link Reservation}.
     * @param placementInfosMap contains a mapping between reservation ids and list of PlacementInfos
     * @param serviceEntityMap contains a mapping between oids and {@link ServiceEntityApiDTO}
     * @param clusterMap contains a mapping between clusterIds and basic grouping info
     * @return {@link DemandReservationApiDTO}.
     * @throws UnknownObjectException if there are any unknown objects.
     * @throws NoSuchValueException if invalid reservation value is specified.
     */
    public DemandReservationApiDTO convertReservationToApiDTO(
            @Nonnull final Reservation reservation, Map<Long, ServiceEntityApiDTO> serviceEntityMap,
            Map<Long, BaseApiDTO> clusterMap, Map<Long, List<PlacementInfo>> placementInfosMap)
            throws UnknownObjectException, NoSuchValueException {
        final DemandReservationApiDTO reservationApiDTO = new DemandReservationApiDTO();
        reservationApiDTO.setUuid(String.valueOf(reservation.getId()));
        reservationApiDTO.setDisplayName(reservation.getName());
        reservationApiDTO.setReserveDateTime(convertProtoDateToString(reservation.getStartDate()));
        reservationApiDTO.setExpireDateTime(convertProtoDateToString(reservation.getExpirationDate()));
        reservationApiDTO.setStatus(reservation.getStatus().toString());
        List<ReservationConstraintApiDTO> constraintInfos = new ArrayList<>();
        if (reservation.hasConstraintInfoCollection()
                && reservation.getConstraintInfoCollection().getReservationConstraintInfoList() != null) {
            for (ScenarioOuterClass.ReservationConstraintInfo reservationConstraintInfo
                    : reservation.getConstraintInfoCollection().getReservationConstraintInfoList()) {
                constraintInfos.add(
                        new ReservationConstraintApiDTO(String.valueOf(reservationConstraintInfo
                                .getConstraintId()),
                                reservationConstraintInfo.getType().name()));
            }
        }
        reservationApiDTO.setConstraintInfos(constraintInfos);
        final List<ReservationTemplate> reservationTemplates =
                reservation.getReservationTemplateCollection().getReservationTemplateList();
        // Because right now, DemandReservationApiDTO support only one type of template for each
        // reservation, it is ok to only pick the first one.
        Optional<ReservationTemplate> reservationTemplate = reservationTemplates.stream().findFirst();
        logCurrentStateOfDemandEntityDTOResources(reservation, placementInfosMap,
                reservationTemplate, logger.isDebugEnabled());
        if (reservationTemplate.isPresent()) {
            convertToDemandEntityDTO(reservationTemplate.get(), reservationApiDTO,
                    placementInfosMap.get(reservation.getId()), serviceEntityMap, clusterMap);
        }
        reservationApiDTO.setReservationDeployed(reservation.getDeployed());
        if (enableReservationEnhancements) {
            if (reservation.hasReservationMode()) {
                reservationApiDTO.setMode(
                        ReservationFieldsConverter.modeToApi(reservation.getReservationMode()));
            }
            if (reservation.hasReservationGrouping()) {
                reservationApiDTO.setGrouping(ReservationFieldsConverter.groupingToApi(
                        reservation.getReservationGrouping()));
            }
            if (reservation.hasConstraintInfoCollection()
                    && !reservation.getConstraintInfoCollection().getScopeIdsList().isEmpty()) {
                reservationApiDTO.setScope(reservation.getConstraintInfoCollection()
                    .getScopeIdsList().stream()
                        .map(Object::toString)
                        .collect(Collectors.toList()));
            }
        }
        return reservationApiDTO;
    }

    /**
     * Create a {@link ReservationNotification} from a {@link ReservationChanges}.
     *
     * @param reservationChanges the the current batch of reservation changes.
     * @return a reservation notification that conforms to what the API layer will broadcast.
     */
    public static ReservationNotification notificationFromReservationChanges(@Nonnull final ReservationChanges reservationChanges) {
        final ReservationStatusNotification.Builder notificationBuilder = ReservationStatusNotification.newBuilder();
        for (ReservationChange resChange : reservationChanges.getReservationChangeList()) {
            notificationBuilder.addReservationStatus(
                ReservationNotificationDTO.ReservationStatus.newBuilder()
                    .setId(Long.toString(resChange.getId()))
                    .setStatus(resChange.getStatus().toString())
                    .build());
        }
        return ReservationNotification.newBuilder()
            .setStatusNotification(notificationBuilder.build())
            .build();
    }

    /**
     * Converts a single raw Reservations into a DemandReservationApiDTOs, by properly populating
     * auxiliary info, and calling generateReservationList.
     *
     * @param reservation {@link Reservation}.
     * @throws UnknownObjectException if there are any unknown objects.
     * @throws ConversionException if error faced converting objects to
     * API DTOs in getServiceEntityMap.
     * @throws InterruptedException if current thread has been interrupted.
     * @throws NoSuchValueException if invalid reservation value is specified.
     */
    public DemandReservationApiDTO generateReservationApiDto(Reservation reservation)
            throws ConversionException, InterruptedException, NoSuchValueException,
            UnknownObjectException {
        List<DemandReservationApiDTO> demandReservationApiDtos =
                generateReservationList(Collections.singletonList(reservation).iterator());

        if (demandReservationApiDtos.size() != 1) {
            logger.error(moreThanOneReservationFound);
            throw new ConversionException(moreThanOneReservationFound);
        }
        return demandReservationApiDtos.get(0);
    }

    /**
     * Converts raw Reservations into a list of DemandReservationApiDTOs, by properly populating
     * auxiliary info.
     *
     * @param reservationIterator Iterable of {@link Reservation}.
     * @throws UnknownObjectException if there are any unknown objects.
     * @throws ConversionException if error faced converting objects to
     * API DTOs in getServiceEntityMap.
     * @throws InterruptedException if current thread has been interrupted.
     * @throws NoSuchValueException if invalid reservation value is specified.
     **/
    public List<DemandReservationApiDTO> generateReservationList(Iterator<Reservation> reservationIterator)
            throws ConversionException, InterruptedException, NoSuchValueException,
            UnknownObjectException {
        final StopWatch stopWatch = new StopWatch("generateReservationList");

        // draining all the contents of Iterator before using it further in the process
        List<Reservation> reservations = new ArrayList<>();
        reservationIterator.forEachRemaining(reservations::add);

        // creating a map between reservation id and the List<PlacementInfo> inside it
        stopWatch.start("reservationToPlacementInfosMap");
        Map<Long, List<PlacementInfo>> reservationToPlacementInfosMap =
                generateReservationToPlacementInfoMap(reservations);
        stopWatch.stop();

        if (reservations.size() != reservationToPlacementInfosMap.size()) {
            logger.error(numberOfReservationsToMapMisMatch, reservations.size(), reservationToPlacementInfosMap.size());
        }

        // collecting all PlacementInfos in the map into a single list to send to
        // getServiceEntityMap & clusterMap
        List<PlacementInfo> allPlacementInfos = reservationToPlacementInfosMap.values().stream().flatMap(Collection::stream).collect(
                Collectors.toList());

        stopWatch.start("getServiceEntityMap");
        final Map<Long, ServiceEntityApiDTO> serviceEntityMap = getServiceEntityMap(allPlacementInfos);
        stopWatch.stop();

        stopWatch.start("getClusterMap");
        final Map<Long, BaseApiDTO> clusterMap = getClusterMap(allPlacementInfos);
        stopWatch.stop();

        final List<DemandReservationApiDTO> result = new ArrayList<>();

        // iterating over all the Reservations, and supplying the convertReservationToApiDTO method
        // with all the parameters it requires to convert reservation to DemandReservationApiDTO
        stopWatch.start("convertReservationToApiDTO");
        for (Reservation reservation : reservations) {
            result.add(convertReservationToApiDTO(reservation, serviceEntityMap, clusterMap,
                    reservationToPlacementInfosMap));
        }
        stopWatch.stop();

        logger.debug(stopWatch::prettyPrint);

        return result;
    }

    /**
     * Generates a Map between reservation id and its associated PlacementInfos.
     * Used in the conversion process between {@link Reservation}
     * and {@link DemandReservationApiDTO}.
     *
     * @param reservations Iterable of Reservation
     * @return map of id and list of PlacementInfos
     */
    public static Map<Long, List<PlacementInfo>> generateReservationToPlacementInfoMap(List<Reservation> reservations) {
        Map<Long, List<PlacementInfo>> map = new HashMap<>();
        for (Reservation reservation : reservations) {
            final List<ReservationTemplate> reservationTemplates =
                    reservation.getReservationTemplateCollection().getReservationTemplateList();
            Optional<ReservationTemplate> reservationTemplate = reservationTemplates.stream().findFirst();
            if (reservationTemplate.isPresent()) {
                map.put(reservation.getId(), createPlacementInfo(reservationTemplate.get()));
            }
        }
        return map;
    }

    /**
     * Generates a list of placementInfos based on a Reservation's ReservationTemplate.
     *
     * @param reservationTemplate of ReservationTemplate
     * @return  PlacementInfo list
     */
    private static List<PlacementInfo> createPlacementInfo(@Nonnull final ReservationTemplate reservationTemplate) {
        return reservationTemplate.getReservationInstanceList().stream()
                .map(reservationInstance -> {
                    final List<ProviderInfo> providerInfos = reservationInstance.getPlacementInfoList().stream()
                            .map(a -> new ProviderInfo(
                                    a.hasProviderId()
                                            ? Optional.of(a.getProviderId()) : Optional.empty(),
                                    a.getCommodityBoughtList(),
                                    a.hasClusterId()
                                            ? Optional.of(a.getClusterId()) : Optional.empty()))
                            .collect(Collectors.toList());
                    final InvalidInfo invalidInfo = reservationInstance.hasInvalidInfo()
                            ? reservationInstance.getInvalidInfo() : null;
                    return new PlacementInfo(reservationInstance.getEntityId(),
                            ImmutableList.copyOf(providerInfos), reservationInstance.getUnplacedReasonList(),
                            Optional.ofNullable(invalidInfo));
                }).collect(Collectors.toList());
    }

    /**
     * Convert reservation's placement information to {@link DemandEntityInfoDTO}.
     * Also generates and sets demandEntityInfoDTOS.
     * @param reservationTemplate {@link ReservationTemplate} contains placement information by template.
     * @param reservationApiDTO {@link DemandReservationApiDTO}
     * @param placementInfos contains a list of placementinfos for use with generateDemandEntityInfoDTO
     * @param serviceEntityMap contains a mapping between oids and {@link ServiceEntityApiDTO}
     * @param clusterMap contains a mapping between clusterIds and basic grouping info
     * @throws UnknownObjectException if there are any unknown objects.
     */
    private void convertToDemandEntityDTO(@Nonnull final ReservationTemplate reservationTemplate,
                                          @Nonnull final DemandReservationApiDTO reservationApiDTO,
            @Nonnull final List<PlacementInfo> placementInfos, @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityMap,
            @Nonnull final Map<Long, BaseApiDTO> clusterMap)
            throws UnknownObjectException {
        reservationApiDTO.setCount(Math.toIntExact(reservationTemplate.getCount()));
        //TODO: need to make sure templates are always available, if templates are deleted, need to
        // mark Reservation not available or also delete related reservations.
        try {
            final Template template = reservationTemplate.hasTemplate()
                    ? reservationTemplate.getTemplate()
                    : templateService.getTemplate(GetTemplateRequest.newBuilder()
                            .setTemplateId(reservationTemplate.getTemplateId())
                            .build()).getTemplate();

            final List<DemandEntityInfoDTO> demandEntityInfoDTOS = new ArrayList<>();
            for (PlacementInfo placementInfo : placementInfos) {
                demandEntityInfoDTOS.add(
                        generateDemandEntityInfoDTO(placementInfo, template, serviceEntityMap, clusterMap));
            }
            reservationApiDTO.setDemandEntities(demandEntityInfoDTOS);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                logger.error("Error: " + e.getMessage());
                return;
            } else {
                throw e;
            }
        }
    }

    /**
     * Validate the input start day and expire date, if there is any wrong input date, it will
     * throw
     * InvalidOperationException. And also it always set Reservation status to INITIAL, during
     * create
     * Reservation, we will update its status if it is start day comes.
     *
     * @param reserveDateStr start date of reservation, and date format is:
     *         yyyy-MM-ddT00:00:00Z.
     * @param expireDateStr expire date of reservation, and date format is:
     *         yyyy-MM-ddT00:00:00Z.
     * @param reservationBuilder builder of Reservation.
     * @throws InvalidOperationException if input start date and expire date are illegal.
     */
    private void convertReservationDateStatus(@Nonnull final String reserveDateStr,
            @Nonnull final String expireDateStr,
            @Nonnull final Reservation.Builder reservationBuilder)
            throws InvalidOperationException {
        if (reserveDateStr == null) {
            throw new InvalidOperationException("Reservation date is missing.");
        }
        if (expireDateStr == null) {
            throw new InvalidOperationException("Expiry date is missing.");
        }
        if (reservationBuilder == null) {
            throw new InvalidOperationException("reservationBuilder is missing.");
        }

        // Right now, UI set reservation start date to empty string when start date is current date,
        // after UI fix this issue, we can remove empty string covert here.
        final long reserveDate = reserveDateStr.isEmpty() ? Instant.now().toEpochMilli()
                : DateTimeUtil.parseIso8601TimeAndAdjustTimezone(reserveDateStr, null);
        final long expireDate = DateTimeUtil.parseIso8601TimeAndAdjustTimezone(expireDateStr, null);
        if (reserveDate > expireDate) {
            throw new InvalidOperationException(
                    "Reservation expire date should be after start date.");
        }
        reservationBuilder.setStartDate(reserveDate);
        reservationBuilder.setExpirationDate(expireDate);
        final long today = Instant.now().toEpochMilli();
        if (today > expireDate) {
            throw new InvalidOperationException(
                    "Reservation expire date should be after current date.");
        }
        reservationBuilder.setStatus(ReservationStatus.INITIAL);
    }

    private ReservationTemplate convertToReservationTemplate(
            @Nonnull final PlacementParametersDTO placementParameter) {
        return ReservationTemplate
                .newBuilder()
                .setCount(placementParameter.getCount())
                .setTemplateId(Long.parseLong(placementParameter.getTemplateID()))
                .build();
    }

    /**
     * Validate and set reservation mode and grouping.
     *
     * @param demandApiInputDTO the reservation from api.
     * @param reservationBuilder the reservation to be built.
     * @throws NoSuchValueException Unsupported mode or grouping specification.
     * @throws InvalidOperationException Invalid combination.
     */
    private void convertReservationModeGroupingFromApi(
        @Nonnull final DemandReservationApiInputDTO demandApiInputDTO,
        @Nonnull final Reservation.Builder reservationBuilder)
        throws NoSuchValueException, InvalidOperationException {
        final String invalidModeGroupingMessage = "Invalid Mode-Grouping combination! MODE: "
            + demandApiInputDTO.getMode() + " - " + "GROUPING: " + demandApiInputDTO.getGrouping();
        if (demandApiInputDTO.getMode() == null && demandApiInputDTO.getGrouping() != null) {
            throw new InvalidOperationException(invalidModeGroupingMessage);
        }
        if (demandApiInputDTO.getMode() == ReservationMode.NO_GROUPING
            && (demandApiInputDTO.getGrouping() != ReservationGrouping.NONE
                || demandApiInputDTO.getGrouping() == null)) {
            throw new InvalidOperationException(invalidModeGroupingMessage);
        }
        if (!(VALID_RESERVATION_MODE_GROUPINGS.containsKey(demandApiInputDTO.getMode())
            && VALID_RESERVATION_MODE_GROUPINGS.get(demandApiInputDTO.getMode())
                .contains(demandApiInputDTO.getGrouping())
            || ((demandApiInputDTO.getMode() == null || demandApiInputDTO.getMode()
                == ReservationMode.NO_GROUPING) && demandApiInputDTO.getGrouping() == null))) {
            throw new InvalidOperationException(invalidModeGroupingMessage);
        }
        if ((demandApiInputDTO.getMode() == null || demandApiInputDTO.getMode()
            == ReservationMode.NO_GROUPING) && demandApiInputDTO.getGrouping() == null) {
            reservationBuilder.setReservationMode(ReservationFieldsConverter
                    .modeFromApi(ReservationMode.NO_GROUPING));
            reservationBuilder.setReservationGrouping(ReservationFieldsConverter
                    .groupingFromApi(ReservationGrouping.NONE));
        } else {
            reservationBuilder.setReservationMode(ReservationFieldsConverter
                    .modeFromApi(demandApiInputDTO.getMode()));
            reservationBuilder.setReservationGrouping(ReservationFieldsConverter
                .groupingFromApi(demandApiInputDTO.getGrouping()));
        }
    }

    /**
     * Convert timestamp to {@link java.util.Date} string with default UTC timezone.
     *
     * @param timestamp milliseconds.
     * @return  {@link java.util.Date} string.
     */
    private String convertProtoDateToString(@Nonnull final long timestamp) {
        return DateTimeUtil.toString(timestamp);
    }

    /**
     * Log all relevant data required to properly convert reservation to DemandEntityDTO.
     *
     * @param reservation to convert.
     * @param placementInfosMap contains reservation id to its placement infos.
     * @param reservationTemplate info is used in processing.
     * @param reservationTemplate info is used in processing.
     */
    private void logCurrentStateOfDemandEntityDTOResources(@NotNull Reservation reservation,
            Map<Long, List<PlacementInfo>> placementInfosMap,
            Optional<ReservationTemplate> reservationTemplate, Boolean isLoggingEnabled) {
        if (isLoggingEnabled) {
            logger.debug("placementInfosMap.get(reservation.getId()) was {}", placementInfosMap.get(
                    reservation.getId()));
            logger.debug("reservation.getId() was {}", reservation.getId());
            logger.debug("checking for null values in generateReservationToPlacementInfoMap");
            for (Entry<Long, List<PlacementInfo>> entry : placementInfosMap.entrySet()) {
                logger.debug("Key : {} had value {}", entry.getKey(), entry.getValue());
            }
            logger.debug("reservationTemplate.isPresent() {}", reservationTemplate.isPresent());
        }
    }

    /**
     * Send request to repository to fetch entity information by entity ids.
     * This set contains provider oid if placed or closest seller oid if not placed.
     *
     * @param placementInfos contains all initial placement results which only keep ids.
     * @return a map which key is entity id, value is {@link ServiceEntityApiDTO}.
     * @throws ConversionException if error faced converting objects to API DTOs
     * @throws InterruptedException if current thread has been interrupted
     */
    private Map<Long, ServiceEntityApiDTO> getServiceEntityMap(
            @Nonnull final List<PlacementInfo> placementInfos)
            throws ConversionException, InterruptedException {
        // This set contains provider oid if placed or closest seller oid if not placed.
        Set<Long> entitiesOid = new HashSet<>();
        for (PlacementInfo placementInfo : placementInfos) {
            for (ProviderInfo providerInfo : placementInfo.getProviderInfos()) {
                if (providerInfo.getProviderId().isPresent()) {
                    entitiesOid.add(providerInfo.getProviderId().get());
                }
            }
            for (UnplacementReason reason : placementInfo.getUnpalcementReasons()) {
                entitiesOid.add(reason.getClosestSeller());
            }
        }

        final Map<Long, ServiceEntityApiDTO> serviceEntityMap =
            repositoryApi.entitiesRequest(entitiesOid)
                .getSEMap();
        return serviceEntityMap;
    }

    private DemandEntityInfoDTO generateDemandEntityInfoDTO(
            @Nonnull final PlacementInfo placementInfo,
            @Nonnull final Template template,
            @Nonnull Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap,
            final Map<Long, BaseApiDTO> clusterMap)
            throws UnknownObjectException {
        DemandEntityInfoDTO demandEntityInfoDTO = new DemandEntityInfoDTO();
        demandEntityInfoDTO.setTemplate(generateTemplateBaseApiDTO(template));
        PlacementInfoDTO placementInfoApiDTO =
                createPlacementInfoDTO(placementInfo, serviceEntityApiDTOMap, clusterMap);
        demandEntityInfoDTO.setPlacements(placementInfoApiDTO);
        return demandEntityInfoDTO;
    }

    private BaseApiDTO generateTemplateBaseApiDTO(@Nonnull final Template template) {
        BaseApiDTO templateApiDTO = new BaseApiDTO();
        templateApiDTO.setDisplayName(template.getTemplateInfo().getName());
        if (template.getTemplateInfo().hasEntityType()) {
            templateApiDTO.setClassName(ApiEntityType.fromType(
                    template.getTemplateInfo().getEntityType()).apiStr() + TemplatesUtils.PROFILE);
        } else {
            templateApiDTO.setClassName("TEMP-" + TemplatesUtils.PROFILE);
        }
        templateApiDTO.setUuid(String.valueOf(template.getId()));
        return templateApiDTO;
    }

    private PlacementInfoDTO createPlacementInfoDTO(
            @Nonnull final PlacementInfo placementInfo,
            @Nonnull final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap,
            final Map<Long, BaseApiDTO> clusterMap)
            throws UnknownObjectException {
        PlacementInfoDTO placementInfoApiDTO = new PlacementInfoDTO();
        addReservationFailureInfoDTO(placementInfo.getUnpalcementReasons(),
                placementInfoApiDTO,
                serviceEntityApiDTOMap,
                clusterMap);

        for (ProviderInfo providerInfo : placementInfo.getProviderInfos()) {
            if (providerInfo.getProviderId().isPresent()) {
                try {
                    addResourcesApiDTO(providerInfo,
                            serviceEntityApiDTOMap, placementInfoApiDTO, clusterMap);
                } catch (ProviderIdNotRecognizedException e) {
                    // If there are providerId not found, it means this reservation is unplaced.
                    logger.error("providerId not found", e);
                }
            }
        }
        if (placementInfo.getInvalidInfo().isPresent()) {
            InvalidInfo invalidInfo = placementInfo.getInvalidInfo().get();
            ReservationInvalidInfoApiDTO invalidInfoApiDTO = new ReservationInvalidInfoApiDTO();
            switch (invalidInfo.getInvalidInfoReasonCase()) {
                case MARKET_NOT_READY:
                    invalidInfoApiDTO.setInvalidReason(InvalidReason.MARKET_NOT_READY);
                    break;
                case MARKET_CONNECTIVITY_ERROR:
                    invalidInfoApiDTO.setInvalidReason(InvalidReason.MARKET_CONNECTIVITY_ERROR);
                    break;
                case INVALID_CONSTRAINTS:
                    invalidInfoApiDTO.setInvalidReason(InvalidReason.INVALID_CONSTRAINTS);
                    break;
            }
            placementInfoApiDTO.setInvalidInfo(invalidInfoApiDTO);
        }
        return placementInfoApiDTO;
    }

    /**
     * Create failure info for reservations that failed.
     *
     * @param reasons                 list of unplacement reasons.
     * @param placementInfoApiDTO    {@link PlacementInfoDTO}.
     * @param serviceEntityApiDTOMap a Map which key is oid, value is {@link ServiceEntityApiDTO}.
     * @param clusterMap a map which key is cluster id, value is {@link BaseApiDTO}.
     */
    private void addReservationFailureInfoDTO(List<UnplacementReason> reasons,
                                              PlacementInfoDTO placementInfoApiDTO,
                                              @Nonnull Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap,
                                              final Map<Long, BaseApiDTO> clusterMap) {
        for (UnplacementReason reason : reasons) {
            Optional<ServiceEntityApiDTO> serviceEntityApiDTO = Optional
                    .ofNullable(serviceEntityApiDTOMap
                            .get(reason.getClosestSeller()));
            final BaseApiDTO providerBaseApiDTO = new BaseApiDTO();
            if (serviceEntityApiDTO.isPresent()) {
                providerBaseApiDTO.setClassName(serviceEntityApiDTO.get().getClassName());
                providerBaseApiDTO.setDisplayName(serviceEntityApiDTO.get().getDisplayName());
                providerBaseApiDTO.setUuid(serviceEntityApiDTO.get().getUuid());
            }
            if (reason.getFailedResourcesList().isEmpty()) {
                logger.warn("Unplacement reason resource list is empty for service entity");
                break;
            }
            FailedResources failedResource = reason.getFailedResourcesList().get(0);
            Pair<String, String> resource = COMMODITY_TYPE_NAME_UNIT_MAP.get(
                    failedResource.getCommType().getType()) == null
                    ? Pair.of(CommodityType.UNKNOWN.name(), CommodityType.UNKNOWN.name())
                    : COMMODITY_TYPE_NAME_UNIT_MAP.get(failedResource.getCommType().getType());
            BaseApiDTO clusterApiDTO = clusterMap.containsKey(reason.getClosestSellerClusterOid())
                    ? clusterMap.get(reason.getClosestSellerClusterOid()) : new BaseApiDTO();
            placementInfoApiDTO.getFailureInfos().add(new ReservationFailureInfoDTO(
                    resource.getLeft(),
                    providerBaseApiDTO,
                    failedResource.getMaxAvailable(),
                    failedResource.getRequestedAmount(),
                    resource.getRight(),
                    reason.getIsCurrent() ? Epoch.CURRENT : Epoch.HISTORICAL,
                    clusterApiDTO));
        }
    }

    /**
     * Creates related resource objects, based on the entity type. If it is Physical machine, it creates
     * compute resources, if it is Storage, it creates storage resources, if it is Network, it creates
     * network resources.
     *
     * @param providerInfo oid of provider.
     * @param serviceEntityApiDTOMap a Map which key is oid, value is {@link ServiceEntityApiDTO}.
     * @param placementInfoApiDTO {@link PlacementInfoDTO}.
     * @throws UnknownObjectException if there are entity types are not support.
     * @throws ProviderIdNotRecognizedException if there are provider id is not exist.
     */
    private void addResourcesApiDTO(final ProviderInfo providerInfo,
                                    @Nonnull Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap,
                                    @Nonnull PlacementInfoDTO placementInfoApiDTO,
                                    final Map<Long, BaseApiDTO> clusterMap)
            throws UnknownObjectException, ProviderIdNotRecognizedException {
        if (!providerInfo.getProviderId().isPresent()) {
            // should not be happening. The caller already verifies this.
            return;
        }
        ServiceEntityApiDTO serviceEntityApiDTO = serviceEntityApiDTOMap
                        .get(providerInfo.getProviderId().get());
        // if entity id is not present, it means this reservation is unplaced.
        if (serviceEntityApiDTO == null) {
            throw  new ProviderIdNotRecognizedException(providerInfo.getProviderId().get());
        }
        final int entityType = ApiEntityType.fromString(serviceEntityApiDTO.getClassName()).typeNumber();
        Optional<BaseApiDTO> clusterBaseApiDTO = providerInfo.getClusterId().map(clusterMap::get);
        switch (entityType) {
            case EntityType.PHYSICAL_MACHINE_VALUE:
                final List<ResourceApiDTO> computeResources =
                        placementInfoApiDTO.getComputeResources() != null
                                ? placementInfoApiDTO.getComputeResources()
                                : new ArrayList<>();
                computeResources.add(generateResourcesApiDTO(serviceEntityApiDTO,
                        providerInfo.getCommoditiesBought(), clusterBaseApiDTO));
                placementInfoApiDTO.setComputeResources(computeResources);
                break;
            case EntityType.STORAGE_VALUE:
                final List<ResourceApiDTO> storageResources =
                        placementInfoApiDTO.getStorageResources() != null
                                ? placementInfoApiDTO.getStorageResources()
                                : new ArrayList<>();
                storageResources.add(generateResourcesApiDTO(serviceEntityApiDTO,
                        providerInfo.getCommoditiesBought(), clusterBaseApiDTO));
                placementInfoApiDTO.setStorageResources(storageResources);
                break;
            default:
                throw new UnknownObjectException("Unknown entity type: " + entityType);
        }
    }

    /**
     * Populate the stats for each of the commodity bought by the VM associated with the reservation.
     * We need the stats info to distinguish between different disk buying from different storages.
     *
     * @param serviceEntityApiDTO  The provider service entity
     * @param commodityBoughtDTOList the commodities bought by the VM associated with the reservation.
     * @param clusterBaseApiDTO the cluster the provider belongs to.
     * @return ResourceApiDTO populated with providerID and the stats.
     */
    private ResourceApiDTO generateResourcesApiDTO(@Nonnull final ServiceEntityApiDTO serviceEntityApiDTO,
                                                   List<CommodityBoughtDTO> commodityBoughtDTOList,
                                                   Optional<BaseApiDTO> clusterBaseApiDTO) {
        final BaseApiDTO providerBaseApiDTO = new BaseApiDTO();
        providerBaseApiDTO.setClassName(serviceEntityApiDTO.getClassName());
        providerBaseApiDTO.setDisplayName(serviceEntityApiDTO.getDisplayName());
        providerBaseApiDTO.setUuid(serviceEntityApiDTO.getUuid());
        final ResourceApiDTO resourceApiDTO = new ResourceApiDTO();
        resourceApiDTO.setProvider(providerBaseApiDTO);
        List<StatApiDTO> statApiDTOS = new ArrayList<>();
        for (CommodityBoughtDTO commodityBoughtDTO : commodityBoughtDTOList) {
            Pair<String, String> commodityNameUnit = COMMODITY_TYPE_NAME_UNIT_MAP.get(commodityBoughtDTO.getCommodityType().getType());
            if (commodityNameUnit != null) {
                final StatApiDTO statApiDTO = new StatApiDTO();
                statApiDTO.setName(commodityNameUnit.getLeft());
                statApiDTO.setUnits(commodityNameUnit.getRight());
                statApiDTO.setValue((float)commodityBoughtDTO.getUsed());
                final StatValueApiDTO statValueApiDTO = new StatValueApiDTO();
                statValueApiDTO.setAvg((float)commodityBoughtDTO.getUsed());
                statApiDTO.setValues(statValueApiDTO);
                statApiDTOS.add(statApiDTO);
            }
        }
        resourceApiDTO.setStats(statApiDTOS);
        clusterBaseApiDTO.ifPresent(resourceApiDTO.getLinkedResources()::add);
        return resourceApiDTO;
    }

    /**
     * Send request to group to fetch cluster information by cluster ids.
     *
     * @param placementInfos contains all initial placement results which only keep ids.
     * @return a map which key is cluster id, value is {@link BaseApiDTO}.
     */
    private Map<Long, BaseApiDTO> getClusterMap(@Nonnull final List<PlacementInfo> placementInfos) {
        // This set contains cluster OIDs.
        Set<Long> clusterIds = new HashSet<>();
        for (PlacementInfo placementInfo : placementInfos) {
            for (ProviderInfo providerInfo : placementInfo.getProviderInfos()) {
                if (providerInfo.getProviderId().isPresent()) {
                    providerInfo.getClusterId().ifPresent(clusterIds::add);
                }
            }
            for (UnplacementReason reason : placementInfo.getUnpalcementReasons()) {
                clusterIds.add(reason.getClosestSellerClusterOid());
                reason.getClosestSellerClusterNameBytes();
            }
        }
        final Map<Long, BaseApiDTO> clusterMap = new HashMap<>();
        if (clusterIds.size() == 0) {
            return clusterMap;
        }
        GetPartialGroupingInfoRequest request =
                GetPartialGroupingInfoRequest.newBuilder().setReturnType(Type.MINIMAL).addAllIds(
                        clusterIds).build();

        Iterator<GroupDTO.PartialGroupingInfo> minimalGroups =
                groupServiceBlockingStub.getPartialGroupingInfo(request);

        while (minimalGroups.hasNext()) {
            GroupDTO.PartialGroupingInfo curGroupInfo = minimalGroups.next();
            if (GroupProtoUtil.CLUSTER_GROUP_TYPES.contains(curGroupInfo.getMinimal().getType())) {
                final BaseApiDTO apiDTO = new BaseApiDTO();
                apiDTO.setDisplayName(curGroupInfo.getMinimal().getDisplayName());
                apiDTO.setUuid(String.valueOf(curGroupInfo.getMinimal().getOid()));
                clusterMap.put(curGroupInfo.getMinimal().getOid(), apiDTO);
            }
        }

        if (clusterMap.values().size() != clusterIds.size()) {
            logger.debug(clusterIdAmountMisMatch, clusterMap.values().size(),
                    clusterIds.size());
        }
        return clusterMap;
    }

    /**
     * A wrapper class to store initial placement results.
     */
    @Immutable
    public static class PlacementInfo {
        // oid of added template entity
        private final long entityId;
        // a list of oids which are the providers of entityId.
        private final List<ProviderInfo> providerInfos;
        // failure information when reservation fails.
        private final List<UnplacementReason> reasons;
        // reason why reservation is invalid
        private final Optional<InvalidInfo> invalidInfo;

        /**
         * Constructor.
         *
         * @param entityId         The ID of the template entity.
         * @param providerInfoList The list of provider OIDs.
         * @param reasons          The reasons for failed reservations.
         */
        public PlacementInfo(final long entityId,
                             @Nonnull final List<ProviderInfo> providerInfoList,
                             @Nonnull final List<UnplacementReason> reasons,
                             final Optional<InvalidInfo> invalidInfo) {
            this.entityId = entityId;
            this.providerInfos = Collections.unmodifiableList(providerInfoList);
            this.reasons = reasons;
            this.invalidInfo = invalidInfo;
        }

        /**
         * Getter method for failureInfos.
         * @return the failureInfos.
         */
        public List<UnplacementReason> getUnpalcementReasons() {
            return reasons;
        }

        public long getEntityId() {
            return this.entityId;
        }

        public Optional<InvalidInfo> getInvalidInfo() {
            return invalidInfo;
        }

        public List<ProviderInfo> getProviderInfos() {
            return this.providerInfos;
        }
    }

    /**
     * A wrapper class to store the oid of provider and the commodities bought by the template from
     * the provider.
     */
    @Immutable
    public static class ProviderInfo {

        private final Optional<Long> providerId;
        private final List<CommodityBoughtDTO> commoditiesBought;
        // the id of the cluster where the entity is placed on.
        private final Optional<Long> clusterId;

        /**
         * Constructor.
         *
         * @param providerId       the oid of the provider
         * @param commoditiesBought the commodities bought by the template from the provider.
         * @param clusterId the id of the cluster where the entity is placed on.
         */
        public ProviderInfo(final Optional<Long> providerId,
                            @Nonnull final List<CommodityBoughtDTO> commoditiesBought,
                            Optional<Long> clusterId) {
            this.providerId = providerId;
            this.commoditiesBought = Collections.unmodifiableList(commoditiesBought);
            this.clusterId = clusterId;
        }

        public Optional<Long> getProviderId() {
            return providerId;
        }

        public List<CommodityBoughtDTO> getCommoditiesBought() {
            return commoditiesBought;
        }

        /**
         * getter for clusterId.
         * @return the clusterId.
         */
        public Optional<Long> getClusterId() {
            return clusterId;
        }
    }

    /**
     * A exception represents provider id is not recognized in current topology.
     */
    private static class ProviderIdNotRecognizedException extends Exception {
        ProviderIdNotRecognizedException(@Nonnull final long id) {
            super("Provider Id: " + id + " not found.");
        }
    }
}
