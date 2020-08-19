package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.DynamicConnectionFilters;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.UNKNOWN;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.service.GroupsService;
import com.vmturbo.api.dto.group.BaseGroupApiDTO;
import com.vmturbo.api.dto.group.FilterApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.topologydefinition.AutomatedEntityDefinitionData;
import com.vmturbo.api.dto.topologydefinition.IEntityDefinitionData;
import com.vmturbo.api.dto.topologydefinition.IManualConnectionsData;
import com.vmturbo.api.dto.topologydefinition.ManualDynamicConnections;
import com.vmturbo.api.dto.topologydefinition.ManualEntityDefinitionData;
import com.vmturbo.api.dto.topologydefinition.ManualGroupConnections;
import com.vmturbo.api.dto.topologydefinition.ManualStaticConnections;
import com.vmturbo.api.dto.topologydefinition.TopologyDataDefinitionApiDTO;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition.TagBasedGenerationAndConnection;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition.AssociatedEntitySelectionCriteria;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchParameters.FilterSpecs;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Responsible for mapping TopologyDataDefinition-related XL objects to their API counterparts.
 */
public class TopologyDataDefinitionMapper {

    private static final Logger logger = LogManager.getLogger();

    private static final String APPLICATION_COMPONENTS_BY_TAG = "applicationComponentsByTag";
    private static final String BUSINESS_TRANSACTIONS_BY_TAG = "businessTransactionsByTag";
    private static final String CONTAINERS_BY_TAG = "containersByTag";
    private static final String CONTAINER_PODS_BY_TAG = "containerPodsByTag";
    private static final String DATABASE_SERVERS_BY_TAG = "databaseServersByTag";
    private static final String SERVICES_BY_TAG = "servicesByTag";
    private static final String VIRTUAL_MACHINES_BY_TAG = "virtualMachinesByTag";

    private static final BiMap<CommonDTO.EntityDTO.EntityType, EntityType> USER_DEFINED_ENTITY_TYPES_MAP =
            ImmutableBiMap.<CommonDTO.EntityDTO.EntityType, EntityType>builder()
                .put(CommonDTO.EntityDTO.EntityType.BUSINESS_APPLICATION, EntityType.BusinessApplication)
                .put(CommonDTO.EntityDTO.EntityType.BUSINESS_TRANSACTION, EntityType.BusinessTransaction)
                .put(CommonDTO.EntityDTO.EntityType.SERVICE, EntityType.Service)
                .build();

    private static final BiMap<CommonDTO.EntityDTO.EntityType, EntityType> CONNECTED_ENTITY_TYPES_MAP =
            ImmutableBiMap.<CommonDTO.EntityDTO.EntityType, EntityType>builder()
                    .put(CommonDTO.EntityDTO.EntityType.APPLICATION_COMPONENT, EntityType.ApplicationComponent)
                    .put(CommonDTO.EntityDTO.EntityType.BUSINESS_TRANSACTION, EntityType.BusinessTransaction)
                    .put(CommonDTO.EntityDTO.EntityType.CONTAINER, EntityType.Container)
                    .put(CommonDTO.EntityDTO.EntityType.CONTAINER_POD, EntityType.ContainerPod)
                    .put(CommonDTO.EntityDTO.EntityType.DATABASE_SERVER, EntityType.DatabaseServer)
                    .put(CommonDTO.EntityDTO.EntityType.SERVICE, EntityType.Service)
                    .put(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE, EntityType.VirtualMachine)
                    .build();

    /**
     * This maps entityType used by XL to default filter type.
     */
    public static final Map<CommonDTO.EntityDTO.EntityType, String> ENTITY_TYPE_TO_FILTER_TYPE_MAP =
            ImmutableMap.<CommonDTO.EntityDTO.EntityType, String>builder()
                    .put(CommonDTO.EntityDTO.EntityType.APPLICATION_COMPONENT, APPLICATION_COMPONENTS_BY_TAG)
                    .put(CommonDTO.EntityDTO.EntityType.BUSINESS_TRANSACTION, BUSINESS_TRANSACTIONS_BY_TAG)
                    .put(CommonDTO.EntityDTO.EntityType.CONTAINER, CONTAINERS_BY_TAG)
                    .put(CommonDTO.EntityDTO.EntityType.CONTAINER_POD, CONTAINER_PODS_BY_TAG)
                    .put(CommonDTO.EntityDTO.EntityType.DATABASE_SERVER, DATABASE_SERVERS_BY_TAG)
                    .put(CommonDTO.EntityDTO.EntityType.SERVICE, SERVICES_BY_TAG)
                    .put(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE, VIRTUAL_MACHINES_BY_TAG)
                    .build();

    private final EntityFilterMapper entityFilterMapper;
    private final GroupsService groupsService;

    /**
     * Constructor.
     *
     * @param entityFilterMapper - a converted between API filters and search filters.
     * @param groupsService      - a provider of group models.
     */
    @ParametersAreNonnullByDefault
    public TopologyDataDefinitionMapper(final EntityFilterMapper entityFilterMapper,
                                        final GroupsService groupsService) {
        this.entityFilterMapper = entityFilterMapper;
        this.groupsService = groupsService;
    }

    /**
     * Logs text and throws {@link IllegalArgumentException}.
     *
     * @param errorText text for the exception
     * @throws IllegalArgumentException exception thrown
     */
    private void processException(String errorText) throws IllegalArgumentException {
        IllegalArgumentException e = new IllegalArgumentException(errorText);
        logger.error(e.getLocalizedMessage(), e);
        throw e;
    }

    /**
     * Convert XL-related definition to API DTO.
     *
     * @param definition XL-related definition
     * @return API DTO
     * @throws IllegalArgumentException error if XL definition is incorrectly specified
     */
    public TopologyDataDefinitionApiDTO convertTopologyDataDefinition(@Nonnull final TopologyDataDefinition definition)
            throws IllegalArgumentException {

        if (!definition.hasAutomatedEntityDefinition() && !definition.hasManualEntityDefinition()) {
            processException(String.format("Neither a manual nor an automated topology data definition was specified in definition: %s",
                    definition.toString()));
        }
        if (definition.hasAutomatedEntityDefinition()) {
            return convertAutomatedDefinition(definition.getAutomatedEntityDefinition());
        }
        return convertManualDefinition(definition.getManualEntityDefinition());
    }

    /**
     * Converts {@link AutomatedEntityDefinition} to {@link TopologyDataDefinitionApiDTO}.
     *
     * @param definition XL-related model of automated entity definition
     * @return API-related {@link TopologyDataDefinitionApiDTO}
     */
    private TopologyDataDefinitionApiDTO convertAutomatedDefinition(@Nonnull final AutomatedEntityDefinition definition) {
        TopologyDataDefinitionApiDTO dto = new TopologyDataDefinitionApiDTO();
        CommonDTO.EntityDTO.EntityType entityType = definition.getEntityType();
        if (!USER_DEFINED_ENTITY_TYPES_MAP.containsKey(entityType)) {
            processException(String.format("Automated topology definition has unsupported entity type: %s",
                    entityType.name()));
        }
        dto.setEntityType(USER_DEFINED_ENTITY_TYPES_MAP.get(entityType));
        AutomatedEntityDefinitionData data = getAutomatedEntityDefinitionData(definition);
        dto.setEntityDefinitionData(data);
        dto.setDisplayName(generateAutomatedDisplayName(data));
        return dto;
    }

    /**
     * Generates displayName for automated entity definition.
     *
     * @param data {@link AutomatedEntityDefinitionData}.
     * @return display name
     */
    private String generateAutomatedDisplayName(@Nonnull final AutomatedEntityDefinitionData data) {
        return String.format("%s_%s", data.getNamePrefix(),
                data.getGenerationAndConnectionAttribute().getExpVal());
    }

    /**
     * Get data for automated entity definition API DTO.
     *
     * @param definition XL-related model of automated entity definition
     * @return API-related {@link AutomatedEntityDefinitionData}
     */
    private AutomatedEntityDefinitionData getAutomatedEntityDefinitionData(@Nonnull final AutomatedEntityDefinition definition) {
        AutomatedEntityDefinitionData data = new AutomatedEntityDefinitionData();
        CommonDTO.EntityDTO.EntityType connectedEntityType = definition.getConnectedEntityType();
        if (!CONNECTED_ENTITY_TYPES_MAP.containsKey(connectedEntityType)
                || !ENTITY_TYPE_TO_FILTER_TYPE_MAP.containsKey(connectedEntityType)) {
            processException(String.format("Automated topology definition connected to unsupported entity type: %s",
                    connectedEntityType.name()));
        }
        data.setEntityType(CONNECTED_ENTITY_TYPES_MAP.get(connectedEntityType));
        if (definition.hasNamingPrefix()) {
            data.setNamePrefix(definition.getNamingPrefix());
        } else {
            processException(String.format("No naming prefix specified for definition: %s",
                    definition.toString()));
        }
        if (definition.hasTagGrouping() && definition.getTagGrouping().hasTagKey()) {
            FilterApiDTO filter = new FilterApiDTO();
            filter.setExpType("EQ");
            filter.setExpVal(definition.getTagGrouping().getTagKey());
            filter.setFilterType(ENTITY_TYPE_TO_FILTER_TYPE_MAP.get(connectedEntityType));
            filter.setCaseSensitive(true);
            filter.setSingleLine(false);
            data.setGenerationAndConnectionAttribute(filter);
        } else {
            processException(String.format("No tag specified for definition: %s",
                    definition.toString()));
        }
        return data;
    }

    /**
     * Converts {@link ManualEntityDefinition} to {@link TopologyDataDefinitionApiDTO}.
     *
     * @param definition XL-related model of manual entity definition
     * @return API-related {@link TopologyDataDefinitionApiDTO}
     */
    private TopologyDataDefinitionApiDTO convertManualDefinition(@Nonnull final ManualEntityDefinition definition) {
        TopologyDataDefinitionApiDTO dto = new TopologyDataDefinitionApiDTO();
        if (!definition.hasEntityName()) {
            processException(String.format("Manual topology definition doesn't have entity name: %s",
                    definition.toString()));
        }
        if (!definition.hasEntityType()) {
            processException(String.format("Manual topology definition doesn't have entity type: %s",
                    definition.toString()));
        }
        CommonDTO.EntityDTO.EntityType entityType = definition.getEntityType();
        if (!USER_DEFINED_ENTITY_TYPES_MAP.containsKey(entityType)) {
            processException(String.format("Manual topology definition has unsupported entity type: %s",
                    entityType.name()));
        }
        dto.setEntityType(USER_DEFINED_ENTITY_TYPES_MAP.get(entityType));
        dto.setEntityDefinitionData(getManualEntityDefinitionData(definition));
        dto.setDisplayName(definition.getEntityName());
        return dto;
    }

    /**
     * Get data for manual entity definition API DTO.
     *
     * @param definition XL-related model of manual entity definition
     * @return API-related {@link AutomatedEntityDefinitionData}
     */
    private IEntityDefinitionData getManualEntityDefinitionData(@Nonnull final ManualEntityDefinition definition) {
        ManualEntityDefinitionData data = new ManualEntityDefinitionData();
        Map<EntityType, IManualConnectionsData> connectionData = new HashMap<>();
        List<AssociatedEntitySelectionCriteria> associatedEntities = definition.getAssociatedEntitiesList();
        for (AssociatedEntitySelectionCriteria criteria : associatedEntities) {
            if (!criteria.hasConnectedEntityType()) {
                processException(String.format("No connected entity type specified in manual topology definition data: %s",
                        criteria.toString()));
            }
            if (!CONNECTED_ENTITY_TYPES_MAP.containsKey(criteria.getConnectedEntityType())) {
                processException(String.format("Unsupported entity type (%s) specified in manual topology definition data: %s",
                        criteria.getConnectedEntityType().name(),
                        criteria.toString()));
            }
            EntityType connectedEntityType = CONNECTED_ENTITY_TYPES_MAP.get(criteria.getConnectedEntityType());
            IManualConnectionsData connections = null;
            switch (criteria.getSelectionTypeCase()) {
                case STATIC_ASSOCIATED_ENTITIES:
                    connections = new ManualStaticConnections();
                    List<StaticMembersByType> members = criteria.getStaticAssociatedEntities().getMembersByTypeList();
                    if (members.size() > 1) {
                        processException(String.format("Incorrectly specified static connections list for type: %s",
                                connectedEntityType.name()));
                    } else if (members.isEmpty()) {
                        processException(String.format("Static topology definition data is empty for type: %s",
                                connectedEntityType.name()));
                    }
                    ((ManualStaticConnections)connections).setStaticConnections(members.get(0)
                            .getMembersList()
                            .stream()
                            .map(String::valueOf)
                            .collect(Collectors.toList()));
                    break;
                case ASSOCIATED_GROUP:
                    connections = new ManualGroupConnections();
                    if (criteria.getAssociatedGroup().hasId()) {
                        final ManualGroupConnections groupConnection = (ManualGroupConnections)connections;
                        final String groupId = String.valueOf(criteria.getAssociatedGroup().getId());
                        try {
                            final GroupApiDTO groupApiDto = groupsService.getGroupByUuid(groupId, false);
                            if (groupApiDto != null) {
                                BaseGroupApiDTO baseGroupApiDTO = new BaseGroupApiDTO();
                                baseGroupApiDTO.setUuid(groupApiDto.getUuid());
                                baseGroupApiDTO.setDisplayName(groupApiDto.getDisplayName());
                                baseGroupApiDTO.setMembersCount(groupApiDto.getMembersCount());
                                groupConnection.setConnectedGroup(baseGroupApiDTO);
                            }
                        } catch (UnknownObjectException | ConversionException | InterruptedException e) {
                            logger.warn(e);
                        }
                    }
                    break;
                case DYNAMIC_CONNECTION_FILTERS:
                    connections = new ManualDynamicConnections();
                    DynamicConnectionFilters connectionFilters = criteria.getDynamicConnectionFilters();
                    List<FilterApiDTO> dynamicConnectionsCriteria = connectionFilters.getSearchParametersList()
                            .stream()
                            .map(SearchParameters::getSourceFilterSpecs)
                            .map(this::mapFilterSpecsToFilterApiDTO)
                            .collect(Collectors.toList());
                    ((ManualDynamicConnections)connections).setDynamicConnectionCriteria(dynamicConnectionsCriteria);
                    break;
                case SELECTIONTYPE_NOT_SET:
                    logger.warn("Connections data type not set: {}", criteria.toString());
                    break;
            }
            if (connections == null) {
                logger.warn("Cannot parse connections data: {}", criteria.toString());
            } else {
                connectionData.put(connectedEntityType, connections);
            }
        }
        data.setManualConnectionData(connectionData);
        return data;
    }

    /**
     * Maps {@link FilterSpecs} to {@link FilterApiDTO}.
     *
     * @param filterSpecs filter specs
     * @return filter API DTO
     */
    private FilterApiDTO mapFilterSpecsToFilterApiDTO(@Nonnull final FilterSpecs filterSpecs) {
        FilterApiDTO filterApiDTO = new FilterApiDTO();
        if (filterSpecs.hasExpressionType()) {
            filterApiDTO.setExpType(filterSpecs.getExpressionType());
        } else {
            processException("Incorrectly set filter, expressionType field is 'null'");
        }
        if (filterSpecs.hasExpressionValue()) {
            filterApiDTO.setExpVal(filterSpecs.getExpressionValue());
        } else {
            processException("Incorrectly set filter, expressionValue field is 'null'");
        }
        if (filterSpecs.hasFilterType()) {
            filterApiDTO.setFilterType(filterSpecs.getFilterType());
        } else {
            processException("Incorrectly set filter, filterType field is 'null'");
        }
        if (filterSpecs.hasIsCaseSensitive()) {
            filterApiDTO.setCaseSensitive(filterSpecs.getIsCaseSensitive());
        } else {
            filterApiDTO.setCaseSensitive(false);
        }
        filterApiDTO.setSingleLine(false);
        return filterApiDTO;
    }

    /**
     * Convert API definition to XL-related one.
     *
     * @param definitionApiDTO API definition
     * @return XL-related definition
     * @throws IllegalArgumentException error if API definition is incorrectly specified
     */
    @Nonnull
    public TopologyDataDefinition convertTopologyDataDefinitionApiDTO(@Nonnull final TopologyDataDefinitionApiDTO definitionApiDTO)
            throws IllegalArgumentException {
        if (definitionApiDTO.getEntityDefinitionData() == null
                || definitionApiDTO.getEntityDefinitionData().getDefinitionType() == null) {
            processException(String.format("Incorrect definition API DTO: %s",
                    definitionApiDTO.toString()));
        }
        switch (definitionApiDTO.getEntityDefinitionData().getDefinitionType()) {
            case AUTOMATED: return convertAutomatedDefinitionApiDTO(definitionApiDTO);
            case MANUAL: return convertManualDefinitionApiDTO(definitionApiDTO);
            default: processException(String.format("Wrong definition API DTO type: %s",
                    definitionApiDTO));
        }
        return TopologyDataDefinition.newBuilder().build();
    }

    /**
     * Convert automated API definition to XL-related one.
     *
     * @param definitionApiDTO automated API definition
     * @return XL-related definition
     * @throws IllegalArgumentException error if API definition is incorrectly specified
     */
    private TopologyDataDefinition convertAutomatedDefinitionApiDTO(@Nonnull final TopologyDataDefinitionApiDTO definitionApiDTO) {

        AutomatedEntityDefinitionData data = (AutomatedEntityDefinitionData)definitionApiDTO.getEntityDefinitionData();

        CommonDTO.EntityDTO.EntityType entityType = USER_DEFINED_ENTITY_TYPES_MAP.inverse()
                .getOrDefault(definitionApiDTO.getEntityType(), UNKNOWN);
        if (entityType == UNKNOWN) {
            processException(String.format("Incorrect entity type: %s",
                    definitionApiDTO.getEntityType().name()));
        }
        CommonDTO.EntityDTO.EntityType connectedEntityType = CONNECTED_ENTITY_TYPES_MAP.inverse()
                .getOrDefault(data.getEntityType(), UNKNOWN);
        if (connectedEntityType == UNKNOWN) {
            processException(String.format("Incorrect associated entity type: %s",
                    data.getEntityType()));
        }

        final String namePrefix = data.getNamePrefix();
        if (namePrefix == null) {
            processException(String.format("Name prefix is 'null' for automated definition: %s",
                    definitionApiDTO.toString()));
        }

        FilterApiDTO filter = data.getGenerationAndConnectionAttribute();
        if (filter == null || filter.getExpVal() == null) {
            processException(String.format("Connection attribute is incorrectly specified for automated definition: %s",
                    entityType.name()));
        }

        return TopologyDataDefinition.newBuilder().setAutomatedEntityDefinition(AutomatedEntityDefinition.newBuilder()
                .setEntityType(entityType)
                .setConnectedEntityType(connectedEntityType)
                .setNamingPrefix(data.getNamePrefix())
                .setTagGrouping(TagBasedGenerationAndConnection.newBuilder().setTagKey(filter.getExpVal())))
        .build();
    }

    /**
     * Convert manual API definition to XL-related one.
     *
     * @param definitionApiDTO manual API definition
     * @return XL-related definition
     * @throws IllegalArgumentException error if API definition is incorrectly specified
     */
    private TopologyDataDefinition convertManualDefinitionApiDTO(@Nonnull final TopologyDataDefinitionApiDTO definitionApiDTO) {

        ManualEntityDefinitionData data = (ManualEntityDefinitionData)definitionApiDTO.getEntityDefinitionData();

        CommonDTO.EntityDTO.EntityType entityType = USER_DEFINED_ENTITY_TYPES_MAP.inverse()
                .getOrDefault(definitionApiDTO.getEntityType(), UNKNOWN);
        if (entityType == UNKNOWN) {
            processException(String.format("Incorrect entity type: %s",
                    definitionApiDTO.getEntityType().name()));
        }

        if (definitionApiDTO.getDisplayName() == null) {
            processException(String.format("Display name of topology definition is 'null': %s",
                    definitionApiDTO.toString()));
        }

        List<AssociatedEntitySelectionCriteria> associatedEntities = new ArrayList<>();
        for (Map.Entry<EntityType, IManualConnectionsData> entry : data.getManualConnectionData().entrySet()) {
            CommonDTO.EntityDTO.EntityType connectedEntityType = CONNECTED_ENTITY_TYPES_MAP.inverse()
                    .getOrDefault(entry.getKey(), UNKNOWN);
            if (connectedEntityType == UNKNOWN) {
                processException(String.format("Incorrect associated entity type: %s",
                        entry.getKey()));
            }
            IManualConnectionsData connectionsData = entry.getValue();
            try {
                AssociatedEntitySelectionCriteria.Builder criteriaBuilder = AssociatedEntitySelectionCriteria.newBuilder();
                switch (connectionsData.getConnectionType()) {
                    case GROUP:
                        criteriaBuilder = getGroupCriteria((ManualGroupConnections)connectionsData);
                        break;
                    case STATIC:
                        criteriaBuilder = getStaticCriteria((ManualStaticConnections)connectionsData, connectedEntityType);
                        break;
                    case DYNAMIC:
                        criteriaBuilder = getDynamicCriteria((ManualDynamicConnections)connectionsData, entry.getKey());
                        break;
                    default:
                        processException(String.format("Incorrect connection type: %s",
                                connectionsData.getConnectionType()));
                }
                criteriaBuilder.setConnectedEntityType(connectedEntityType);
                associatedEntities.add(criteriaBuilder.build());
            } catch (Exception e) {
                IllegalArgumentException ex = new IllegalArgumentException("Cannot parse connections data", e);
                logger.error(ex.getLocalizedMessage(), ex);
                throw ex;
            }
        }
        return TopologyDataDefinition.newBuilder()
                .setManualEntityDefinition(ManualEntityDefinition.newBuilder()
                        .setEntityType(entityType)
                        .setEntityName(definitionApiDTO.getDisplayName())
                        .addAllAssociatedEntities(associatedEntities))
                .build();
    }

    /**
     * Get group connections criteria from API DTO.
     *
     * @param groupConnections API DTO for group connections
     * @return criteria for group connections
     */
    private AssociatedEntitySelectionCriteria.Builder getGroupCriteria(@Nonnull final ManualGroupConnections groupConnections) {
        AssociatedEntitySelectionCriteria.Builder criteriaBuilder = AssociatedEntitySelectionCriteria.newBuilder();
        if (groupConnections.getConnectedGroup() != null
                && !StringUtils.isBlank(groupConnections.getConnectedGroup().getUuid())) {
            criteriaBuilder.setAssociatedGroup(GroupDTO.GroupID.newBuilder()
                    .setId(Long.parseLong(groupConnections.getConnectedGroup().getUuid()))
                    .build());
        }
        return criteriaBuilder;
    }

    /**
     * Get static connections criteria from API DTO.
     *
     * @param staticConnections API DTO for static connections
     * @param entityType type of entity for static connections
     * @return criteria for static connections
     */
    private AssociatedEntitySelectionCriteria.Builder getStaticCriteria(@Nonnull final ManualStaticConnections staticConnections,
                                                                        @Nonnull final CommonDTO.EntityDTO.EntityType entityType) {
        StaticMembersByType.Builder members = StaticMembersByType.newBuilder()
                .setType(GroupDTO.MemberType.newBuilder().setEntity(entityType.getNumber()).build())
                .addAllMembers(staticConnections
                        .getStaticConnections()
                        .stream()
                        .map(Long::parseLong)
                        .collect(Collectors.toList()));
        return AssociatedEntitySelectionCriteria.newBuilder()
                .setStaticAssociatedEntities(GroupDTO.StaticMembers.newBuilder()
                        .addMembersByType(members));
    }

    /**
     * Get dynamic connections criteria from API DTO.
     *
     * @param dynamicConnections API DTO for dynamic connections.
     * @param entityType API entity type of dynamic connection members.
     * @return criteria for dynamic connections
     */
    @Nonnull
    @ParametersAreNonnullByDefault
    private AssociatedEntitySelectionCriteria.Builder getDynamicCriteria(final ManualDynamicConnections dynamicConnections,
                                                                         final EntityType entityType) {
        final List<SearchParameters> searchParameters = entityFilterMapper
                .convertToSearchParameters(dynamicConnections.getDynamicConnectionCriteria(),
                        entityType.name(), null);
        return AssociatedEntitySelectionCriteria.newBuilder()
                .setDynamicConnectionFilters(DynamicConnectionFilters.newBuilder()
                        .addAllSearchParameters(searchParameters).build());
    }

}
