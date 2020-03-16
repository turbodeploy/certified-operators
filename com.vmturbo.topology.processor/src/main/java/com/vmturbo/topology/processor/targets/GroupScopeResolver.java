package com.vmturbo.topology.processor.targets;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.commons.Pair;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.AccountValue;
import com.vmturbo.platform.common.dto.Discovery.AccountValue.PropertyValueList;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.GroupScopeProperty;
import com.vmturbo.platform.sdk.common.EntityPropertyName;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 * Utility class for extracting group scope information from a probe's account definition list
 * and then populating the account values for each target with the properties defined in the
 * account definition list.
 */
public class GroupScopeResolver {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Group service stub for getting group membership from Group Service.
     */
    private final GroupServiceBlockingStub groupService;

    /**
     * Search Service stub used for getting entities whose properties need to be extracted and
     * put into group scope, as well as the related guest load entities.
     */
    private final SearchServiceBlockingStub searchService;

    /**
     * Target store to use for retrieve the probe category based on the target OID. We need to make
     * sure an application entity is discovered by hypervisor or cloud probe to be guest load entity.
     */
    private final TargetStore targetStore;

    /**
     * Entity store to use for retrieving the original EntityDTO returned from probe.
     */
    private final EntityStore entityStore;

    /**
     * The probe categories which discover the real VMs instead of proxy ones, and the guest load
     * entities of them must be discovered by these kinds of probes as well.
     */
    private final Set<ProbeCategory> guestLoadOriginProbeCategories = Sets.immutableEnumSet(
            ProbeCategory.HYPERVISOR, ProbeCategory.CLOUD_MANAGEMENT);

    private final CustomScopingOperationLibrary scopingOperationLibrary = new CustomScopingOperationLibrary();

    /**
     * Constructor of a GroupScopeResolver.
     *
     * @param groupChannel Channel to use for creating a blocking stub to query the Group Service.
     * @param repositoryChannel Channel to use for creating a blocking stub to query the
     *                          Repository Service.
     * @param targetStore Target store to use for retrieve the probe category based on the target
     *                    OID.
     */
    public GroupScopeResolver(@Nonnull final Channel groupChannel,
                              @Nonnull final Channel repositoryChannel,
                              @Nonnull final TargetStore targetStore,
                              @Nonnull final EntityStore entityStore) {
        this.groupService = GroupServiceGrpc.newBlockingStub(Objects.requireNonNull(groupChannel));
        this.searchService = SearchServiceGrpc.newBlockingStub(
                Objects.requireNonNull(repositoryChannel));
        this.targetStore = Objects.requireNonNull(targetStore);
        this.entityStore = Objects.requireNonNull(entityStore);
    }

    private Map<String, CustomAccountDefEntry> generateGroupScopeMap(
            @Nonnull List<AccountDefEntry> accountDefList) {
        Objects.requireNonNull(accountDefList);
        return accountDefList.stream()
                .filter(AccountDefEntry::hasCustomDefinition)
                .map(AccountDefEntry::getCustomDefinition)
                .filter(customAcctDefEntry -> customAcctDefEntry.hasEntityScope() ||
                    customAcctDefEntry.hasGroupScope())
                .collect(Collectors.toMap(CustomAccountDefEntry::getName, Function.identity()));

    }

    /**
     * Take a collection of account values and probe info for a probe and return a collection of
     * account values with any group scope account values properly populated.
     *
     * @param probeType the {@link SDKProbeType} that the account values and account definition list
     *                  belong to.  May be null if the probe type is not listed in the enum.  This
     *                  will not be an issue for group scope, but entity scope will fail if the
     *                  probe type is not in the enum.
     * @param newAccountValues a collection of {@link AccountValue} that needs to have its group
     *                         scope account values populated.
     * @param accountDefinitionList the list of {@link AccountDefEntry} that contains the account
     *                             value definitions for the probe associated with newAccountValues.
     * @return {@link List} of {@link AccountValue} where group scopes have been populated
     * with values for any AccountValues whose corresponding {@link AccountDefEntry} has a group
     * scope.
     */
    public List<AccountValue> processGroupScope(
            @Nullable SDKProbeType probeType,
            @Nonnull List<AccountValue> newAccountValues,
            @Nonnull List<AccountDefEntry> accountDefinitionList) {
        Map<String, CustomAccountDefEntry> keyToGroupScopeMap =
                generateGroupScopeMap(accountDefinitionList);
        logger.debug("Found {} account definitions with scope.",
            () -> keyToGroupScopeMap.keySet().size());

        return newAccountValues.stream()
                .map(accountValue -> keyToGroupScopeMap.keySet().contains(accountValue.getKey()) ?
                        populatePropertyValueList(probeType,
                            keyToGroupScopeMap.get(accountValue.getKey()),
                            accountValue)
                        : accountValue)
                .collect(Collectors.toList());
    }

    private Set<Long> getScopeOids(@Nullable SDKProbeType probeType,
                                    @Nonnull CustomAccountDefEntry customAcctDef,
                                    @Nonnull AccountValue accountVal) {
        if (customAcctDef.hasGroupScope()) {
            String groupId = accountVal.getStringValue();
            logger.debug("Getting OIDs for group scope with group ID: {}", groupId);
            GetMembersResponse membersResponse = groupService.getMembers(GetMembersRequest.newBuilder()
                .addId(Long.parseLong(groupId))
                .setExpectPresent(true)
                .build())
                    .next();
            if (membersResponse.getMemberIdCount() == 0) {
                logger.warn("Group {} has no members.  "
                    + "No property values will be returned for group scope.", groupId);
                return Collections.emptySet();
            }
            logger.debug("Group {} has members {} in group scope processing.",
                groupId, membersResponse.getMemberIdList());
            // need to check if entityType of group is same as entityType of accountDef
            GetGroupResponse groupResponse =
                groupService.getGroup(
                                GroupID.newBuilder().setId(Long.parseLong(groupId)).build());
            if (!GroupProtoUtil.getEntityTypes(groupResponse.getGroup()).contains(
                            UIEntityType.fromType(customAcctDef
                                .getGroupScope().getEntityType().getNumber()))) {
                logger.error("Group {} contains the wrong entity type for group scope.  "
                        + "Expected type {}, but got type {}", groupId,
                    customAcctDef.getGroupScope().getEntityType(),
                    GroupProtoUtil.getEntityTypes(groupResponse.getGroup()).stream()
                        .map(UIEntityType::apiStr).collect(Collectors.joining(",")));
                return Collections.emptySet();
            }
            logger.trace("Group type matches group scope type.");
            return Sets.newHashSet(membersResponse.getMemberIdList());
        } else if (customAcctDef.hasEntityScope() && probeType != null) {
            final String entityProperty = accountVal.getStringValue();
            logger.debug("Getting entity scope OID for property value {}", entityProperty);
            Optional<CustomScopingOperation> scopingOp =
                scopingOperationLibrary.getCustomScopingOperation(probeType);
            if (scopingOp.isPresent()) {
                logger.debug("Found custom scoping operation {}",
                    () -> scopingOp.get().getClass().getSimpleName());
                return scopingOp.get().convertScopeValueToOid(entityProperty, searchService);
            }
        }
        return Collections.emptySet();
    }

    /**
     * If there is a group scope corresponding to this account value, populate the requested values
     * into the {@link AccountValue} that is passed in and return it.  If there is no Group Scope
     * in the {@link CustomAccountDefEntry} just return the {@link AccountValue} as is.
     *
     * @param probeType The {@link SDKProbeType} that this scope belongs to.  This is used in the
     *                  case of an entity scope to access the custom operation to convert the
     *                  account value to an OID.
     * @param customAcctDef the {@link CustomAccountDefEntry} corresponding to accountVal
     * @param accountVal the {@link AccountValue} to populate with group scope values if
     *                   necessary
     * @return either the original {@link AccountValue} or the {@link AccountValue} populated with
     * values extracted from the group scope
     */
    private AccountValue populatePropertyValueList(@Nullable SDKProbeType probeType,
                                                   @Nonnull CustomAccountDefEntry customAcctDef,
                                                   @Nonnull AccountValue accountVal) {
        Objects.requireNonNull(customAcctDef);
        Objects.requireNonNull(accountVal);
        final List<GroupScopeProperty> scopeProperties = customAcctDef.hasGroupScope() ?
            customAcctDef.getGroupScope().getPropertyList()
            : customAcctDef.getEntityScope().getPropertyList();
        List<Pair<EntityPropertyName, Boolean>> entityPropsPairs =
                getGroupScopePropertyNames(scopeProperties);
        Set<Long> scopeOids = getScopeOids(probeType, customAcctDef, accountVal);
        if (scopeOids.size() == 0) {
            return accountVal;
        }
        // retrieve all the scoped Topology Entity DTOs
        final List<TopologyEntityDTO> scopedTopologyEntityDTOs = retrieveScopedTopologyEntityDTOs(
                scopeOids);
        // retrieve related GuestLoad Topology Entity DTOs if the scoped entities are VMs
        final EntityType scopedEntityType = customAcctDef.hasGroupScope() ?
            customAcctDef.getGroupScope().getEntityType()
            : customAcctDef.getEntityScope().getEntityType();
        final List<TopologyEntityDTO> guestLoadTopologyEntityDTOs =
            scopedEntityType == EntityType.VIRTUAL_MACHINE ?
                retrieveGuestLoadTopologyEntityDTOs(scopeOids)
                : Collections.emptyList();
        logger.debug("Retrieved {} scoped entities and {} guest load entities " +
                        "from repository service.", () -> scopedTopologyEntityDTOs.size(),
            () -> guestLoadTopologyEntityDTOs.size());

        // retrieve the relationship between group scoped entity DTO and guest load entity DTO, and
        // wrap the result into GroupScopedEntity
        final List<GroupScopedEntity> groupScopedEntities = constructGroupScopedEntities(
                guestLoadTopologyEntityDTOs,
                scopedTopologyEntityDTOs.stream()
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Functions.identity())));

        final List<PropertyValueList> propertyValueLists = Lists.newArrayList();
        // iterate over entities in the group and add their properties to the group scope
        // account value.
        for (GroupScopedEntity nxtEntity : groupScopedEntities) {
            final PropertyValueList.Builder propList = PropertyValueList.newBuilder();
            boolean mandatoryMissing = false;
            for (Pair<EntityPropertyName, Boolean> nextPair : entityPropsPairs) {
                Optional<String> propertyValue = GroupScopePropertyExtractor
                        .extractEntityProperty(nextPair.first, nxtEntity);
                if (propertyValue.isPresent()) {
                    propList.addValue(propertyValue.get());
                    logger.debug("Property extracted: {}", propertyValue.get());
                } else {
                    propList.addValue("");
                    if (nextPair.second) {
                        logger.error("Mandatory property {} does not exist in entity."
                                        + " Skipping group scope property extraction for entity {}",
                                nextPair.first.name(),
                                nxtEntity.getTopologyEntityDTO().getDisplayName());
                        mandatoryMissing = true;
                        break;
                    }
                }
            }
            if (propList.getValueCount() > 0 && !mandatoryMissing) {
                propertyValueLists.add(propList.build());
            }
        }
        return accountVal.toBuilder().addAllGroupScopePropertyValues(propertyValueLists).build();
    }

    /**
     * Function to get all the scoped topology entity DTOs based on the given scoped entity DTO
     * OIDs.
     *
     * @param scopedEntitiesOids the scoped entity OIDs
     * @return list of scoped topology entity DTOs
     */
    @Nonnull
    private List<TopologyEntityDTO> retrieveScopedTopologyEntityDTOs(
            @Nonnull final Collection<Long> scopedEntitiesOids) {
        if (scopedEntitiesOids.isEmpty()) {
            // return empty list immediately since repository returns all entities for empty oids
            return Collections.emptyList();
        }
        // build search group scoped entities request
        final SearchEntitiesRequest.Builder searchTopologyRequest = SearchEntitiesRequest.newBuilder()
            .setReturnType(Type.FULL)
            .addAllEntityOid(scopedEntitiesOids);

        try {
            return RepositoryDTOUtil.topologyEntityStream(searchService.searchEntitiesStream(
                searchTopologyRequest.build()))
                .map(PartialEntity::getFullEntity)
                .collect(Collectors.toList());
        } catch (StatusRuntimeException e) {
            logger.error("Unable to fetch entities {} from repository", scopedEntitiesOids, e);
            return Collections.emptyList();
        }
    }

    /**
     * Function to get all the guest load topology entity DTOs based on the given scoped entity DTO
     * OIDs. The start points are the given group scoped entities and they should be VMs, then we
     * traverse to the application entities which consumes from the given VMs and retrieve all
     * of them as the result.
     *
     * NOTICE that we retrieve ALL applications and identify the guestload by the fact that it is
     * discovered by a hypervisor or cloud target.
     *
     * @param groupScopedEntitiesOids the group scoped entity OIDs, we use them as the starting points
     * @return list of guest load topology entity DTOs
     */
    @Nonnull
    private List<TopologyEntityDTO> retrieveGuestLoadTopologyEntityDTOs(
            @Nonnull final Collection<Long> groupScopedEntitiesOids) {
        if (groupScopedEntitiesOids.isEmpty()) {
            // return empty list immediately since repository returns all entities for empty oids
            return Collections.emptyList();
        }
        final SearchEntitiesRequest.Builder searchTopologyRequest = SearchEntitiesRequest.newBuilder()
            .setReturnType(Type.FULL)
            .addSearchParameters(SearchParameters.newBuilder()
                .setStartingFilter(SearchProtoUtil.idFilter(groupScopedEntitiesOids))
                .addSearchFilter(SearchFilter.newBuilder()
                    .setTraversalFilter(TraversalFilter.newBuilder()
                        .setTraversalDirection(TraversalDirection.PRODUCES)
                        .setStoppingCondition(StoppingCondition.newBuilder()
                            .setStoppingPropertyFilter(SearchProtoUtil.entityTypeFilter(EntityType.APPLICATION_COMPONENT.getNumber())))
                    )));
        try {
            return RepositoryDTOUtil.topologyEntityStream(searchService.searchEntitiesStream(
                searchTopologyRequest.build()))
                .map(PartialEntity::getFullEntity)
                .collect(Collectors.toList());
        } catch (StatusRuntimeException e) {
            logger.error("Unable to fetch GuestLoad apps for entities {} from repository",
                groupScopedEntitiesOids, e);
            return Collections.emptyList();
        }
    }

    /**
     * Construct {@link GroupScopedEntity} with the {@link TopologyEntityDTO} of scoped
     * entity and guest load OID.
     *
     * @param guestLoadTopologyEntities all the returned guest load entity DTOs
     * @param oidToGroupScopedEntities the map with scoped entity OID to its entity
     * @return list of {@link GroupScopedEntity}
     */
    @Nonnull
    private List<GroupScopedEntity> constructGroupScopedEntities(
            @Nonnull final List<TopologyEntityDTO> guestLoadTopologyEntities,
            @Nonnull final Map<Long, TopologyEntityDTO> oidToGroupScopedEntities) {
        final Map<TopologyEntityDTO, TopologyEntityDTO> providerToGuestLoad = Maps.newHashMap();
        // retrieve relationship between group scoped entities and guest load entities
        retrieveProviderToGuestLoadRelationship(providerToGuestLoad, guestLoadTopologyEntities,
                oidToGroupScopedEntities);

        // construct the GroupScopedEntity list
        final List<GroupScopedEntity> groupScopedEntities = Lists.newArrayList();
        oidToGroupScopedEntities.values().stream().forEach(scopedEntityDTO -> {
            final Optional<String> guestLoadOid = providerToGuestLoad.containsKey(scopedEntityDTO) ?
                    Optional.of(String.valueOf(providerToGuestLoad.get(scopedEntityDTO).getOid())) :
                    Optional.empty();
            final Optional<String> targetAddress = scopedEntityDTO.getOrigin().getDiscoveryOrigin()
                .getDiscoveredTargetDataMap().keySet().stream()
                .findAny()
                .flatMap(targetStore::getTargetDisplayName);
            final Optional<String> localName = entityStore.chooseEntityDTO(scopedEntityDTO.getOid())
                .getEntityPropertiesList().stream()
                .filter(entityProperty -> SDKUtil.DEFAULT_NAMESPACE.equals(entityProperty.getNamespace()))
                .filter(entityProperty -> SupplyChainConstants.LOCAL_NAME.equals(entityProperty.getName()))
                .map(EntityProperty::getValue)
                .findAny();
            groupScopedEntities.add(new GroupScopedEntity(scopedEntityDTO, guestLoadOid,
                targetAddress, localName));
        });
        return groupScopedEntities;
    }

    /**
     * The method to retrieve relationship between group scoped entities and guest load entities.
     * We iterate all the returned application entity DTOs to find if there is matched provider OID,
     * and compare the discovery origin with provider to make sure they are from the same target.
     *
     * @param providerToGuestLoad the map we need to build in the method to cache the relationship
     * @param guestLoadTopologyEntities all the returned guest load entity DTOs
     * @param oidToGroupScopedEntities the map with scoped entity OID to its entity
     */
    private void retrieveProviderToGuestLoadRelationship(
            @Nonnull final Map<TopologyEntityDTO, TopologyEntityDTO> providerToGuestLoad,
            @Nonnull final List<TopologyEntityDTO> guestLoadTopologyEntities,
            @Nonnull final Map<Long, TopologyEntityDTO> oidToGroupScopedEntities) {
        guestLoadTopologyEntities.stream().forEach(guestLoadEntityDTO -> {
            final List<Long> providerOids = guestLoadEntityDTO
                    .getCommoditiesBoughtFromProvidersList().stream()
                    .map(CommoditiesBoughtFromProvider::getProviderId)
                    .collect(Collectors.toList());
            // since an application can have multiple providers, we need to find the matched
            // one.
            providerOids.forEach(providerOid -> {
                TopologyEntityDTO provider = oidToGroupScopedEntities.get(providerOid);
                if (provider == null) {
                    // should not happen
                    logger.error("No provider entity mapped for oid {}, pairing with guest " +
                                    "load entity {} will be skipped.", providerOid,
                            guestLoadEntityDTO.getDisplayName());
                    return;
                }
                // the guest load entity DTO should be discovered by same probe which in the set
                // guestLoadOriginProbeCategories
                final boolean guestLoadHasOrigin = guestLoadEntityDTO.hasOrigin() &&
                        guestLoadEntityDTO.getOrigin().hasDiscoveryOrigin();
                final boolean providerHasOrigin = provider.hasOrigin() &&
                        provider.getOrigin().hasDiscoveryOrigin();
                if (guestLoadHasOrigin && providerHasOrigin) {
                    Set<Long> guestLoadTargetIds = guestLoadEntityDTO.getOrigin().getDiscoveryOrigin()
                            .getDiscoveredTargetDataMap().keySet();
                    Set<Long> providerTargetIds = provider.getOrigin().getDiscoveryOrigin()
                            .getDiscoveredTargetDataMap().keySet();
                    Set<Long> targetsForBoth = Sets.intersection(guestLoadTargetIds, providerTargetIds);
                    if (!targetsForBoth.isEmpty() && hasValidGuestLoadTarget(targetsForBoth)) {
                        logger.debug("Paired group scope entity {} with the guest load entity {}.",
                                provider.getDisplayName(), guestLoadEntityDTO.getDisplayName());
                        providerToGuestLoad.put(provider, guestLoadEntityDTO);
                    }
                }
            });
        });
    }

    /**
     * The method to check if there is a target belong to {@link guestLoadOriginProbeCategories}
     * using the parameter of target OIDs. The entities discovered from application probe won't
     * pass the check here.
     *
     * @param targetOids The origin target OIDs for both scoped entity and its guest load entity.
     * @return True if there is a target which its probe category in the set
     * {@link guestLoadOriginProbeCategories}.
     */
    private boolean hasValidGuestLoadTarget(@Nonnull final Collection<Long> targetOids) {
        return targetOids.stream().anyMatch(targetOid -> {
            final Optional<SDKProbeType> probeType = targetStore.getProbeTypeForTarget(targetOid);
            if (!probeType.isPresent()) {
                logger.error("No target found for target OID {}.", targetOid);
                return false;
            }
            return guestLoadOriginProbeCategories.contains(probeType.get().getProbeCategory());
        });
    }

    /**
     * Take a list of {@link GroupScopeProperty} and return a {@link List} of {@link Pair} where
     * each Pair consists of an {@link EntityPropertyName} and a boolean indicating whether or not
     * that property is mandatory.  Note that this is an ordered list.  The server sends the
     * group scope property values as a list of strings which must be in the order that the probe
     * requested them in in the CustomAccountDefEntry it used to specify the GroupScope.
     *
     * @param rawProperties the {@link List} of {@link GroupScopeProperty} to traverse.
     * @return {@link List} of {@link Pair} giving {@link EntityPropertyName} and a flag indicating
     * whether the property is mandatory.
     */
    private List<Pair<EntityPropertyName, Boolean>> getGroupScopePropertyNames(
            @Nonnull final List<GroupScopeProperty> rawProperties) {
        return Objects.requireNonNull(rawProperties).stream()
                .map(property -> { try {
                    return new Pair<>(EntityPropertyName.valueOf(property.getPropertyName()),
                            property.getIsMandatory());
                } catch (IllegalArgumentException ex) {
                    logger.error("Unknown entity property field in group scope: {}",
                            property);
                    return null;
                }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }


    /**
     * Wrapper class for {@link TopologyEntityDTO}, and its guest load OID.
     */
    public static class GroupScopedEntity {

        private final TopologyEntityDTO topologyEntityDTO;

        private final Optional<String> guestLoadEntityOid;

        private final Optional<String> targetAddress;

        private final Optional<String> localName;

        public GroupScopedEntity(@Nonnull final TopologyEntityDTO topologyEntityDTO,
                                 @Nonnull final Optional<String> guestLoadEntityOid,
                                 @Nonnull final Optional<String> targetAddress,
                                 @Nonnull final Optional<String> localName) {
            this.topologyEntityDTO = topologyEntityDTO;
            this.guestLoadEntityOid = guestLoadEntityOid;
            this.targetAddress = targetAddress;
            this.localName = localName;
        }

        public @Nonnull TopologyEntityDTO getTopologyEntityDTO() {
            return this.topologyEntityDTO;
        }

        public @Nonnull Optional<String> getGuestLoadEntityOid() {
            return this.guestLoadEntityOid;
        }

        @Nonnull
        public Optional<String> getTargetAddress() {
            return targetAddress;
        }

        @Nonnull
        public Optional<String> getLocalName() {
            return localName;
        }
    }
}
