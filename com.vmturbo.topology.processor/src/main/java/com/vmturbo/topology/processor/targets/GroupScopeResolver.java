package com.vmturbo.topology.processor.targets;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesResponse;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.Pair;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.GroupScopeProperty;
import com.vmturbo.platform.sdk.common.EntityPropertyName;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue.PropertyValueList;

/**
 * Utility class for extracting group scope information from a probe's account definition list
 * and then populating the account values for each target with the properties defined in the
 * account definition list.
 */
public class GroupScopeResolver {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Group service stub for getting group memebership from Group Service.
     */
    private final GroupServiceBlockingStub groupService;

    /**
     * Repository Service stub used for getting enitities whose properties need to be extracted and
     * put into group scope.
     */
    private final RepositoryServiceBlockingStub repositoryRpcService;

    /**
     * Context id pointing to the realtime market.  Passed in as a parameter to repository service.
     */
    private final long realtimeTopologyContextId;

    /**
     * Constructor of a GroupScoperResolver.
     *
     * @param groupChannel Channel to use for creating a blocking stub to query the Group Service.
     * @param repositoryChannel Channel to use for creating a blocking stub to query the
     *                          Repository Service.
     * @param realtimeTopologyContextId Context ID for the realtime market.  Needed for constructing
     *                                  Repository Service Queries.
     */
    public GroupScopeResolver(@Nonnull final Channel groupChannel,
                              @Nonnull final Channel repositoryChannel,
                              @Nonnull final Long realtimeTopologyContextId) {
        this.groupService = GroupServiceGrpc.newBlockingStub(Objects.requireNonNull(groupChannel));
        this.repositoryRpcService = RepositoryServiceGrpc.newBlockingStub(
                Objects.requireNonNull(repositoryChannel));
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    private Map<String, CustomAccountDefEntry> generateGroupScopeMap(@Nonnull ProbeInfo probeInfo) {
        Objects.requireNonNull(probeInfo);
        return probeInfo.getAccountDefinitionList().stream()
                .filter(AccountDefEntry::hasCustomDefinition)
                .map(AccountDefEntry::getCustomDefinition)
                .filter(CustomAccountDefEntry::hasGroupScope)
                .collect(Collectors.toMap(CustomAccountDefEntry::getName, Function.identity()));

    }

    /**
     * Take a collection of account values and probe info for a probe and return a collection of
     * account values with any group scope account values properly populated.
     *
     * @param newAccountValues a collection of {@link AccountValue} that needs to have its group
     *                         scope account values populated.
     * @param probe the {@link ProbeInfo} that contains the group scope definitions for the probe
     *              associated with newAccountValues.
     * @return {@link Collection} of {@link AccountValue} where group scopes have been populated
     * with values.
     */
    public Collection<AccountValue> processGroupScope(
            @Nonnull Collection<AccountValue> newAccountValues,
            @Nonnull ProbeInfo probe) {
        Map<String, CustomAccountDefEntry> keyToGroupScopeMap = generateGroupScopeMap(probe);
        return newAccountValues.stream()
                .map(accountValue -> keyToGroupScopeMap.keySet().contains(accountValue.getKey()) ?
                        populatePropertyValueList(keyToGroupScopeMap.get(accountValue.getKey()),
                                accountValue)
                        : accountValue)
                .collect(Collectors.toList());
    }

    /**
     * If there is a group scope corresponding to this account value, populate the requested values
     * into the {@link AccountValue} that is passed in and return it.  If there is no Group Scope
     * in the {@link CustomAccountDefEntry} just return the {@link AccountValue} as is.
     *
     * @param customAcctDef the {@link CustomAccountDefEntry} corresponding to accountVal
     * @param accountVal the {@link AccountValue} to populate with group scope values if
     *                   necessary
     * @return either the original {@link AccountValue} or the {@link AccountValue} populated with
     * values extracted from the group scope
     */
    private AccountValue populatePropertyValueList(@Nonnull CustomAccountDefEntry customAcctDef,
                                                   AccountValue accountVal) {
        Objects.requireNonNull(customAcctDef);
        String groupId = accountVal.getStringValue();
        GetMembersResponse membersResponse = groupService.getMembers(GetMembersRequest.newBuilder()
                .setId(Long.parseLong(groupId))
                .setExpectPresent(true)
                .build());
        if (!membersResponse.hasMembers()) {
            logger.warn("Group {} has no members.  "
                    + "No property values will be returned for group scope.", groupId);
            return accountVal;
        }
        logger.debug("Group {} has members {} in group scope processing.",
                groupId, membersResponse.getMembers());
        // need to check if entityType of group is same as entityType of accountDef
        GetGroupResponse groupResponse =
                groupService.getGroup(GroupID.newBuilder().setId(Long.parseLong(groupId)).build());
        if (customAcctDef.getGroupScope().getEntityType().getNumber() !=
                groupResponse.getGroup().getGroup().getEntityType()) {
            logger.error("Group {} contains the wrong entity type for group scope.  "
                            + "Expected type {}, but got type {}",
                    customAcctDef.getGroupScope().getEntityType(),
                    groupResponse.getGroup().getGroup().getEntityType());
            return accountVal;
        }
        logger.debug("Group type matches group scope type.");
        List<Pair<EntityPropertyName, Boolean>> entityPropsPairs =
                getGroupScopePropertyNames(
                        customAcctDef.getGroupScope().getPropertyList());
        final RetrieveTopologyEntitiesRequest.Builder retrieveTopoEntitiesBuilder =
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .addAllEntityOids(membersResponse.getMembers().getIdsList())
                        .setTopologyType(TopologyType.SOURCE)
                        .setTopologyContextId(realtimeTopologyContextId);
        RetrieveTopologyEntitiesResponse topoEntitiesResponse =
                repositoryRpcService
                        .retrieveTopologyEntities(retrieveTopoEntitiesBuilder.build());
        logger.debug("Retrieved {} entities from repository service.",
                topoEntitiesResponse.getEntitiesList().size());
        List<PropertyValueList> propertyValueLists = Lists.newArrayList();
        // iterate over entities in the group and add their properties to the group scope
        // account value.
        for (TopologyEntityDTO nxtEntity : topoEntitiesResponse.getEntitiesList()) {
            final PropertyValueList.Builder propList = PropertyValueList.newBuilder();
            boolean mandatoryMissing = false;
            for (Pair<EntityPropertyName, Boolean> nextPair : entityPropsPairs) {
                Optional<String> propertyValue = GroupScopePropertyExtractor
                        .extractEntityProperty(nextPair.first, nxtEntity);
                if (propertyValue.isPresent()) {
                    propList.addValue(propertyValue.get());
                    logger.debug("Property extracted: {}", propertyValue.get());
                } else if (nextPair.second) {
                    logger.error("Mandatory property {} does not exist in entity."
                                    + " Skipping group scope property extraction for entity {}",
                            nextPair.first.name(),
                            nxtEntity.getDisplayName());
                    mandatoryMissing = true;
                    break;
                }
            }
            if (propList.getValueCount() > 0 && !mandatoryMissing) {
                propertyValueLists.add(propList.build());
            }
        }
        return accountVal.toBuilder().addAllGroupScopePropertyValues(propertyValueLists).build();
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
}
