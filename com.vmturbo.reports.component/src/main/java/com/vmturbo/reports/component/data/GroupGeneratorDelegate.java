package com.vmturbo.reports.component.data;

import static com.vmturbo.components.common.utils.StringConstants.PHYSICAL_MACHINE;
import static com.vmturbo.components.common.utils.StringConstants.STORAGE;
import static com.vmturbo.components.common.utils.StringConstants.VIRTUAL_MACHINE;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import javaslang.Tuple2;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.CreateTempGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.TempGroupInfo;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.reports.component.data.ReportDataUtils.EntitiesTableGeneratedId;
import com.vmturbo.reports.component.data.ReportDataUtils.MetaGroup;
import com.vmturbo.reports.component.instances.ReportsGenerator;
import com.vmturbo.sql.utils.DbException;

/**
 * A delegate class to populate common group relationships:
 * 1. VM group relationships
 * 2. PM group relationships
 * 3. Storage group relationships
 * TODO: cache data to optimize performance
 */
public class GroupGeneratorDelegate {
    private static final Logger logger = LogManager.getLogger();
    public static final String FAKE_VM_GROUP = "fake_vm_group";

    /**
     * Insert compute clusters and their members to database.
     *
     * @param context report context which contains clients to external services
     * @throws DbException if DB related exception is thrown
     */
    public synchronized void insertVMClusterRelationships(@Nonnull final ReportsDataContext context) throws DbException {
        logger.info("Generating VM cluster relationships");
        insertClusterRelationshipsInternal(context, ClusterInfo.Type.COMPUTE, MetaGroup.VMs, VIRTUAL_MACHINE);
        logger.info("Finished generating VM cluster relationships");
    }

    /**
     * Insert system discovered or user created VM group and their members to database.
     * It will return user selected group's UUID from entities table which will be used
     * in report generation..
     *
     * @param context report context which contains clients to external services
     * @return user selected VM group's UUID from entities table.
     * @throws DbException if DB related exception is thrown
     */
    public synchronized Optional<String> insertVMGroupRelationships(@Nonnull final ReportsDataContext context,
                                                                    long id) throws DbException {
        logger.info("Generating VM group relationships for group id {}", id);
        final GetGroupResponse groupResponse = context.getGroupService()
            .getGroup(GroupID.newBuilder().setId(id).build());
        if (groupResponse.hasGroup()) {
            return writeGroupToDB(context, MetaGroup.VMs, VIRTUAL_MACHINE, groupResponse.getGroup());
        }
        logger.info("Finished generating VM group relationships");
        return Optional.empty();

    }

    /**
     * Insert system discovered or user created PM group and their members to database.
     * It will return user selected PM group's UUID from entities table which will be used
     * in report generation.
     *
     * @param context report context which contains clients to external services.
     * @param groupId the groupId user selected.
     * @return user selected PM group's UUID from entities table.
     * @throws DbException if DB related exception is thrown.
     */
    public synchronized Optional<String> insertPMGroupRelationships(@Nonnull final ReportsDataContext context,
                                                                    long groupId) throws DbException {
        logger.info("Generating PM group relationships for group id {}", groupId);
        final GetGroupResponse groupResponse = context.getGroupService()
            .getGroup(GroupID.newBuilder().setId(groupId).build());
        if (groupResponse.hasGroup()) {
            return writeGroupToDB(context, MetaGroup.PMs, PHYSICAL_MACHINE, groupResponse.getGroup());
        }
        logger.info("Finished generating PM group relationships");
        return Optional.empty();

    }


    /**
     * Insert system discovered or user created PM group and their members to database.
     * Also insert the same PM group and their VM members to database using arbitrary group name:
     * "fake_vm_group". To build a temp (PM) group for the VM members:
     * 1. get PM members
     * 2. for each PM member retrieve VM members
     * 3. create a temp group with all the VM members and name "fake_vm_group".
     * It will return user selected PM group's UUID from entities table which will be used
     * in report generation. The "fake_vm_group" is used in {@link ReportsGenerator#prepareReportAttributes}.
     * @param context report context which contains clients to external services.
     * @param groupId the groupId user selected.
     * @throws DbException if DB related exception is thrown.
     */
    public synchronized void insertPMGroupAndVMRelationships(@Nonnull final ReportsDataContext context,
                                                             @Nonnull final long groupId) throws DbException {
        logger.info("Generating PM group relationships for group id {}", groupId);
        final GetGroupResponse groupResponse = context.getGroupService()
            .getGroup(GroupID.newBuilder().setId(groupId).build());
        if (groupResponse.hasGroup()) {
            final Group group = groupResponse.getGroup();
            final Set<Long> allMemberVMIds =
                getSupplyChainNodes(context, VIRTUAL_MACHINE, getEntitiesInScope(context, group))
                    .stream().flatMap(node -> RepositoryDTOUtil.getAllMemberOids(node).stream())
                .collect(Collectors.toSet());
            final TempGroupInfo tempGroupInfo = TempGroupInfo.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setMembers(StaticGroupMembers.newBuilder()
                    .addAllStaticMemberOids(allMemberVMIds))
                .setName(MetaGroup.FAKE_VM_GROUP.name().toLowerCase())
                .setIsGlobalScopeGroup(false)
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
                .build();
            final CreateTempGroupResponse response = context.getGroupService().createTempGroup(
                CreateTempGroupRequest.newBuilder()
                    .setGroupInfo(tempGroupInfo)
                    .build());
            if (response.hasGroup()) {
                writeGroupToDB(context, MetaGroup.VMs, VIRTUAL_MACHINE, response.getGroup());
            }
        }
        logger.info("Finished generating PM group relationships");
    }

    /**
     * Insert PM and their members to database. It will create temp group
     * @param context report context which contains clients to external services.
     * @param pmId PM oid
     * @return user selected PM UUID from entities table.
     * @throws DbException if DB related exception is thrown.
     */
    public synchronized Optional<String> insertPMVMsRelationships(@Nonnull final ReportsDataContext context,
                                                                  long pmId) throws DbException {
        final TempGroupInfo tempGroupInfo = createTemporaryPmGroupInfo(context, pmId);
        final CreateTempGroupResponse response = context.getGroupService().createTempGroup(
            CreateTempGroupRequest.newBuilder()
                .setGroupInfo(tempGroupInfo)
                .build());
        if (response.hasGroup()) {
            return writeGroupToDB(context, MetaGroup.VMs, VIRTUAL_MACHINE, response.getGroup());
        }
        return Optional.empty();
    }

    // create a temporary PM group with VM members
    private TempGroupInfo createTemporaryPmGroupInfo(final ReportsDataContext context, final long id) {
        final Set<Long> groupMembers =
            getSupplyChainNodes(context, VIRTUAL_MACHINE, Collections.singleton(id)).stream().flatMap(
                node -> RepositoryDTOUtil.getAllMemberOids(node).stream()
            ).collect(Collectors.toSet());
        return TempGroupInfo.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setMembers(StaticGroupMembers.newBuilder()
                .addAllStaticMemberOids(groupMembers))
            .setName(MetaGroup.FAKE_VM_GROUP.name().toLowerCase())
            .setIsGlobalScopeGroup(false)
            .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
            .build();
    }

    /**
     * Insert compute clusters and their members to database.
     *
     * @param context report context which contains clients to external services
     * @throws DbException if DB related exception is thrown
     */
    public synchronized void insertPMClusterRelationships(@Nonnull final ReportsDataContext context) throws DbException {
        logger.info("Generating PM cluster relationships");
        insertClusterRelationshipsInternal(context, ClusterInfo.Type.COMPUTE, MetaGroup.PMs, PHYSICAL_MACHINE);
        logger.info("Finished generating PM cluster relationships");
    }

    /**
     * Insert Storage clusters and their members to database.
     * Note: it seems not currently used in classis reports.
     *
     * @param context report context which contains clients to external services
     * @throws DbException if DB related exception is thrown
     */
    public synchronized void insertStorageClusterRelationships(@Nonnull final ReportsDataContext context) throws DbException {
        logger.info("Generating Storage cluster relationships");
        insertClusterRelationshipsInternal(context, ClusterInfo.Type.STORAGE, MetaGroup.Storages, STORAGE);
        logger.info("Finished generating Storage cluster relationships");
    }


    // insert groups and their members relationships to entity_assns_members table.
    private void insertClusterMembersRelationships(@Nonnull final ReportsDataContext context,
                                                   @Nonnull final Map<Group, Long> groupToPK,
                                                   @Nonnull final Long defaultGroupId,
                                                   @Nonnull final String entityType)
        throws DbException {
        for (Map.Entry<Group, Long> entry : groupToPK.entrySet()) {
            final List<Long> staticMemberOidsList = entry.getKey().getCluster().getMembers().getStaticMemberOidsList();
            final List<SupplyChainNode> nodes = getSupplyChainNodes(context, entityType, staticMemberOidsList);
            for (SupplyChainNode node : nodes) {
                final Set<Long> entitiesInScope = RepositoryDTOUtil.getAllMemberOids(node);
                final List<Long> groupEntityIds = Lists.newArrayList(entitiesInScope);
                context.getReportDataWriter().cleanUpEntity_Assns(groupEntityIds);
                context.getReportDataWriter().insertEntityAssnsBatch(groupEntityIds);
                final Map<Long, Set<Long>> groupMembers = new HashMap<>();
                // we want the primary key, not the group id
                groupMembers.put(entry.getValue(), entitiesInScope);
                // we always need to put the VMs to the special VM group
                groupMembers.put(defaultGroupId, entitiesInScope);
                context.getReportDataWriter().insertEntityAssnsMembersEntities(groupMembers);
            }
        }
    }

    private List<SupplyChainNode> getSupplyChainNodes(final @Nonnull ReportsDataContext context,
                                                      final @Nonnull String entityType,
                                                      final @Nonnull Collection<Long> staticMemberOidsList) {
        final GetSupplyChainRequest request = GetSupplyChainRequest.newBuilder()
            .setEnvironmentType(EnvironmentType.ON_PREM)
            .addAllStartingEntityOid(staticMemberOidsList)
            .addAllEntityTypesToInclude(Collections.singleton(entityType))
            .build();
        final GetSupplyChainResponse response = context.getSupplyChainRpcService().getSupplyChain(request);
        return response.getSupplyChain().getSupplyChainNodesList();
    }


    // 1. Insert ENTITIES table with group names.
    // 2. Get members from the clusters
    // 3. Insert groups and their members to ENTITY_ASSNS table with name "AllGroupMembers"
    // 4. Insert cluster id -> members to ENTITY_ASSNS_MEMBERS_ENTITIES table
    // 5. Insert cluster id with entity type and "sETypeName"
    private void insertClusterRelationshipsInternal(@Nonnull final ReportsDataContext context,
                                                    @Nonnull final ClusterInfo.Type type,
                                                    @Nonnull final MetaGroup metaGroup,
                                                    @Nonnull final String entityType) throws DbException {
        final Iterable<Group> groups = () -> context
            .getGroupService()
            .getGroups(GetGroupsRequest.newBuilder()
                .addTypeFilter(Type.CLUSTER)
                .setClusterFilter(ClusterFilter.newBuilder()
                    .setTypeFilter(type)
                    .build())
                .build());
        final List<Group> groupList = StreamSupport.stream(groups.spliterator(), false)
            .filter(group -> !(group.hasGroup() && group.getGroup().getIsHidden()))
            .filter(group -> group.getCluster().getMembers().getStaticMemberOidsCount() > 0)
            .collect(Collectors.toList());
        final EntitiesTableGeneratedId groupResults = context.getReportDataWriter().insertGroupIntoEntitiesTable(groupList, metaGroup);
        writeGroupRelationships(context, entityType, groupResults);
    }

    /**
     * Write group relationship to DB. It will return user selected group's UUID from entities table.
     */
    private Optional<String> writeGroupToDB(@Nonnull final ReportsDataContext context,
                                            @Nonnull final MetaGroup metaGroup,
                                            @Nonnull final String entityType,
                                            @Nonnull final Group group) throws DbException {
        final Tuple2<EntitiesTableGeneratedId, Optional<String>> resultsTuple =
            context.getReportDataWriter().insertGroup(group, metaGroup);
        final EntitiesTableGeneratedId results = resultsTuple._1;
        final Map<Group, Long> groupToPK = results.getGroupToPK();
        final List<Long> allGeneratedGroupIdsFromEntitiesTable = groupToPK.values().stream().collect(Collectors.toList());
        allGeneratedGroupIdsFromEntitiesTable.add(results.getDefaultGroupPK());
        context.getReportDataWriter().cleanUpEntity_Assns(allGeneratedGroupIdsFromEntitiesTable);
        final EntitiesTableGeneratedId resultsFromEntityAssns = context.getReportDataWriter().insertEntityAssns(results);
        // TODO move group related logics to Group api component.
        final Set<Long> entitiesInScope = getEntitiesInScope(context, group);
        context.getReportDataWriter().cleanUpEntity_Assns(Lists.newArrayList(entitiesInScope));
        context.getReportDataWriter().insertEntityAssnsBatch(Lists.newArrayList(entitiesInScope));
        for (Map.Entry<Group, Long> entry : resultsFromEntityAssns.getGroupToPK().entrySet()) {
            final Map<Long, Set<Long>> groupMembers = new HashMap<>();
            // we want the primary key, not the group id
            groupMembers.put(entry.getValue(), entitiesInScope);
            // we always need to put the VMs to the special VM group
            groupMembers.put(resultsFromEntityAssns.getDefaultGroupPK(), entitiesInScope);
            context.getReportDataWriter().insertEntityAssnsMembersEntities(groupMembers);
        }
        final List<Long> allGroupsFromEntityAssnsTable =
            resultsFromEntityAssns.getGroupToPK().values().stream().collect(Collectors.toList());
        allGroupsFromEntityAssnsTable.add(resultsFromEntityAssns.getDefaultGroupPK());
        context.getReportDataWriter().insertEntityAttrs(allGeneratedGroupIdsFromEntitiesTable, entityType);
        return resultsTuple._2;
    }

    private Set<Long> getEntitiesInScope(final @Nonnull ReportsDataContext context, final @Nonnull Group group) {
        return GroupProtoUtil.getStaticMembers(group).map(members -> members.stream().collect(Collectors.toSet()))
            .orElse(ReportDataUtils.expandOids(Collections.singleton(group.getId()), context.getGroupService()));
    }

    private void writeGroupRelationships(@Nonnull final ReportsDataContext context,
                                         @Nonnull final String entityType,
                                         @Nonnull final EntitiesTableGeneratedId results) throws DbException {
        final Map<Group, Long> groupToPK = results.getGroupToPK();
        final List<Long> allGroupsFromEntitiesTable = groupToPK.values().stream().collect(Collectors.toList());
        allGroupsFromEntitiesTable.add(results.getDefaultGroupPK());
        context.getReportDataWriter().cleanUpEntity_Assns(allGroupsFromEntitiesTable);
        final EntitiesTableGeneratedId resultsFromEntityAssns = context.getReportDataWriter().insertEntityAssns(results);
        insertClusterMembersRelationships(context, resultsFromEntityAssns.getGroupToPK(),
            resultsFromEntityAssns.getDefaultGroupPK(), entityType);
        final List<Long> allGroupsFromEntityAssnsTable =
            resultsFromEntityAssns.getGroupToPK().values().stream().collect(Collectors.toList());
        allGroupsFromEntityAssnsTable.add(resultsFromEntityAssns.getDefaultGroupPK());
        context.getReportDataWriter().insertEntityAttrs(allGroupsFromEntitiesTable, entityType);
    }
}
