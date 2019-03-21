package com.vmturbo.reports.component.data;

import static com.vmturbo.components.common.utils.StringConstants.GROUP;
import static com.vmturbo.components.common.utils.StringConstants.STORAGE;
import static com.vmturbo.components.common.utils.StringConstants.VIRTUAL_MACHINE;
import static com.vmturbo.history.schema.abstraction.tables.Entities.ENTITIES;
import static com.vmturbo.history.schema.abstraction.tables.EntityAssns.ENTITY_ASSNS;
import static com.vmturbo.history.schema.abstraction.tables.EntityAssnsMembersEntities.ENTITY_ASSNS_MEMBERS_ENTITIES;
import static com.vmturbo.history.schema.abstraction.tables.EntityAttrs.ENTITY_ATTRS;
import static com.vmturbo.reports.component.data.ReportDBDataWriter.PMS;
import static com.vmturbo.reports.component.data.ReportDBDataWriter.RIGHTSIZING_INFO;
import static com.vmturbo.reports.component.data.ReportDBDataWriter.STATIC_META_GROUP;
import static com.vmturbo.reports.component.data.ReportDBDataWriter.VMS;
import static org.jooq.impl.DSL.using;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.history.schema.abstraction.tables.records.EntitiesRecord;
import com.vmturbo.history.schema.abstraction.tables.records.EntityAssnsMembersEntitiesRecord;
import com.vmturbo.history.schema.abstraction.tables.records.EntityAssnsRecord;
import com.vmturbo.history.schema.abstraction.tables.records.EntityAttrsRecord;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc;
import com.vmturbo.reports.component.ReportingTestConfig;
import com.vmturbo.reports.component.data.ReportDataUtils.MetaGroup;
import com.vmturbo.reports.component.data.ReportDataUtils.Results;
import com.vmturbo.sql.utils.DbException;

/**
 * Tests for CRUD operations with Reports schedules in db.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class,
    classes = {ReportingTestConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class ReportDBDataWriterTest {
    public static final String DISPLAY_NAME = "Test";
    public static final String NAME = "domain-c26";
    ReportDBDataWriter reportDBDataWriter;
    @Autowired
    private ReportingTestConfig reportingConfig;
    private ReportingServiceGrpc.ReportingServiceBlockingStub reportingService;
    private DSLContext dslContext;

    @Before
    public void init() throws Exception {
        dslContext = reportingConfig.dslContext();
        reportDBDataWriter = new ReportDBDataWriter(dslContext);
    }

    @After
    public void cleanup() {
        dslContext.transaction(transaction -> {
            final DSLContext transactionContext = using(transaction);
            // Clean up rows with creation class as "Group" and "StaticMetaGroup".
            transactionContext.deleteFrom(ENTITIES).where(ENTITIES.CREATION_CLASS.eq(GROUP)
                .or(ENTITIES.CREATION_CLASS.eq(STATIC_META_GROUP))).execute();
        });
    }

    @Test
    public void testInsertGroupsForVMs() throws DbException {
        final Group group = getGroup(DISPLAY_NAME, NAME);
        Results results = reportDBDataWriter.insertGroups(ImmutableList.of(group), MetaGroup.VMs);
        assertNotNull(results.getDefaultGroupPK());
        assertEquals(1, results.getGroupToPK().size());
        Result<EntitiesRecord> resultGroup = dslContext.selectFrom(ENTITIES)
            .where(ENTITIES.CREATION_CLASS.eq(GROUP)).fetch();
        assertEquals(1, resultGroup.size());
        assertEquals(DISPLAY_NAME, resultGroup.getValues(ENTITIES.DISPLAY_NAME).get(0));
        Result<EntitiesRecord> resultStaticGroup = dslContext.selectFrom(ENTITIES)
            .where(ENTITIES.CREATION_CLASS.eq(STATIC_META_GROUP)).fetch();
        assertEquals(1, resultStaticGroup.size());
        assertEquals(VMS, resultStaticGroup.getValues(ENTITIES.DISPLAY_NAME).get(0));
    }

    @Test
    public void testInsertGroupsForVMsAndPMs() throws DbException {
        final Group group1 = getGroup(DISPLAY_NAME, NAME);
        Results resultsVMs = reportDBDataWriter.insertGroups(ImmutableList.of(group1), MetaGroup.VMs);
        Results resultsPMs = reportDBDataWriter.insertGroups(ImmutableList.of(group1), MetaGroup.PMs);
        verifyDBResults(resultsVMs, resultsPMs);
    }

    @Test
    public void testInsertGroupsForPMsAndVMs() throws DbException {
        final Group group1 = getGroup(DISPLAY_NAME, NAME);
        Results resultsPMs = reportDBDataWriter.insertGroups(ImmutableList.of(group1), MetaGroup.PMs);
        Results resultsVMs = reportDBDataWriter.insertGroups(ImmutableList.of(group1), MetaGroup.VMs);
        verifyDBResults(resultsVMs, resultsPMs);
    }


    public void verifyDBResults(final Results resultsVMs, final Results resultsPMs) {
        // verify VMs
        assertNotNull(resultsVMs.getDefaultGroupPK());
        assertEquals(1, resultsVMs.getGroupToPK().size());

        // verify PMs
        assertNotNull(resultsPMs.getDefaultGroupPK());
        assertEquals(1, resultsPMs.getGroupToPK().size());
        Result<EntitiesRecord> resultGroup = dslContext.selectFrom(ENTITIES)
            .where(ENTITIES.CREATION_CLASS.eq(GROUP)).fetch();

        // verify both VMs and PMs
        assertEquals(2, resultGroup.size());
        assertEquals(ImmutableList.of(DISPLAY_NAME, DISPLAY_NAME), resultGroup.getValues(ENTITIES.DISPLAY_NAME));

        Result<EntitiesRecord> resultStaticGroup = dslContext.selectFrom(ENTITIES)
            .where(ENTITIES.CREATION_CLASS.eq(STATIC_META_GROUP)).fetch();
        assertEquals(2, resultStaticGroup.size());
        assertEquals(ImmutableSet.of(PMS, VMS), ImmutableSet.copyOf(resultStaticGroup.getValues(ENTITIES.DISPLAY_NAME)));
    }

    public Group getGroup(final String displayName, final String name) {
        return Group.newBuilder()
            .setCluster(ClusterInfo.newBuilder()
                .setName(name)
                .setDisplayName(displayName)
                .build())
            .build();
    }

    @Test
    public void testInsertGroupsForPMs() throws DbException {
        final Group group = getGroup(DISPLAY_NAME, NAME);
        Results results = reportDBDataWriter.insertGroups(ImmutableList.of(group), MetaGroup.PMs);
        assertNotNull(results.getDefaultGroupPK());
        assertEquals(1, results.getGroupToPK().size());
        Result<EntitiesRecord> resultGroup = dslContext.selectFrom(ENTITIES)
            .where(ENTITIES.CREATION_CLASS.eq(GROUP)).fetch();
        assertEquals(1, resultGroup.size());
        assertEquals(DISPLAY_NAME, resultGroup.getValues(ENTITIES.DISPLAY_NAME).get(0));
        Result<EntitiesRecord> resultStaticGroup = dslContext.selectFrom(ENTITIES)
            .where(ENTITIES.CREATION_CLASS.eq(STATIC_META_GROUP)).fetch();
        assertEquals(1, resultStaticGroup.size());
        assertEquals("PMs", resultStaticGroup.getValues(ENTITIES.DISPLAY_NAME).get(0));
    }

    @Test
    public void testInsertGroupsForStorages() throws DbException {
        final Group group = getGroup(DISPLAY_NAME, NAME);
        Results results = reportDBDataWriter.insertGroups(ImmutableList.of(group), MetaGroup.Storages);
        assertNotNull(results.getDefaultGroupPK());
        assertEquals(1, results.getGroupToPK().size());
        Result<EntitiesRecord> resultGroup = dslContext.selectFrom(ENTITIES)
            .where(ENTITIES.CREATION_CLASS.eq(GROUP)).fetch();
        assertEquals(1, resultGroup.size());
        assertEquals(DISPLAY_NAME, resultGroup.getValues(ENTITIES.DISPLAY_NAME).get(0));
        Result<EntitiesRecord> resultStaticGroup = dslContext.selectFrom(ENTITIES)
            .where(ENTITIES.CREATION_CLASS.eq(STATIC_META_GROUP)).fetch();
        assertEquals(1, resultStaticGroup.size());
        assertEquals(STORAGE, resultStaticGroup.getValues(ENTITIES.DISPLAY_NAME).get(0));
    }

    @Test
    public void testInsertEntityAssns() throws DbException {
        final Group group = getGroup(DISPLAY_NAME, NAME);
        Results results = reportDBDataWriter.insertGroups(ImmutableList.of(group), MetaGroup.VMs);
        reportDBDataWriter.insertEntityAssnsBatch(ImmutableList.of(results.getDefaultGroupPK()));
        Result<EntityAssnsRecord> resultGroup = dslContext.selectFrom(ENTITY_ASSNS)
            .where(ENTITY_ASSNS.ENTITY_ENTITY_ID.eq(results.getDefaultGroupPK())).fetch();
        assertEquals(1, resultGroup.size());
    }


    @Test
    public void testCleanUpEntity_Assns() throws DbException {
        final Group group = getGroup(DISPLAY_NAME, NAME);
        Results results = reportDBDataWriter.insertGroups(ImmutableList.of(group), MetaGroup.VMs);
        List resultList = results.getGroupToPK().values().stream().collect(Collectors.toList());
        resultList.add(results.getDefaultGroupPK());
        reportDBDataWriter.insertEntityAssnsBatch(resultList);
        Result<EntityAssnsRecord> resultGroup = dslContext.selectFrom(ENTITY_ASSNS).fetch();
        assertEquals(2, resultGroup.size());
        // only clean up one row from default group
        reportDBDataWriter.cleanUpEntity_Assns(ImmutableList.of(results.getDefaultGroupPK()));
        resultGroup = dslContext.selectFrom(ENTITY_ASSNS).fetch();
        // show still have one from the group
        assertEquals(1, resultGroup.size());
    }

    @Test
    public void testInsertEntityAttrs() throws DbException {
        final Group group = getGroup(DISPLAY_NAME, NAME);
        Results results = reportDBDataWriter.insertGroups(ImmutableList.of(group), MetaGroup.VMs);
        Long id = results.getGroupToPK().get(group);
        reportDBDataWriter.insertEntityAttrs(ImmutableList.of(id), VIRTUAL_MACHINE);
        Result<EntityAttrsRecord> resultGroup = dslContext.selectFrom(ENTITY_ATTRS)
            .where(ENTITY_ATTRS.ENTITY_ENTITY_ID.eq(id)).fetch();
        assertEquals(1, resultGroup.size());
    }

    @Test
    public void testInsertEntity_Assns() throws DbException {
        final Group group = getGroup(DISPLAY_NAME, NAME);
        Results results = reportDBDataWriter.insertGroups(ImmutableList.of(group), MetaGroup.VMs);
        Long id = results.getGroupToPK().get(group);
        reportDBDataWriter.insertEntityAssns(results);
        Result<EntityAssnsRecord> resultGroup = dslContext.selectFrom(ENTITY_ASSNS)
            .where(ENTITY_ASSNS.ENTITY_ENTITY_ID.eq(id)).fetch();
        assertEquals(1, resultGroup.size());
    }

    @Test
    public void testInsertEntityAssnsMembersEntities() throws DbException {
        final Group group = getGroup(DISPLAY_NAME, NAME);
        final Results results = reportDBDataWriter.insertGroups(ImmutableList.of(group), MetaGroup.VMs);
      //  Long id = results.getGroupToPK().get(group);
        Results newResults = reportDBDataWriter.insertEntityAssns(results);
        // just make up members (pk) that exists in entities table,
        final ImmutableSet<Long> entityIds = ImmutableSet.<Long>builder()
            .addAll(results.getGroupToPK().values())
            .add(results.getDefaultGroupPK())
            .build();
          final ImmutableSet<Long> entityAssnsIds = ImmutableSet.<Long>builder()
              .addAll(newResults.getGroupToPK().values())
              .add(newResults.getDefaultGroupPK())
            .build();

        final Map<Long, Set<Long>> groupMembers = new HashMap<>();
        for (Map.Entry<Group, Long> entry : newResults.getGroupToPK().entrySet()) {
            // we want the primary key, not the group id
            groupMembers.put(entry.getValue(), entityIds);
            // we always need to put the VMs to the special VM group
            groupMembers.put(newResults.getDefaultGroupPK(), entityIds);
            reportDBDataWriter.insertEntityAssnsMembersEntities(groupMembers);
            Result<EntityAssnsMembersEntitiesRecord> resultGroup =
                dslContext.selectFrom(ENTITY_ASSNS_MEMBERS_ENTITIES)
                    .where(ENTITY_ASSNS_MEMBERS_ENTITIES.ENTITY_ASSN_SRC_ID.in(entityAssnsIds)).fetch();
            // two groups and each has two members.
            assertEquals(4, resultGroup.size());
        }
    }

    @Test
    public void testInsertRightSizeActions() throws DbException {
        final Group group = getGroup(DISPLAY_NAME, NAME);
        final Results results = reportDBDataWriter.insertGroups(ImmutableList.of(group), MetaGroup.VMs);
        reportDBDataWriter.insertRightSizeActions(Collections.singletonList(TestHelper
            .resizeActionSpec(results.getDefaultGroupPK())));
        Result<EntityAttrsRecord> resultGroup = dslContext.selectFrom(ENTITY_ATTRS)
            .where(ENTITY_ATTRS.NAME.eq(RIGHTSIZING_INFO)).fetch();
        assertEquals(1, resultGroup.size());
    }

    /**
     * Verify following data are int vmtdb database, they were populated with
     * V1.9__insert_missing_reportdata.sql
     * 1. MarketSettingsManager is in the entities table
     * ('MarketSettingsManager', 'MarketSettingsManager', '_1wq9QTUoEempSo2vlSygbA', 'MarketSettingsManager', NULL);
     * 2. PresentationManager is in the entities table
     * ('PresentationManager', 'PresentationManager', '_zfrLATUoEempSo2vlSygbA', 'PresentationManager', NULL);
     * 3. utilThresholds in entity_attrs table
     * ('utilThreshold_SA', '90.0', @market_settings_manager_id),
     * ('utilThreshold_IOPS', '100.0', @market_settings_manager_id),
     * ('utilThreshold_CPU', '100.0', @market_settings_manager_id),
     * ('utilThreshold_MEM', '100.0', @market_settings_manager_id),
     * ('utilThreshold_IO', '50.0', @market_settings_manager_id),
     * ('utilThreshold_NW', '50.0', @market_settings_manager_id),
     * ('utilThreshold_NW_Switch', '70.0', @market_settings_manager_id),
     * ('utilThreshold_NW_Network', '100.0', @market_settings_manager_id),
     * ('utilThreshold_NW_internet', '100.0', @market_settings_manager_id),
     * ('utilThreshold_SW', '20.0', @market_settings_manager_id),
     * ('utilThreshold_CPU_SC', '100.0', @market_settings_manager_id),
     * ('utilThreshold_LT', '100.0', @market_settings_manager_id),
     * ('utilThreshold_RQ', '50.0', @market_settings_manager_id),
     * ('utilUpperBound_VMEM', '85.0', @market_settings_manager_id),
     * ('utilUpperBound_VCPU', '85.0', @market_settings_manager_id),
     * ('utilLowerBound_VMEM', '10.0', @market_settings_manager_id),
     * ('utilLowerBound_VCPU', '10.0', @market_settings_manager_id),
     * ('utilUpperBound_VStorage', '85.0', @market_settings_manager_id),
     * ('utilLowerBound_VStorage', '10.0', @market_settings_manager_id),
     * ('utilTarget', '70.0', @market_settings_manager_id),
     * 4. currencySetting in entity_attrs table
     * ('currencySetting', '$', @presentation_manager_id);
     *
     * @throws DbException
     */
    @Test
    public void verifyPrerequisites() throws DbException {
        Result<EntitiesRecord> resultGroup = dslContext.selectFrom(ENTITIES)
            .where(ENTITIES.DISPLAY_NAME.eq("MarketSettingsManager")).fetch();
        assertEquals(1, resultGroup.size());
        resultGroup = dslContext.selectFrom(ENTITIES)
            .where(ENTITIES.DISPLAY_NAME.eq("PresentationManager")).fetch();
        assertEquals(1, resultGroup.size());
        Result<EntityAttrsRecord> resultAttrs = dslContext.selectFrom(ENTITY_ATTRS).where(ENTITY_ATTRS.NAME.contains("util")).fetch();
        assertEquals(20, resultAttrs.size());
        resultAttrs = dslContext.selectFrom(ENTITY_ATTRS).where(ENTITY_ATTRS.NAME.eq("currencySetting")).fetch();
        assertEquals(1, resultAttrs.size());

    }
}
