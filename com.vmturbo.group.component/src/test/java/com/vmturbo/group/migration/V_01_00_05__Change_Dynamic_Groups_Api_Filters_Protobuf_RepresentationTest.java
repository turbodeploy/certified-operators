package com.vmturbo.group.migration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;

import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.common.protobuf.group.GroupDTO.Group.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupDTOREST.Group.Origin;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchParameters.FilterSpecs;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.tables.records.GroupingRecord;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestSQLDatabaseConfig.class})
@TestPropertySource(properties = {"originalSchemaName=group_component"})
public class V_01_00_05__Change_Dynamic_Groups_Api_Filters_Protobuf_RepresentationTest {
    @Autowired
    private TestSQLDatabaseConfig dbConfig;

    private DSLContext dslContext;

    private V_01_00_05__Change_Dynamic_Groups_Api_Filters_Protobuf_Representation migration;

    @Before
    public void setup() {
        dslContext = dbConfig.prepareDatabase();
        migration = new V_01_00_05__Change_Dynamic_Groups_Api_Filters_Protobuf_Representation(dslContext);
    }

    /**
     * Tests treatment of a simple group with multiple filters.
     * Some of the filters must be translated for the group to function properly in 7.17.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testSimpleGroup() throws Exception {
        final String name = "name";
        final long cpus = 2L;
        final long groupId = 0L;

        // starting filter: entity type
        final PropertyFilter startingFilter = PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.ENTITY_TYPE)
                .setNumericFilter(
                        NumericFilter.newBuilder()
                                .setComparisonOperator(ComparisonOperator.EQ)
                                .setValue(EntityType.VIRTUAL_MACHINE.ordinal()))
                .build();

        // first filter: display name regex
        final SearchFilter displayNameFilter =
                SearchFilter.newBuilder()
                        .setPropertyFilter(PropertyFilter.newBuilder()
                                .setPropertyName(SearchableProperties.DISPLAY_NAME)
                                .setStringFilter(StringFilter.newBuilder()
                                        .setStringPropertyRegex(name)))
                        .build();
        final FilterSpecs displayNameSpecs = FilterSpecs.newBuilder()
                .setExpressionValue("^" + name + "$")
                .setExpressionType("EQ") // must change in migration
                .setFilterType("vmsByName")
                .build();

        // second filter: number of CPUs
        final SearchFilter numCPUsFilter = SearchFilter.newBuilder()
                .setPropertyFilter(
                        PropertyFilter.newBuilder()
                                .setPropertyName(
                                        SearchableProperties.VM_INFO_NUM_CPUS)
                                .setNumericFilter(
                                        NumericFilter.newBuilder()
                                                .setComparisonOperator(ComparisonOperator.GT)
                                                .setValue(cpus)))
                .build();
        final FilterSpecs numCPUsSpecs = FilterSpecs.newBuilder()
                .setExpressionValue(Long.toString(cpus))
                .setExpressionType("EQ") // must not change in migration
                .setFilterType("vmsByNumCPUs")
                .build();

        // third filter: entity state
        final SearchFilter entityStateFilter =
                SearchFilter.newBuilder()
                        .setPropertyFilter(PropertyFilter.newBuilder()
                                .setPropertyName(SearchableProperties.ENTITY_STATE)
                                .setStringFilter(
                                        StringFilter.newBuilder()
                                                .setStringPropertyRegex(EntityState.POWERED_ON.name())))
                        .build();
        final FilterSpecs entityStateSpecs = FilterSpecs.newBuilder()
                .setExpressionValue(EntityState.POWERED_ON.name())
                .setExpressionType("EQ") // must not change in migration
                .setFilterType("vmsByState")
                .build();

        final SearchParametersCollection searchParametersCollection =
                SearchParametersCollection.newBuilder()
                        .addSearchParameters(SearchParameters.newBuilder()
                                .setStartingFilter(startingFilter)
                                .addSearchFilter(displayNameFilter)
                                .setSourceFilterSpecs(displayNameSpecs))
                        .addSearchParameters(SearchParameters.newBuilder()
                                .setStartingFilter(startingFilter)
                                .addSearchFilter(numCPUsFilter)
                                .setSourceFilterSpecs(numCPUsSpecs))
                        .addSearchParameters(SearchParameters.newBuilder()
                                .setStartingFilter(startingFilter)
                                .addSearchFilter(entityStateFilter)
                                .setSourceFilterSpecs(entityStateSpecs))
                        .build();
        final GroupInfo groupInfoIn = GroupInfo.newBuilder()
                .setName(name)
                .setDisplayName(name)
                .setEntityType(EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                .setSearchParametersCollection(searchParametersCollection)
                .build();

        final GroupingRecord recordIn = new GroupingRecord(
                groupId, name, Origin.USER.getValue(),
                Type.GROUP_VALUE, EntityType.VIRTUAL_MACHINE.getValue(),
                null, groupInfoIn.toByteArray());
        dslContext.insertInto(Tables.GROUPING).set(recordIn).execute();

        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        final List<GroupingRecord> allRecordsOut = dslContext.selectFrom(Tables.GROUPING).fetch();
        Assert.assertEquals(1, allRecordsOut.size());
        final GroupingRecord recordOut = allRecordsOut.get(0);
        Assert.assertEquals(groupId, (long)recordOut.getId());
        Assert.assertEquals(name, recordOut.getName());
        Assert.assertEquals(Origin.USER.getValue(), (int)recordOut.getOrigin());
        Assert.assertEquals(Type.GROUP_VALUE, (int)recordOut.getType());
        Assert.assertEquals(EntityType.VIRTUAL_MACHINE.getValue(), (int)recordOut.getEntityType());
        Assert.assertNull(recordOut.getDiscoveredById());

        final GroupInfo groupInfoOut = GroupInfo.parseFrom(recordOut.getGroupData());
        Assert.assertEquals(groupInfoIn.getName(), groupInfoOut.getName());
        Assert.assertEquals(groupInfoIn.getDisplayName(), groupInfoOut.getDisplayName());
        Assert.assertEquals(groupInfoIn.getEntityType(), groupInfoOut.getEntityType());
        final List<SearchParameters> searchParametersOut =
                groupInfoOut.getSearchParametersCollection().getSearchParametersList();
        Assert.assertEquals(3, searchParametersOut.size());
        Assert.assertEquals(searchParametersOut.get(1), searchParametersCollection.getSearchParameters(1));
        Assert.assertEquals(searchParametersOut.get(2), searchParametersCollection.getSearchParameters(2));
        Assert.assertEquals(searchParametersOut.get(0).getStartingFilter(),
                searchParametersCollection.getSearchParameters(0).getStartingFilter());
        Assert.assertEquals(searchParametersOut.get(0).getSearchFilter(0),
                searchParametersCollection.getSearchParameters(0).getSearchFilter(0));
        Assert.assertEquals(searchParametersOut.get(0).getSourceFilterSpecs(),
                FilterSpecs.newBuilder(displayNameSpecs).setExpressionType("RXEQ").build());

        // check migration idempotence
        final MigrationProgressInfo migrationResult1 = migration.startMigration();
        assertThat(migrationResult1.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult1.getCompletionPercentage(), is(100.0f));
        final List<GroupingRecord> allRecordsOut1 = dslContext.selectFrom(Tables.GROUPING).fetch();
        Assert.assertEquals(allRecordsOut, allRecordsOut1);
    }

    /**
     * Tests treatment of a static group: it should not be deleted or translated.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testStaticGroup() throws Exception {
        final String name = "name";
        final long groupId = 0L;
        final List<Long> memberOids = ImmutableList.of(1L, 2L, 5L);

        final StaticGroupMembers staticGroupMembers = StaticGroupMembers.newBuilder()
                                                            .addAllStaticMemberOids(memberOids)
                                                            .build();
        final GroupInfo groupInfoIn = GroupInfo.newBuilder()
                                            .setName(name)
                                            .setDisplayName(name)
                                            .setEntityType(EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
                                            .setStaticGroupMembers(staticGroupMembers)
                                            .build();
        final GroupingRecord recordIn = new GroupingRecord(
                                                groupId, name, Origin.USER.getValue(),
                                                Type.GROUP_VALUE, EntityType.VIRTUAL_MACHINE.getValue(),
                                                null, groupInfoIn.toByteArray());
        dslContext.insertInto(Tables.GROUPING).set(recordIn).execute();

        final MigrationProgressInfo migrationResult = migration.startMigration();
        assertThat(migrationResult.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult.getCompletionPercentage(), is(100.0f));

        final List<GroupingRecord> allRecordsOut = dslContext.selectFrom(Tables.GROUPING).fetch();
        Assert.assertEquals(1, allRecordsOut.size());
        final GroupingRecord recordOut = allRecordsOut.get(0);
        Assert.assertEquals(groupId, (long)recordOut.getId());
        Assert.assertEquals(name, recordOut.getName());
        Assert.assertEquals(Origin.USER.getValue(), (int)recordOut.getOrigin());
        Assert.assertEquals(Type.GROUP_VALUE, (int)recordOut.getType());
        Assert.assertEquals(EntityType.VIRTUAL_MACHINE.getValue(), (int)recordOut.getEntityType());
        Assert.assertNull(recordOut.getDiscoveredById());

        final GroupInfo groupInfoOut = GroupInfo.parseFrom(recordOut.getGroupData());
        Assert.assertEquals(groupInfoIn, groupInfoOut);

        // check migration idempotence
        final MigrationProgressInfo migrationResult1 = migration.startMigration();
        assertThat(migrationResult1.getStatus(), is(MigrationStatus.SUCCEEDED));
        assertThat(migrationResult1.getCompletionPercentage(), is(100.0f));
        final List<GroupingRecord> allRecordsOut1 = dslContext.selectFrom(Tables.GROUPING).fetch();
        Assert.assertEquals(allRecordsOut, allRecordsOut1);
    }
}
