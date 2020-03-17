package com.vmturbo.group.group;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin;
import com.vmturbo.common.protobuf.group.GroupDTO.Origin.Type;
import com.vmturbo.common.protobuf.group.GroupDTO.OriginFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.MapFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.group.db.GroupComponent;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Unit test to cover {@link GroupDAO} search functionality. This is pretty large a set of tests
 * so they are just extracted to this file of the sake of organization.
 */
public class GroupDaoSearchTest {

    /**
     * Class rule to create a DB.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(GroupComponent.GROUP_COMPONENT);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    /**
     * Expected exception rule.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private GroupDAO groupStore;
    private static final long OWNER_ID = 4566L;
    private static final String tagKey = "the-tag";
    private static final String tagValue1 = "tag-value1";
    private static final String tagValue2 = "tag-value2";
    private static final Set<MemberType> EXPECTED_MEMBERS =
            Collections.singleton(MemberType.newBuilder().setEntity(1).build());
    private static final long OID1 = 1001L;
    private static final long OID2 = 1002L;
    private static final long OID3 = 1003L;

    private final AtomicInteger counter = new AtomicInteger(0);

    /**
     * Initialize local variables.
     *
     * @throws Exception on exceptions occurred
     */
    @Before
    public void setup() throws Exception {
        final DSLContext dslContext = dbConfig.getDslContext();
        groupStore = new GroupDAO(dslContext);

        final Origin userOrigin = createUserOrigin();
        final Origin systemOrigin = createSystemOrigin();
        final Origin discovered = createUserOrigin();
        final GroupDefinition group1 = GroupDefinition.newBuilder(createGroupDefinition())
                .setTags(Tags.newBuilder()
                        .putTags(tagKey, TagValuesDTO.newBuilder().addValues(tagValue1).build()))
                .build();
        final GroupDefinition group2 = GroupDefinition.newBuilder(createGroupDefinition())
                .setIsHidden(true)
                .setOwner(OWNER_ID)
                .setDisplayName("fatherGroup")
                .setTags(Tags.newBuilder()
                        .putTags(tagKey, TagValuesDTO.newBuilder().addValues(tagValue2).build()))
                .build();
        final GroupDefinition group3 = GroupDefinition.newBuilder(createGroupDefinition())
                .setOwner(OWNER_ID)
                .setType(GroupType.COMPUTE_HOST_CLUSTER)
                .setDisplayName("grandFatherGroup")
                .build();
        groupStore.createGroup(OID1, userOrigin, group1, EXPECTED_MEMBERS, true);
        groupStore.createGroup(OID2, systemOrigin, group2, EXPECTED_MEMBERS, true);
        groupStore.createGroup(OID3, discovered, group3, EXPECTED_MEMBERS, true);
    }

    /**
     * Tests how search call works.
     */
    @Test
    public void testGroupSearchAllGroups() {
        final Collection<Grouping> groupsAll =
                groupStore.getGroups(GroupDTO.GroupFilter.newBuilder().build());
        Assert.assertEquals(Sets.newHashSet(OID1, OID3),
                groupsAll.stream().map(Grouping::getId).collect(Collectors.toSet()));
    }

    /**
     * Tests search for origins.
     */
    @Test
    public void testSearchSystemDiscovered() {
        // Search by origin type
        final Collection<Grouping> groupsSystemDiscovered = groupStore.getGroups(
                GroupDTO.GroupFilter.newBuilder()
                        .setOriginFilter(OriginFilter.newBuilder()
                                .addOrigin(Type.DISCOVERED)
                                .addOrigin(Type.USER)
                                .build())
                        .build());
        Assert.assertEquals(Sets.newHashSet(OID1, OID3),
                groupsSystemDiscovered.stream().map(Grouping::getId).collect(Collectors.toSet()));
    }

    /**
     * Tests search by owner id including hidden objects.
     */
    @Test
    public void testSearchNoHiddenWithOwner() {
        final Collection<Grouping> groupsNotHiddenWithOwner = groupStore.getGroups(
                GroupDTO.GroupFilter.newBuilder()
                        .addPropertyFilters(PropertyFilter.newBuilder()
                                .setPropertyName(StringConstants.ACCOUNTID)
                                .setStringFilter(StringFilter.newBuilder()
                                        .addOptions("1234")
                                        .addOptions(Long.toString(OWNER_ID)))
                                .build())
                        .build());
        Assert.assertEquals(Collections.singleton(OID3),
                groupsNotHiddenWithOwner.stream().map(Grouping::getId).collect(Collectors.toSet()));
    }

    /**
     * Tests search by OIDs including hidden groups.
     */
    @Test
    public void testSearchByIdsIncludingHidden() {
        // search by ids including hidden
        final Collection<Grouping> groupsByIdAll = groupStore.getGroups(
                GroupDTO.GroupFilter.newBuilder(GroupProtoUtil.createGroupFilterByIds(
                        Arrays.asList(OID1, OID2)))
                        .setIncludeHidden(true)
                        .build());
        Assert.assertEquals(Sets.newHashSet(OID1, OID2),
                groupsByIdAll.stream().map(Grouping::getId).collect(Collectors.toSet()));
    }

    /**
     * Tests searching by display name using regexp case sensitive.
     */
    @Test
    public void testSearchByDisplayNameRegexpCaseSensitive() {
        // Search by display name regexp case-sensitive
        final Collection<Grouping> groupsByDisplayName1 = groupStore.getGroups(
                GroupDTO.GroupFilter.newBuilder()
                        .addPropertyFilters(PropertyFilter.newBuilder()
                                .setPropertyName(SearchableProperties.DISPLAY_NAME)
                                .setStringFilter(StringFilter.newBuilder()
                                        .setStringPropertyRegex("^.*fatherGroup$")
                                        .setCaseSensitive(true)))
                        .setIncludeHidden(true)
                        .build());
        Assert.assertEquals(Sets.newHashSet(OID2),
                groupsByDisplayName1.stream().map(Grouping::getId).collect(Collectors.toSet()));
    }

    /**
     * Tests searching by display name using regexp case insensitive.
     */
    @Test
    public void testSearchDisplayNameRegexpCaseInsensitive() {
        // Search by display name regexp case-insensitive
        final Collection<Grouping> groupsByDisplayName2 = groupStore.getGroups(
                GroupDTO.GroupFilter.newBuilder()
                        .addPropertyFilters(PropertyFilter.newBuilder()
                                .setPropertyName(SearchableProperties.DISPLAY_NAME)
                                .setStringFilter(StringFilter.newBuilder()
                                        .setStringPropertyRegex("^.*fatherGroup$")
                                        .setCaseSensitive(false)))
                        .setIncludeHidden(true)
                        .build());
        Assert.assertEquals(Sets.newHashSet(OID2, OID3),
                groupsByDisplayName2.stream().map(Grouping::getId).collect(Collectors.toSet()));
    }

    /**
     * Tests searching by display name using options.
     */
    @Test
    public void testSearchDisplayNameOptions() {
        // Search by display name using options
        final Collection<Grouping> groupsByDisplayName3 = groupStore.getGroups(
                GroupDTO.GroupFilter.newBuilder()
                        .addPropertyFilters(PropertyFilter.newBuilder()
                                .setPropertyName(SearchableProperties.DISPLAY_NAME)
                                .setStringFilter(StringFilter.newBuilder()
                                        .addOptions("therG")
                                        .setCaseSensitive(false)))
                        .setIncludeHidden(true)
                        .build());
        Assert.assertEquals(Sets.newHashSet(OID2, OID3),
                groupsByDisplayName3.stream().map(Grouping::getId).collect(Collectors.toSet()));
    }

    /**
     * Tests searching by tags when the tag filter provides options for
     * the values of a tag.
     */
    @Test
    public void testSearchByTagsWithOptionsFilter() {
        final Collection<Grouping> groupsByTags = groupStore.getGroups(
                GroupDTO.GroupFilter.newBuilder()
                        .addPropertyFilters(PropertyFilter.newBuilder()
                                .setPropertyName(StringConstants.TAGS_ATTR)
                                .setMapFilter(MapFilter.newBuilder()
                                        .setKey(tagKey)
                                        .addValues(tagValue1)
                                        .addValues(tagValue2)))
                        .setIncludeHidden(true)
                        .build());
        Assert.assertEquals(Sets.newHashSet(OID1, OID2),
                groupsByTags.stream().map(Grouping::getId).collect(Collectors.toSet()));
    }

    /**
     * Tests search by group types.
     */
    @Test
    public void testSearchByType() {
        final Collection<Grouping> groupsByTags = groupStore.getGroups(
                GroupDTO.GroupFilter.newBuilder()
                        .setGroupType(GroupType.COMPUTE_HOST_CLUSTER)
                        .build());
        Assert.assertEquals(Sets.newHashSet(OID3),
                groupsByTags.stream().map(Grouping::getId).collect(Collectors.toSet()));
    }

    @Nonnull
    private Origin createUserOrigin() {
        return Origin.newBuilder()
                .setUser(Origin.User.newBuilder().setUsername("user-" + counter.getAndIncrement()))
                .build();
    }

    @Nonnull
    private Origin createSystemOrigin() {
        return Origin.newBuilder()
                .setSystem(Origin.System.newBuilder()
                        .setDescription("system-group-" + counter.getAndIncrement()))
                .build();
    }

    @Nonnull
    private GroupDefinition createGroupDefinition() {
        return GroupDefinition.newBuilder()
                .setType(GroupType.REGULAR)
                .setDisplayName("Group-" + counter.getAndIncrement())
                .setStaticGroupMembers(StaticMembers.newBuilder()
                        .addMembersByType(StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder().setEntity(1))
                                .addMembers(counter.getAndIncrement())))
                .setIsHidden(false)
                .build();
    }

    /**
     * Tests searching by tags when the tag filter provides a regex
     * to match key=value against.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testSearchByTagsWithRegexFilter() throws Exception {
        final Origin origin = createUserOrigin();
        final String tagKey1 = "tagkey1";
        final String tagKey2 = "tagkey2";
        final String tagValue1 = "tagValue1";
        final String tagValue2 = "tagValue2";
        final String tagValue3 = "tagValue3";
        final long oidForFourthGroup = 1004L;

        final GroupDefinition groupDefinition =
                GroupDefinition.newBuilder(createGroupDefinition())
                        .setTags(Tags.newBuilder()
                                    .putTags(tagKey1, TagValuesDTO.newBuilder()
                                                            .addValues(tagValue1)
                                                            .addValues(tagValue2).build())
                                    .putTags(tagKey2, TagValuesDTO.newBuilder()
                                                            .addValues(tagValue3).build()))
                        .build();
        groupStore.createGroup(oidForFourthGroup, origin, groupDefinition, EXPECTED_MEMBERS, false);

        final Set<Long> emptyResult = Collections.emptySet();
        final Set<Long> resultWithOnlyGroup4 = ImmutableSet.of(oidForFourthGroup);
        final Set<Long> resultWithTwoGroups = ImmutableSet.of(oidForFourthGroup, OID1);

        Assert.assertEquals(resultWithOnlyGroup4,
                            numberOfAcceptedGroups(MapFilter.newBuilder()
                                                        .setKey(tagKey1)
                                                        .addValues(tagValue2)
                                                        .build()));
        Assert.assertEquals(emptyResult,
                            numberOfAcceptedGroups(MapFilter.newBuilder()
                                                        .setKey(tagKey2)
                                                        .addValues(tagValue1)
                                                        .addValues(tagValue2)
                                                        .build()));
        Assert.assertEquals(emptyResult,
                            numberOfAcceptedGroups(MapFilter.newBuilder()
                                                        .setRegex(".*=.*4")
                                                        .build()));
        Assert.assertEquals(resultWithOnlyGroup4,
                            numberOfAcceptedGroups(MapFilter.newBuilder()
                                                        .setRegex(".*1=t.*")
                                                        .build()));
        Assert.assertEquals(resultWithOnlyGroup4,
                            numberOfAcceptedGroups(MapFilter.newBuilder()
                                                        .setRegex(".*1=.*2")
                                                        .build()));
        Assert.assertEquals(emptyResult,
                            numberOfAcceptedGroups(MapFilter.newBuilder()
                                                        .setRegex(".*1=.*3")
                                                        .build()));
        Assert.assertEquals(resultWithTwoGroups,
                            numberOfAcceptedGroups(MapFilter.newBuilder()
                                                        .setRegex("t.*")
                                                        .build()));
    }

    private Set<Long> numberOfAcceptedGroups(MapFilter mapFilter) {
        return groupStore.getGroups(GroupFilter.newBuilder()
                                        .addPropertyFilters(
                                            PropertyFilter.newBuilder()
                                                .setPropertyName(SearchableProperties.TAGS_TYPE_PROPERTY_NAME)
                                                .setMapFilter(mapFilter))
                                        .build())
                         .stream()
                         .map(Grouping::getId)
                         .collect(Collectors.toSet());
    }
}
