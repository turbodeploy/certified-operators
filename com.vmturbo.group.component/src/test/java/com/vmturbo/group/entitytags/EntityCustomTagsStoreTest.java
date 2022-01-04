package com.vmturbo.group.entitytags;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.hamcrest.Matchers;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass;
import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass.EntityCustomTags;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.group.db.GroupComponent;
import com.vmturbo.group.db.TestGroupDBEndpointConfig;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Test the user defined entity tags store.
 */
@RunWith(Parameterized.class)
public class EntityCustomTagsStoreTest extends MultiDbTestBase {

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public EntityCustomTagsStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(GroupComponent.GROUP_COMPONENT, configurableDbDialect, dialect, "group",
                TestGroupDBEndpointConfig::groupEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    private EntityCustomTagsStore entityCustomTagsStore;

    private static final long ENTITY_ID = 107L;
    private static final String tagName1 = "tag1";
    private static final String tagValue1 = "value1";
    private static final String tagName2 = "tag2";
    private static final String tagValue2 = "value2";

    private static final Tags tags = Tags.newBuilder()
            .putTags(tagName1, TagValuesDTO.newBuilder()
                    .addAllValues(Arrays.asList(tagValue1, tagValue2)).build())
            .putTags(tagName2, TagValuesDTO.newBuilder()
                    .addAllValues(Collections.singletonList(tagValue1)).build()).build();

    private static final EntityCustomTagsOuterClass.EntityCustomTags entityCustomTags =
            EntityCustomTagsOuterClass.EntityCustomTags.newBuilder()
                    .setEntityId(ENTITY_ID)
                    .setTags(tags)
                    .build();

    /**
     * Initialize the context and the store.
     *
     * @throws SQLException                if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException        if interrupted
     */
    @Before
    public void setup() throws SQLException, UnsupportedDialectException, InterruptedException {
        entityCustomTagsStore = new EntityCustomTagsStore(dsl);
    }

    /**
     * Test the default case of inserting three different tags. Two of them has the same key. Should
     * insert all three tags.
     *
     * @throws StoreOperationException should not happen.
     */
    @Test
    public void testInsertTags() throws StoreOperationException {
        int result = entityCustomTagsStore.insertTags(ENTITY_ID, tags);
        assertThat(result, is(3));
    }

    /**
     * Test the case of inserting 3 tags, out of which 2 are the same.
     *
     * @throws StoreOperationException due to duplicate tags insertion.
     */
    @Test(expected = StoreOperationException.class)
    public void testInsertDuplicateTags() throws StoreOperationException {

        final Tags tags1 = Tags.newBuilder()
                .putTags(tagName1, TagValuesDTO.newBuilder()
                        .addAllValues(Arrays.asList(tagValue1, tagValue2)).build()).build();
        final Tags tags2 = Tags.newBuilder().putTags(tagName1, TagValuesDTO.newBuilder()
                .addAllValues(Collections.singletonList(tagValue1)).build()).build();
        entityCustomTagsStore.insertTags(ENTITY_ID, tags2);
        entityCustomTagsStore.insertTags(ENTITY_ID, tags1);
    }

    /**
     * Test the case of inserting 0 tags. Should complete with zero insertions.
     *
     * @throws StoreOperationException should not happen.
     */
    @Test
    public void testEmptyTags() throws StoreOperationException {

        final Tags tags = Tags.newBuilder().build();
        int result = entityCustomTagsStore.insertTags(ENTITY_ID, tags);
        assertThat(result, is(0));
    }

    /**
     * Test the case of getting tags of an entity that were just inserted.
     *
     * @throws StoreOperationException should not happen.
     */
    @Test
    public void getTagsTest() throws StoreOperationException {
        final Tags tags1 = Tags.newBuilder()
                .putTags(tagName1, TagValuesDTO.newBuilder()
                        .addAllValues(Arrays.asList(tagValue1, tagValue2)).build()).build();
        entityCustomTagsStore.insertTags(ENTITY_ID, tags1);

        Map<String, TagValuesDTO> tags = entityCustomTagsStore.getTags(ENTITY_ID).getTagsMap();
        TagValuesDTO values = tags.get(tagName1);
        assertThat(values, is(notNullValue()));
        assertThat(values.getValuesList().get(0), is(tagValue1));
        assertThat(values.getValuesList().get(1), is(tagValue2));
    }

    /**
     * Test the case of getting all tags for all entities that were just inserted.
     *
     * @throws StoreOperationException should not happen.
     */
    @Test
    public void getAllTagsTest() throws StoreOperationException {
        final Tags tags1 = Tags.newBuilder()
                .putTags(tagName1, TagValuesDTO.newBuilder()
                        .addAllValues(Arrays.asList(tagValue1, tagValue2)).build()).build();
        final Tags tags2 = Tags.newBuilder().putTags(tagName2, TagValuesDTO.newBuilder()
                .addAllValues(Collections.singletonList(tagValue1)).build()).build();
        entityCustomTagsStore.insertTags(ENTITY_ID, tags2);
        entityCustomTagsStore.insertTags(ENTITY_ID, tags1);

        List<EntityCustomTags> allTags = entityCustomTagsStore.getAllTags();
        assertThat(allTags.get(0).getEntityId(), is(ENTITY_ID));

        Map<String, TagValuesDTO> tags = allTags.get(0).getTags().getTagsMap();

        TagValuesDTO values = tags.get(tagName1);
        assertThat(values, is(notNullValue()));
        assertThat(values.getValuesList().get(0), is(tagValue1));
        assertThat(values.getValuesList().get(1), is(tagValue2));

        values = tags.get(tagName2);
        assertThat(values, is(notNullValue()));
        assertThat(values.getValuesList().get(0), is(tagValue1));
    }

    /**
     * Tests how tags are deleted.
     *
     * @throws StoreOperationException should not happen
     */
    @Test
    public void testDeleteTag() throws StoreOperationException {
        int result = entityCustomTagsStore.insertTags(ENTITY_ID, tags);
        assertThat(result, is(3));

        int affectedRows = entityCustomTagsStore.deleteTag(ENTITY_ID, tagName1);
        assertThat(affectedRows, is(2));
        Map<String, TagValuesDTO> tagsMap = entityCustomTagsStore.getTags(ENTITY_ID).getTagsMap();
        Assert.assertThat(tagsMap.size(), is(1));
        Assert.assertThat(tagsMap.get(tagName1), is(Matchers.nullValue()));
        Assert.assertThat(tagsMap.get(tagName2), is(Matchers.notNullValue()));
    }

    /**
     * Tests how tags are deleted if tag does not exist.
     *
     * @throws StoreOperationException should not happen
     */
    @Test
    public void testDeleteTagNotExist() throws StoreOperationException {
        final String notExistTag = "randomTag";

        int affectedRows = entityCustomTagsStore.deleteTag(ENTITY_ID, notExistTag);
        assertThat(affectedRows, is(0));
    }

    /**
     * Tests how tags are deleted.
     *
     * @throws StoreOperationException should not happen
     */
    @Test
    public void testDeleteTags() throws StoreOperationException {
        int result = entityCustomTagsStore.insertTags(ENTITY_ID, tags);
        assertThat(result, is(3));

        entityCustomTagsStore.deleteTags(ENTITY_ID);

        Map<String, TagValuesDTO> tagsMap = entityCustomTagsStore.getTags(ENTITY_ID).getTagsMap();
        Assert.assertThat(tagsMap.size(), is(0));
        Assert.assertThat(tagsMap.get(tagName1), is(Matchers.nullValue()));
        Assert.assertThat(tagsMap.get(tagName2), is(Matchers.nullValue()));
    }

    /**
     * Test the case of deleting a tag list for an entity.
     *
     * @throws StoreOperationException should not happen.
     */
    @Test
    public void deleteTagListTest() throws StoreOperationException {
        final String notDeleted = "notDeleted";
        final Tags tags = Tags.newBuilder()
                .putTags(tagName1, TagValuesDTO.newBuilder()
                        .addAllValues(Arrays.asList(tagValue1, tagValue2)).build())
                .putTags(tagName2, TagValuesDTO.newBuilder()
                        .addValues(tagValue1).build())
                .putTags(notDeleted, TagValuesDTO.newBuilder()
                        .addValues(tagValue1).build())
                .build();
        entityCustomTagsStore.insertTags(ENTITY_ID, tags);

        int affectedRows = entityCustomTagsStore.deleteTagList(
                ENTITY_ID,
                Arrays.asList(tagName1, tagName2)
        );
        assertThat(affectedRows, is(3));

        Map<String, TagValuesDTO> tagsMap = entityCustomTagsStore.getTags(ENTITY_ID).getTagsMap();
        assertThat(tagsMap.size(), is(1));

        TagValuesDTO values = tagsMap.get(notDeleted);
        assertThat(values, is(notNullValue()));
        assertThat(values.getValuesList().size(), is(1));
        assertThat(values.getValuesList().get(0), is(tagValue1));
    }

    /**
     * Test the case of deleting a tag list for an entity that does not exist.
     *
     * @throws StoreOperationException due to deleting a tag that does not exist.
     */
    @Test(expected = StoreOperationException.class)
    public void deleteTagListNotExistTest() throws StoreOperationException {
        entityCustomTagsStore.deleteTagList(
                ENTITY_ID,
                Arrays.asList(tagName1)
        );
    }
}
