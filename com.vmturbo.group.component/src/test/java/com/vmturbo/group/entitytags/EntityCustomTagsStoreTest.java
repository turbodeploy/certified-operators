package com.vmturbo.group.entitytags;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collections;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.EntityCustomTagsOuterClass;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.group.db.GroupComponent;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Test the user defined entity tags store.
 */
public class EntityCustomTagsStoreTest {

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(GroupComponent.GROUP_COMPONENT);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

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
     */
    @Before
    public void setup() {
        final DSLContext dslContext = dbConfig.getDslContext();
        entityCustomTagsStore = new EntityCustomTagsStore(dslContext);
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
}
