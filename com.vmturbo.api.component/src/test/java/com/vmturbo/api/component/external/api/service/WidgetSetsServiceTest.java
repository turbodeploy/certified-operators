package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.api.dto.WidgetsetApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.commons.idgen.IdentityGenerator;

/**
 * Tests for the in-memory scaffolding for the widgetset storage. This will be completely re-done
 * for the full implementation of widgetsets.
 */
public class WidgetSetsServiceTest {

    private static final String NEW_WIDGETSET_NAME = "new widgetset";
    private static final String WIDGETSET_CATEGORY_1 = "category1";
    private static final String WIDGETSET_SCOPETYPE_1 = "scopetype1";
    private static final String WIDGETSET_NAME_1 = "widgetset 1";
    private static final String WIDGETSET_NAME_UPDATED = "widgetset 1 UPDATED";
    private static final String WIDGETSET_UUID_1 = "widgetset1";
    private static final String WIDGETSET_NAME_2 = "widgetset 2";
    private static final String WIDGETSET_UUID_2 = "widgetset2";
    private static final String WIDGETSET_CATEGORY_2 = "category2";
    private static final String WIDGETSET_SCOPETYPE_2 = "scopetype2";
    private static final String WIDGETSET_NOT_FOUND_UUID = "widgetset_not_found";

    private WidgetSetsService widgetSetsService;
    private WidgetsetApiDTO widgetset1 = new WidgetsetApiDTO();
    private WidgetsetApiDTO widgetset2 = new WidgetsetApiDTO();

    /**
     * Initialize widgetset store with two widgetsets.
     */
    @Before
    public void setup() {

        // set up a unique prefix for IDs generated here
        IdentityGenerator.initPrefix(1);

        // create some {@link WidgetsetApiDTO} values to test with
        widgetset1.setDisplayName(WIDGETSET_NAME_1);
        widgetset1.setUuid(WIDGETSET_UUID_1);
        widgetset1.setCategory(WIDGETSET_CATEGORY_1);
        widgetset1.setScopeType(WIDGETSET_SCOPETYPE_1);

        widgetset2.setDisplayName(WIDGETSET_NAME_2);
        widgetset2.setUuid(WIDGETSET_UUID_2);
        widgetset2.setCategory(WIDGETSET_CATEGORY_2);
        widgetset2.setScopeType(WIDGETSET_SCOPETYPE_2);

        // initialize test instance
        widgetSetsService = new WidgetSetsService();
        widgetSetsService.getInMemoryWidgetSetCache().put(widgetset1.getUuid(), widgetset1);
        widgetSetsService.getInMemoryWidgetSetCache().put(widgetset2.getUuid(), widgetset2);
    }

    /**
     * Test that we can retrieve the full widgetset list.
     */
    @Test
    public void testGetFullWidgetsetList() throws Exception {
        // Arrange
        // Act
        List<WidgetsetApiDTO> widgetsets = widgetSetsService.getWidgetsetList(null, null);
        // Assert
        assertThat(widgetsets, hasSize(2));
        assertThat(widgetsets, containsInAnyOrder(widgetset1,widgetset2));
    }

    /**
     * Test that we can retrieve a widgetset by id.
     */
    @Test
    public void testGetWidgetset() throws Exception {
        // Arrange
        // Act
        WidgetsetApiDTO widgetseta = widgetSetsService.getWidgetset(WIDGETSET_UUID_1);
        WidgetsetApiDTO widgetsetb = widgetSetsService.getWidgetset(WIDGETSET_UUID_2);
        // Assert
        assertThat(widgetseta.getUuid(), equalTo(WIDGETSET_UUID_1));
        assertThat(widgetsetb.getUuid(), equalTo(WIDGETSET_UUID_2));
    }

    /**
     * Test that fetching a widgetset by an unknown UUID throws {@link UnknownObjectException}
     * @throws UnknownObjectException if the object being requested is not found
     */
    @Test(expected = UnknownObjectException.class)
    public void testGetWidgetsetNotFound() throws Exception {
        // Act
        widgetSetsService.getWidgetset(WIDGETSET_NOT_FOUND_UUID);
        // Assert
        fail("Should never get here!");
    }

    /**
     * Test that we can create (add) a new widgetset. The UUID should be auto generated.
     */
    @Test
    public void testCreateWidgetset() throws Exception {
        // Arrange
        WidgetsetApiDTO newWidgetset = new WidgetsetApiDTO();
        newWidgetset.setUuid(null);
        newWidgetset.setDisplayName(NEW_WIDGETSET_NAME);
        newWidgetset.setCategory(WIDGETSET_CATEGORY_1);
        newWidgetset.setScopeType(WIDGETSET_SCOPETYPE_1);
        // Act
        WidgetsetApiDTO created = widgetSetsService.createWidgetset(newWidgetset);
        // Assert
        assertNotNull(created.getUuid());
        assertThat(widgetSetsService.getInMemoryWidgetSetCache().keySet(), hasSize(3));
        assertThat(widgetSetsService.getInMemoryWidgetSetCache(), hasKey(created.getUuid()));
        assertThat(widgetSetsService.getInMemoryWidgetSetCache().get(created.getUuid()),
                equalTo(created));
        assertThat(created.getDisplayName(), equalTo(NEW_WIDGETSET_NAME));
        assertThat(created.getCategory(), equalTo(WIDGETSET_CATEGORY_1));
        assertThat(created.getScopeType(), equalTo(WIDGETSET_SCOPETYPE_1));

    }

    /**
     * Test that a widgetset is updated (replaced).
     */
    @Test
    public void testUpdateWidgetset() throws Exception {
        // Arrange
        WidgetsetApiDTO updatedWidgetset = new WidgetsetApiDTO();
        updatedWidgetset.setDisplayName(WIDGETSET_NAME_UPDATED);
        updatedWidgetset.setUuid(WIDGETSET_UUID_1);
        updatedWidgetset.setCategory(WIDGETSET_CATEGORY_1);
        updatedWidgetset.setScopeType(WIDGETSET_SCOPETYPE_1);

        // Act
        WidgetsetApiDTO updatedAnswer = widgetSetsService.updateWidgetset(WIDGETSET_UUID_1,
                updatedWidgetset);

        // Assert
        assertThat(widgetSetsService.getInMemoryWidgetSetCache().keySet(), hasSize(2));
        assertThat(widgetSetsService.getInMemoryWidgetSetCache(), hasKey(updatedWidgetset.getUuid()));
        assertThat(widgetSetsService.getInMemoryWidgetSetCache().get(updatedWidgetset.getUuid()),
                equalTo(updatedWidgetset));
        assertThat(updatedAnswer, equalTo(updatedWidgetset));
    }

    /**
     * Test that a widgetset is added if "update()" but not found.
     */
    @Test
    public void testUpdateWidgetsetNotFound() throws Exception {
        // Arrange
        WidgetsetApiDTO updatedWidgetset = new WidgetsetApiDTO();
        updatedWidgetset.setUuid(WIDGETSET_NOT_FOUND_UUID);
        updatedWidgetset.setDisplayName(WIDGETSET_NAME_UPDATED);
        updatedWidgetset.setCategory(WIDGETSET_CATEGORY_1);
        updatedWidgetset.setScopeType(WIDGETSET_SCOPETYPE_1);
        // Act
        WidgetsetApiDTO updated = widgetSetsService.updateWidgetset(WIDGETSET_NOT_FOUND_UUID, updatedWidgetset);
        // Assert
        assertNotNull(updated.getUuid());
        assertThat(widgetSetsService.getInMemoryWidgetSetCache().keySet(), hasSize(3));
        assertThat(widgetSetsService.getInMemoryWidgetSetCache(), hasKey(updated.getUuid()));
        assertThat(widgetSetsService.getInMemoryWidgetSetCache().get(updated.getUuid()),
                equalTo(updated));
        assertThat(updated.getDisplayName(), equalTo(WIDGETSET_NAME_UPDATED));
        assertThat(updated.getCategory(), equalTo(WIDGETSET_CATEGORY_1));
        assertThat(updated.getScopeType(), equalTo(WIDGETSET_SCOPETYPE_1));
    }

    /**
     * Test that a widgetset is deleted.
     */
    @Test
    public void testDeleteWidgetset() throws Exception {
        // Arrange
        // Act
        widgetSetsService.deleteWidgetset(WIDGETSET_UUID_1);
        // Assert
        assertThat(widgetSetsService.getInMemoryWidgetSetCache().keySet(), hasSize(1));
        assertThat(widgetSetsService.getInMemoryWidgetSetCache(), not(hasKey(WIDGETSET_UUID_1)));
    }

    /**
     * Test that a widgetset is deleted.
     */
    @Test
    public void testDeleteWidgetsetNotFound() throws Exception {
        // Arrange
        // Act
        widgetSetsService.deleteWidgetset(WIDGETSET_NOT_FOUND_UUID);
        // Assert
        assertThat(widgetSetsService.getInMemoryWidgetSetCache().keySet(), hasSize(2));
    }

}