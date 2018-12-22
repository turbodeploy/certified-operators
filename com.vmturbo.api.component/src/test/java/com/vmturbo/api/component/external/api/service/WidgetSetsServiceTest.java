package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.grpc.Status;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.api.component.external.api.mapper.WidgetsetMapper;
import com.vmturbo.api.dto.widget.WidgetApiDTO;
import com.vmturbo.api.dto.widget.WidgetsetApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.widgets.Widgets;
import com.vmturbo.common.protobuf.widgets.Widgets.DeleteWidgetsetRequest;
import com.vmturbo.common.protobuf.widgets.WidgetsMoles;
import com.vmturbo.common.protobuf.widgets.WidgetsetsServiceGrpc;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;

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
    private static final String WIDGETSET_UUID_1 = "1";
    private static final String WIDGETSET_OWNER_1 = "101";
    private static final String WIDGETSET_NAME_2 = "widgetset 2";
    private static final String WIDGETSET_UUID_2 = "2";
    private static final String WIDGETSET_OWNER_2 = "102";
    private static final String WIDGETSET_CATEGORY_2 = "category2";
    private static final String WIDGETSET_SCOPETYPE_2 = "scopetype2";
    private static final String WIDGETSET_NOT_FOUND_UUID = "9999";

    private WidgetsMoles.WidgetsetsServiceMole widgetsetsserviceSpy =
            spy(new WidgetsMoles.WidgetsetsServiceMole());
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(widgetsetsserviceSpy);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private WidgetSetsService widgetSetsService;
    private WidgetsetApiDTO widgetset1 = new WidgetsetApiDTO();
    private WidgetsetApiDTO widgetset2 = new WidgetsetApiDTO();
    private WidgetApiDTO widget1 = new WidgetApiDTO();
    private WidgetApiDTO widget2 = new WidgetApiDTO();

    private Widgets.Widgetset widgetsetProto1;
    private Widgets.Widgetset widgetsetProto2;



    /**
     * Initialize widgetset store with two widgetsets.
     */
    @Before
    public void setup() {

        // set up a unique prefix for IDs generated here
        IdentityGenerator.initPrefix(1);

        // create some {@link WidgetsetApiDTO} values to test with
        widget1.setDisplayName("widget1");
        widgetset1.setDisplayName(WIDGETSET_NAME_1);
        widgetset1.setUuid(WIDGETSET_UUID_1);
        widgetset1.setUsername(WIDGETSET_OWNER_1);
        widgetset1.setCategory(WIDGETSET_CATEGORY_1);
        widgetset1.setScopeType(WIDGETSET_SCOPETYPE_1);
        widgetset1.setWidgets(ImmutableList.of(widget1));

        widget2.setDisplayName("widget2");
        widgetset2.setDisplayName(WIDGETSET_NAME_2);
        widgetset2.setUuid(WIDGETSET_UUID_2);
        widgetset2.setUsername(WIDGETSET_OWNER_2);
        widgetset2.setCategory(WIDGETSET_CATEGORY_2);
        widgetset2.setScopeType(WIDGETSET_SCOPETYPE_2);
        widgetset2.setWidgets(ImmutableList.of(widget2));

        widgetsetProto1 = WidgetsetMapper.fromUiWidgetset(widgetset1);
        widgetsetProto2 = WidgetsetMapper.fromUiWidgetset(widgetset2);

        // initialize test instance
        widgetSetsService = new WidgetSetsService(WidgetsetsServiceGrpc.newBlockingStub(
                grpcServer.getChannel()));
    }

    /**
     * Test that we can retrieve the full widgetset list.
     */
    @Test
    public void testGetFullWidgetsetList() {
        // Arrange
        when(widgetsetsserviceSpy.getWidgetsetList(anyObject()))
                .thenReturn(Lists.newArrayList(widgetsetProto1, widgetsetProto2));
        // Act
        List<WidgetsetApiDTO> widgetsets = widgetSetsService.getWidgetsetList(null, null);
        // Assert
        assertThat(widgetsets, hasSize(2));
        // sadly you cannot compare WidgetsetApiDTO with equal()
        assertThat(widgetsets.get(0).toString(), equalTo(widgetset1.toString()));
        assertThat(widgetsets.get(1).toString(), equalTo(widgetset2.toString()));
    }

    /**
     * Test that we can retrieve a widgetset by id.
     */
    @Test
    public void testGetWidgetset() throws Exception {
        // Arrange
        when(widgetsetsserviceSpy.getWidgetset(Widgets.GetWidgetsetRequest.newBuilder()
                .setOid(Long.valueOf(WIDGETSET_UUID_1))
                .build()))
                .thenReturn(widgetsetProto1);
        when(widgetsetsserviceSpy.getWidgetset(Widgets.GetWidgetsetRequest.newBuilder()
                .setOid(Long.valueOf(WIDGETSET_UUID_2))
                .build()))
                .thenReturn(widgetsetProto2);
        // Act
        WidgetsetApiDTO widgetseta = widgetSetsService.getWidgetset(WIDGETSET_UUID_1);
        WidgetsetApiDTO widgetsetb = widgetSetsService.getWidgetset(WIDGETSET_UUID_2);
        // Assert
        assertThat(widgetseta.toString(), equalTo(widgetset1.toString()));
        assertThat(widgetsetb.toString(), equalTo(widgetset2.toString()));
    }

    /**
     * Test that fetching a widgetset by an unknown UUID throws {@link UnknownObjectException}
     * @throws UnknownObjectException if the object being requested is not found
     */
    @Test(expected = UnknownObjectException.class)
    public void testGetWidgetsetNotFound() throws Exception {
        // Arrange
        when(widgetsetsserviceSpy.getWidgetset(anyObject()))
                .thenReturn(Widgets.Widgetset.newBuilder().build());

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
        newWidgetset.setUsername(WIDGETSET_OWNER_1);
        newWidgetset.setDisplayName(NEW_WIDGETSET_NAME);
        newWidgetset.setCategory(WIDGETSET_CATEGORY_1);
        newWidgetset.setScopeType(WIDGETSET_SCOPETYPE_1);
        newWidgetset.setWidgets(ImmutableList.of(widget1));

        when(widgetsetsserviceSpy.createWidgetset(anyObject()))
                .thenReturn(widgetsetProto2);
        // Act
        WidgetsetApiDTO created = widgetSetsService.createWidgetset(newWidgetset);
        // Assert
        // convert to protobuf so 'equals()' can be used
        assertThat(WidgetsetMapper.fromUiWidgetset(created), equalTo(widgetsetProto2));

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
        updatedWidgetset.setUsername(WIDGETSET_OWNER_1);
        updatedWidgetset.setCategory(WIDGETSET_CATEGORY_1);
        updatedWidgetset.setScopeType(WIDGETSET_SCOPETYPE_1);
        updatedWidgetset.setWidgets(ImmutableList.of(widget1));

        when(widgetsetsserviceSpy.updateWidgetset(anyObject()))
                .thenReturn(widgetsetProto2);

        // Act
        WidgetsetApiDTO updatedAnswer = widgetSetsService.updateWidgetset(WIDGETSET_UUID_1,
                updatedWidgetset);

        // Assert
        // convert to protobuf so 'equals()' can be used
        assertThat(WidgetsetMapper.fromUiWidgetset(updatedAnswer), equalTo(widgetsetProto2));
    }

    /**
     * Test that a widgetset is added if "update()" but not found.
     */
    @Test
    public void testUpdateWidgetsetNotFound() throws Exception {
        // Arrange
        WidgetsetApiDTO updatedWidgetset = new WidgetsetApiDTO();
        updatedWidgetset.setUuid(WIDGETSET_NOT_FOUND_UUID);
        updatedWidgetset.setUsername(WIDGETSET_OWNER_1);
        updatedWidgetset.setDisplayName(WIDGETSET_NAME_UPDATED);
        updatedWidgetset.setCategory(WIDGETSET_CATEGORY_1);
        updatedWidgetset.setScopeType(WIDGETSET_SCOPETYPE_1);
        updatedWidgetset.setWidgets(ImmutableList.of(widget1));

        when(widgetsetsserviceSpy.updateWidgetsetError(anyObject()))
                .thenReturn(Optional.of((Status.NOT_FOUND.asException())));

        expectedException.expect(UnknownObjectException.class);
        expectedException.expectMessage("Cannot find widgetset: " + WIDGETSET_NOT_FOUND_UUID);
        // Act
        widgetSetsService.updateWidgetset(WIDGETSET_NOT_FOUND_UUID, updatedWidgetset);
        // Assert
        fail("Should never get here - Exception should have been thrown.");
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
        Mockito.verify(widgetsetsserviceSpy).deleteWidgetset(DeleteWidgetsetRequest.newBuilder()
                .setOid(Long.valueOf(WIDGETSET_UUID_1)).build());
    }

    /**
     * Test that a widgetset is deleted.
     */
    @Test
    public void testDeleteWidgetsetNotFound() throws Exception {
        // Arrange
        when(widgetsetsserviceSpy.deleteWidgetsetError(anyObject()))
                .thenReturn(Optional.of(Status.NOT_FOUND.asException()));
        expectedException.expect(UnknownObjectException.class);
        expectedException.expectMessage("Cannot find widgetset to delete: " + WIDGETSET_NOT_FOUND_UUID);
        // Act
        widgetSetsService.deleteWidgetset(WIDGETSET_NOT_FOUND_UUID);
        // Assert
        fail("Should not get here.");
    }

}
