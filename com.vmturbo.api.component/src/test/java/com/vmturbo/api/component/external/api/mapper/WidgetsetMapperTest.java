package com.vmturbo.api.component.external.api.mapper;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.gson.Gson;

import io.grpc.Status;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.widget.WidgetApiDTO;
import com.vmturbo.api.dto.widget.WidgetsetApiDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.widgets.Widgets.Widgetset;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * Test conversion between WidgetsetApiDTO (external) and Widgetset (internal protobuf)
 **/
public class WidgetsetMapperTest {

    public static final String DEFAULT_WIDGETSET_ID = "1";
    public static final String DEFAULT_USER_ID_STRING = "2";
    public static final long DEFAULT_USER_ID = 2L;

    private GroupServiceMole groupServiceBackend = spy(new GroupServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(groupServiceBackend);

    private GroupServiceBlockingStub groupServiceClient;

    private WidgetsetMapper widgetsetMapper;

    private GroupMapper groupMapper = mock(GroupMapper.class);

    @Before
    public void setup() {
        groupServiceClient = GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        widgetsetMapper = new WidgetsetMapper(groupMapper, groupServiceClient);
    }

    @Test
    public void testMapToWidgetsetDTOWithUuid() {
        // Arrange
        WidgetsetApiDTO widgetsetApiDTO = getBaseWidgetsetApiDTO();
        widgetsetApiDTO.setUuid(DEFAULT_USER_ID_STRING);
        // Act
        Widgetset result = widgetsetMapper.fromUiWidgetset(widgetsetApiDTO);
        // Assert
        assertTrue(result.hasOid());
        assertThat(result.getOid(), equalTo(DEFAULT_USER_ID));
    }

    @Test
    public void testMapToWidgetsetDTOWithNoUuid() {
        // Arrange
        WidgetsetApiDTO widgetsetApiDTO = getBaseWidgetsetApiDTO();
        // Act
        Widgetset result = widgetsetMapper.fromUiWidgetset(widgetsetApiDTO);
        // Assert
        assertFalse(result.hasOid());
    }

    @Test
    public void testRoundTrip() {
        // Arrange
        WidgetsetApiDTO widgetsetApiDTO = getBaseWidgetsetApiDTO();
        widgetsetApiDTO.setClassName("CLASSNAME");
        widgetsetApiDTO.setDisplayName("DISPLAYNAME");
        widgetsetApiDTO.setUuid(DEFAULT_WIDGETSET_ID);
        widgetsetApiDTO.setCategory("CATEGORY");
        widgetsetApiDTO.setScope("SCOPE");
        widgetsetApiDTO.setSharedWithAllUsers(true);
        // Act
        final Widgetset intermediate = widgetsetMapper.fromUiWidgetset(widgetsetApiDTO);
        WidgetsetApiDTO answer = widgetsetMapper.toUiWidgetset(intermediate);
        // Assert
        assertThat(GSON.toJson(answer), equalTo(GSON.toJson(widgetsetApiDTO)));
    }

    @Test
    public void testGroupPostProcessing() {
        final BaseApiDTO groupScope = new BaseApiDTO();
        groupScope.setUuid("7");
        groupScope.setClassName(StringConstants.GROUP);
        final Group group = Group.newBuilder()
            .setId(7)
            .build();
        when(groupServiceBackend.getGroups(any())).thenReturn(Collections.singletonList(group));

        final GroupApiDTO mappedGroup = new GroupApiDTO();
        when(groupMapper.toGroupApiDto(group)).thenReturn(mappedGroup);

        // Two widgets, both scoped to the one group.
        final WidgetApiDTO widget1 = new WidgetApiDTO();
        widget1.setScope(groupScope);

        final WidgetApiDTO widget2 = new WidgetApiDTO();
        widget2.setScope(groupScope);

        // Act
        final List<WidgetApiDTO> widgets = widgetsetMapper.postProcessWidgets(widget1, widget2);

        // Assert
        verify(groupServiceBackend).getGroups(GetGroupsRequest.newBuilder()
            .addId(7)
            .build());

        assertThat(widgets.size(), is(2));
        assertThat(widgets.get(0), is(widget1));
        assertThat(widgets.get(0).getScope(), is(mappedGroup));
        assertThat(widgets.get(1), is(widget2));
        assertThat(widgets.get(1).getScope(), is(mappedGroup));
    }

    /**
     * Test that multiple groups get fetched properly in a single call.
     */
    @Test
    public void testMultiGroupPostProcessing() {
        // Arrange
        final BaseApiDTO groupScope1 = new BaseApiDTO();
        groupScope1.setUuid("7");
        groupScope1.setClassName(StringConstants.GROUP);

        final BaseApiDTO groupScope2 = new BaseApiDTO();
        groupScope2.setUuid("8");
        groupScope2.setClassName(StringConstants.GROUP);

        final Group group1 = Group.newBuilder()
            .setId(7)
            .build();

        final Group group2 = Group.newBuilder()
            .setId(8)
            .build();

        final GroupApiDTO mappedGroup1 = new GroupApiDTO();
        when(groupMapper.toGroupApiDto(group1)).thenReturn(mappedGroup1);

        final GroupApiDTO mappedGroup2 = new GroupApiDTO();
        when(groupMapper.toGroupApiDto(group2)).thenReturn(mappedGroup2);

        when(groupServiceBackend.getGroups(any())).thenReturn(Arrays.asList(group1, group2));

        final WidgetApiDTO widget1 = new WidgetApiDTO();
        widget1.setScope(groupScope1);

        final WidgetApiDTO widget2 = new WidgetApiDTO();
        widget2.setScope(groupScope2);

        // Act
        final List<WidgetApiDTO> widgets = widgetsetMapper.postProcessWidgets(widget1, widget2);

        // Assert
        verify(groupServiceBackend).getGroups(GetGroupsRequest.newBuilder()
            .addId(7)
            .addId(8)
            .build());

        assertThat(widgets.size(), is(2));
        assertThat(widgets.get(0), is(widget1));
        assertThat(widgets.get(0).getScope(), is(mappedGroup1));
        assertThat(widgets.get(1), is(widget2));
        assertThat(widgets.get(1).getScope(), is(mappedGroup2));
    }

    @Test
    public void testGroupPostProcessingException() {
        // Arrange
        final BaseApiDTO groupScope = new BaseApiDTO();
        groupScope.setUuid("7");
        groupScope.setClassName(StringConstants.GROUP);
        when(groupServiceBackend.getGroupsError(any()))
            .thenReturn(Optional.of(Status.INTERNAL.asException()));

        final WidgetApiDTO widget1 = new WidgetApiDTO();
        widget1.setScope(groupScope);

        // Act
        final List<WidgetApiDTO> widgets = widgetsetMapper.postProcessWidgets(widget1);

        // Assert
        assertThat(widgets.size(), is(1));
        assertThat(widgets.get(0), is(widget1));
        // The original "basic" scope preserved.
        assertThat(widgets.get(0).getScope(), is(groupScope));
    }

    @Test
    public void testClusterPostProcessing() {
        // Arrange
        final BaseApiDTO groupScope = new BaseApiDTO();
        groupScope.setUuid("7");
        groupScope.setClassName(StringConstants.CLUSTER);
        final Group group = Group.newBuilder()
            .setId(7)
            .build();
        when(groupServiceBackend.getGroups(any())).thenReturn(Collections.singletonList(group));

        final GroupApiDTO mappedGroup = new GroupApiDTO();
        when(groupMapper.toGroupApiDto(group)).thenReturn(mappedGroup);

        final WidgetApiDTO widget1 = new WidgetApiDTO();
        widget1.setScope(groupScope);

        // Act
        final List<WidgetApiDTO> widgets = widgetsetMapper.postProcessWidgets(widget1);

        // Assert
        verify(groupServiceBackend).getGroups(GetGroupsRequest.newBuilder()
            .addId(7)
            .build());

        assertThat(widgets.size(), is(1));
        assertThat(widgets.get(0), is(widget1));
        assertThat(widgets.get(0).getScope(), is(mappedGroup));
    }

    @Test
    public void testStorageClusterPostProcessing() {
        // Arrange
        final BaseApiDTO groupScope = new BaseApiDTO();
        groupScope.setUuid("7");
        groupScope.setClassName(StringConstants.STORAGE_CLUSTER);
        final Group group = Group.newBuilder()
            .setId(7)
            .build();
        when(groupServiceBackend.getGroups(any())).thenReturn(Collections.singletonList(group));

        final GroupApiDTO mappedGroup = new GroupApiDTO();
        when(groupMapper.toGroupApiDto(group)).thenReturn(mappedGroup);

        final WidgetApiDTO widget1 = new WidgetApiDTO();
        widget1.setScope(groupScope);

        // Act
        final List<WidgetApiDTO> widgets = widgetsetMapper.postProcessWidgets(widget1);

        // Assert
        verify(groupServiceBackend).getGroups(GetGroupsRequest.newBuilder()
            .addId(7)
            .build());

        assertThat(widgets.size(), is(1));
        assertThat(widgets.get(0), is(widget1));
        assertThat(widgets.get(0).getScope(), is(mappedGroup));
    }

    /**
     * Everty WidgetsetApiDTO must have username and widgets fields.
     * @return a WidgetSetApiDTO object initialized with the required set of fields.
     */
    private WidgetsetApiDTO getBaseWidgetsetApiDTO() {
        WidgetsetApiDTO widgetsetApiDTO = new WidgetsetApiDTO();
        widgetsetApiDTO.setUsername(DEFAULT_USER_ID_STRING);
        widgetsetApiDTO.setWidgets(SAMPLE_WIDGETS);
        return widgetsetApiDTO;
    }

    /**
     * Sample Widgets definition (with two widgets) captured from the UI
     */
    private static final String SAMPLE_WIDGETS_STRING = "[\n" +
            "    {\n" +
            "      \"column\": 0, \n" +
            "      \"displayName\": \"RISKS_WIDGET_TITLE\", \n" +
            "      \"row\": 0, \n" +
            "      \"scope\": {\n" +
            "        \"className\": \"Market\", \n" +
            "        \"displayName\": \"Global Environment\", \n" +
            "        \"uuid\": \"Market\"\n" +
            "      }, \n" +
            "      \"sizeColumns\": 2, \n" +
            "      \"sizeRows\": 8, \n" +
            "      \"type\": \"risks\", \n" +
            "      \"widgetElements\": [\n" +
            "        {\n" +
            "          \"column\": 0, \n" +
            "          \"properties\": {\n" +
            "            \"chartType\": \"TEXT\", \n" +
            "            \"directive\": \"risks-summary\", \n" +
            "            \"overrideScope\": \"false\", \n" +
            "            \"show\": \"true\", \n" +
            "            \"widgetScopeName\": \"Global Environment\"\n" +
            "          }, \n" +
            "          \"row\": 0, \n" +
            "          \"type\": \"SUMMARY\"\n" +
            "        }\n" +
            "      ]\n" +
            "    }, \n" +
            "    {\n" +
            "      \"column\": 0, \n" +
            "      \"displayName\": \"All Actions\", \n" +
            "      \"endPeriod\": \"1D\", \n" +
            "      \"row\": 0, \n" +
            "      \"scope\": {\n" +
            "        \"className\": \"Market\", \n" +
            "        \"displayName\": \"Global Environment\", \n" +
            "        \"uuid\": \"Market\"\n" +
            "      }, \n" +
            "      \"sizeColumns\": 6, \n" +
            "      \"sizeRows\": 8, \n" +
            "      \"startPeriod\": \"-7D\", \n" +
            "      \"type\": \"actions\", \n" +
            "      \"widgetElements\": [\n" +
            "        {\n" +
            "          \"column\": 1, \n" +
            "          \"properties\": {\n" +
            "            \"chartType\": \"Stacked Bar Chart\", \n" +
            "            \"directive\": \"actions-chart\", \n" +
            "            \"displayParamName\": \"All Actions\", \n" +
            "            \"overrideScope\": \"false\", \n" +
            "            \"show\": \"true\", \n" +
            "            \"showAvgOpCostVMScope\": \"false\", \n" +
            "            \"showBudget\": \"false\", \n" +
            "            \"showUtilization\": \"false\", \n" +
            "            \"widgetScopeName\": \"Global Environment\"\n" +
            "          }, \n" +
            "          \"row\": 0, \n" +
            "          \"type\": \"CHART\"\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  ]\n";

    private static final Gson GSON = ComponentGsonFactory.createGson();
    private final static List<WidgetApiDTO> SAMPLE_WIDGETS;
    static {
        SAMPLE_WIDGETS = Arrays.asList(
                GSON.fromJson(SAMPLE_WIDGETS_STRING, WidgetApiDTO[].class));
    }

}
