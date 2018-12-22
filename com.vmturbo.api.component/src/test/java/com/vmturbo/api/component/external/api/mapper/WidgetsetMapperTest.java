package com.vmturbo.api.component.external.api.mapper;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.google.gson.Gson;

import com.vmturbo.api.component.external.api.mapper.WidgetsetMapper;
import com.vmturbo.api.dto.widget.WidgetApiDTO;
import com.vmturbo.api.dto.widget.WidgetsetApiDTO;
import com.vmturbo.common.protobuf.widgets.Widgets.Widgetset;
import com.vmturbo.components.api.ComponentGsonFactory;

/**
 * Test conversion between WidgetsetApiDTO (external) and Widgetset (internal protobuf)
 **/
public class WidgetsetMapperTest {

    public static final String DEFAULT_WIDGETSET_ID = "1";
    public static final String DEFAULT_USER_ID_STRING = "2";
    public static final long DEFAULT_USER_ID = 2L;

    @Test
    public void testMapToWidgetsetDTOWithUuid() {
        // Arrange
        WidgetsetApiDTO widgetsetApiDTO = getBaseWidgetsetApiDTO();
        widgetsetApiDTO.setUuid(DEFAULT_USER_ID_STRING);
        // Act
        Widgetset result = WidgetsetMapper.fromUiWidgetset(widgetsetApiDTO);
        // Assert
        assertTrue(result.hasOid());
        assertThat(result.getOid(), equalTo(DEFAULT_USER_ID));
    }

    @Test
    public void testMapToWidgetsetDTOWithNoUuid() {
        // Arrange
        WidgetsetApiDTO widgetsetApiDTO = getBaseWidgetsetApiDTO();
        // Act
        Widgetset result = WidgetsetMapper.fromUiWidgetset(widgetsetApiDTO);
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
        final Widgetset intermediate = WidgetsetMapper.fromUiWidgetset(widgetsetApiDTO);
        WidgetsetApiDTO answer = WidgetsetMapper.toUiWidgetset(intermediate);
        // Assert
        assertThat(GSON.toJson(answer), equalTo(GSON.toJson(widgetsetApiDTO)));
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
