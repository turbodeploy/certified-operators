package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.vmturbo.api.dto.widget.WidgetApiDTO;
import com.vmturbo.api.dto.widget.WidgetsetApiDTO;
import com.vmturbo.common.protobuf.widgets.Widgets;
import com.vmturbo.common.protobuf.widgets.Widgets.Widgetset;

/**
 * Mappings between external REST API {@link WidgetsetApiDTO} and protobuf {@link Widgetset}.
 **/
public class WidgetsetMapper {

    // Jackson converter to go to and from JSON strings
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger logger = LogManager.getLogger();

    /**
     * Convert an External API {@link WidgetsetApiDTO} to an internal protobuf {@link Widgetset}.
     * @param widgetsetApiDTO the external API {@link WidgetsetApiDTO} to convert
     * @return an internal protobuf {@link Widgetset} initialized from the given {@link WidgetsetApiDTO}
     */
    public static Widgetset fromUiWidgetset(WidgetsetApiDTO widgetsetApiDTO) {
        Widgetset.Builder answer = Widgetset.newBuilder();
        if (widgetsetApiDTO.getUuid() != null) {
            try {
                answer.setOid(Long.valueOf(widgetsetApiDTO.getUuid()));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid uuid " + widgetsetApiDTO.getUuid());
            }
        }
        if (widgetsetApiDTO.getUsername() == null) {
            throw new IllegalArgumentException("Owner userid for a widgetset " +
                    widgetsetApiDTO.getUuid() + " must not be empty");
        }
        answer.setOwnerUserid(widgetsetApiDTO.getUsername());

        // add the widgetset info
        answer.setInfo(fromUiWidgetsetApiDTO(widgetsetApiDTO));

        return answer.build();
    }

    /**
     * Convert from an external API representation of a widgetset ({@link WidgetsetApiDTO} into an
     * internal protobuf for the widgetset info {@link Widgets.WidgetsetInfo}. Note that the
     * widgetset oid and owner id are part of the {@link Widgets.Widgetset} wrapper, not the
     * WidgetsetInfo.
     *
     * @param widgetsetApiDTO an external API {@link WidgetsetApiDTO} representation for a Widgetset.
     * @return an {@link Widgets.WidgetsetInfo} protobuf built from the given {@link WidgetsetApiDTO}
     */
    public static Widgets.WidgetsetInfo fromUiWidgetsetApiDTO(WidgetsetApiDTO widgetsetApiDTO) {
        // populate the WidgetsetInfo for this Widgetset
        Widgets.WidgetsetInfo.Builder infoBuilder = Widgets.WidgetsetInfo.newBuilder();
        if (widgetsetApiDTO.getDisplayName() != null) {
            infoBuilder.setDisplayName(widgetsetApiDTO.getDisplayName());
        }
        if (widgetsetApiDTO.getClassName() != null) {
            infoBuilder.setClassName(widgetsetApiDTO.getClassName());
        }
        if (widgetsetApiDTO.getCategory() != null) {
            infoBuilder.setCategory(widgetsetApiDTO.getCategory());
        }
        if (widgetsetApiDTO.getScope() != null) {
            infoBuilder.setScope(widgetsetApiDTO.getScope());
        }
        if (widgetsetApiDTO.getScopeType() != null) {
            infoBuilder.setScopeType(widgetsetApiDTO.getScopeType());
        }
        infoBuilder.setSharedWithAllUsers(widgetsetApiDTO.isSharedWithAllUsers());
        if (widgetsetApiDTO.getWidgets() == null) {
            throw new IllegalArgumentException("widgets definiton for " + widgetsetApiDTO.getUuid() +
                     "is null.");
        }
        String widgetsString;
        try {
            widgetsString = objectMapper.writeValueAsString(widgetsetApiDTO.getWidgets());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Error converting widgets to JSON String");
        }
        infoBuilder.setWidgets(widgetsString);
        return infoBuilder.build();
    }

    /**
     * Convert an internal protobuf {@link Widgetset} to an External API {@link WidgetsetApiDTO} .
     *
     * @param widgetset an internal protobuf {@link Widgetset} to convert
     * @return an external API {@link WidgetsetApiDTO} initialized from the given {@link Widgetset}
     */
    public static WidgetsetApiDTO toUiWidgetset(Widgetset widgetset) {
        WidgetsetApiDTO answer = new WidgetsetApiDTO();
        if (widgetset.hasOid()) {
            answer.setUuid(Long.toString(widgetset.getOid()));
        } else {
            throw new IllegalArgumentException("OID for a widgetset " +
                    widgetset.getOid() + " must not be empty");
        }
        answer.setUuid(Long.toString(widgetset.getOid()));
        if (widgetset.hasOwnerUserid()) {
            answer.setUsername(widgetset.getOwnerUserid());
        }
        if (widgetset.hasInfo()) {
            Widgets.WidgetsetInfo widgetsetInfo = widgetset.getInfo();
            if (widgetsetInfo.hasDisplayName()) {
                answer.setDisplayName(widgetsetInfo.getDisplayName());
            }
            if (widgetsetInfo.hasClassName()) {
                answer.setClassName(widgetsetInfo.getClassName());
            }
            if (widgetsetInfo.hasCategory()) {
                answer.setCategory(widgetsetInfo.getCategory());
            }
            if (widgetsetInfo.hasScope()) {
                answer.setScope(widgetsetInfo.getScope());
            }
            if (widgetsetInfo.hasScopeType()) {
                answer.setScopeType(widgetsetInfo.getScopeType());
            }
            answer.setSharedWithAllUsers(widgetsetInfo.getSharedWithAllUsers());
            if (!widgetsetInfo.hasWidgets()) {
                throw new IllegalArgumentException("widgets definiton for widgetset " +
                        widgetset.getOid() + " is empty.");
            }
            WidgetApiDTO[] widgets;
            try {
                widgets = objectMapper.readValue(widgetsetInfo.getWidgets(), WidgetApiDTO[].class);
            } catch (IOException e) {
                throw new IllegalArgumentException("Error parsing widgets data from JSON String");
            }
            answer.setWidgets(Arrays.asList(widgets));
        } else {
            logger.warn("Widgetset {} has no info to be converted.", widgetset.getOid());
        }

        return answer;
    }
}
