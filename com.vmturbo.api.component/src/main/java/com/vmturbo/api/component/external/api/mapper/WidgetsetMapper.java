package com.vmturbo.api.component.external.api.mapper;

import java.util.Arrays;

import com.google.gson.Gson;

import com.vmturbo.api.dto.widget.WidgetApiDTO;
import com.vmturbo.api.dto.widget.WidgetsetApiDTO;
import com.vmturbo.common.protobuf.widgets.Widgets;
import com.vmturbo.common.protobuf.widgets.Widgets.Widgetset;
import com.vmturbo.components.api.ComponentGsonFactory;

/**
 * Mappings between external REST API {@link WidgetsetApiDTO} and protobuf {@link Widgetset}.
 **/
public class WidgetsetMapper {

    // Gson used to convert widgets structure to and from string
    private static final Gson GSON = ComponentGsonFactory.createGson();

    /**
     * Convert an External API {@link WidgetsetApiDTO} to an internal protobuf {@link Widgetset}.
     * @param widgetsetApiDTO the external API {@link WidgetsetApiDTO} to convert
     * @return an internal protobuf {@link Widgetset} initialized from the given {@link WidgetsetApiDTO}
     */
    public Widgetset fromUiWidgetset(WidgetsetApiDTO widgetsetApiDTO) {
        Widgetset.Builder answer = Widgetset.newBuilder();
        if (widgetsetApiDTO.getUuid() != null) {
            try {
                answer.setOid(Long.valueOf(widgetsetApiDTO.getUuid()));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid uuid " + widgetsetApiDTO.getUuid());
            }
        }
        try {
            answer.setOwnerOid(Long.valueOf(widgetsetApiDTO.getUsername()));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid owner uuid " +
                    widgetsetApiDTO.getUsername());
        }
        // populate the WidgetsetInfo for this Widgetset
        Widgets.WidgetsetInfo.Builder infoBuilder = Widgets.WidgetsetInfo.newBuilder();
        if (widgetsetApiDTO.getUsername() == null) {
            throw new IllegalArgumentException("Owner uuid for a widgetset " +
                    widgetsetApiDTO.getUuid() + " must not be empty");
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
        Gson gson = new Gson();
        String widgetsString = gson.toJson(widgetsetApiDTO.getWidgets());
        infoBuilder.setWidgets(widgetsString);
        answer.setInfo(infoBuilder.build());

        return answer.build();
    }

    /**
     * Convert an internal protobuf {@link Widgetset} to an External API {@link WidgetsetApiDTO} .
     * @param widgetset an internal protobuf {@link Widgetset} to convert
     * @return an external API {@link WidgetsetApiDTO} initialized from the given {@link Widgetset}
     */
    public WidgetsetApiDTO toUiWidgetset(Widgetset widgetset) {
        WidgetsetApiDTO answer = new WidgetsetApiDTO();
        if (widgetset.hasOid()) {
            answer.setUuid(Long.toString(widgetset.getOid()));
        }
        if (!widgetset.hasOwnerOid()) {
            throw new IllegalArgumentException("Owner OID for a widgetset " +
                    widgetset.getOid() + " must not be empty");
        }
        answer.setUsername(Long.toString(widgetset.getOwnerOid()));
        answer.setUuid(Long.toString(widgetset.getOid()));
        if (widgetset.hasInfo()) {
            Widgets.WidgetsetInfo widgetsetInfo = widgetset.getInfo();
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
            WidgetApiDTO[] widgets = GSON.fromJson(widgetsetInfo.getWidgets(), WidgetApiDTO[].class);
            answer.setWidgets(Arrays.asList(widgets));
        }

        return answer;
    }
}
