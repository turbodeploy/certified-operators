package com.vmturbo.api.component.external.api.mapper;

import java.util.Arrays;

import com.google.gson.Gson;

import com.vmturbo.api.dto.widget.WidgetApiDTO;
import com.vmturbo.api.dto.widget.WidgetsetApiDTO;
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
        if (widgetsetApiDTO.getUsername() == null) {
            throw new IllegalArgumentException("Username for a widgetset " +
                    widgetsetApiDTO.getUuid() + " must not be empty");
        }
        try {
            answer.setOwnerOid(Long.valueOf(widgetsetApiDTO.getUsername()));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid owner uuid " +
                    widgetsetApiDTO.getUsername());
        }
        if (widgetsetApiDTO.getCategory() != null) {
            answer.setCategory(widgetsetApiDTO.getCategory());
        }
        if (widgetsetApiDTO.getScope() != null) {
            answer.setScope(widgetsetApiDTO.getScope());
        }
        if (widgetsetApiDTO.getScopeType() != null) {
            answer.setScopeType(widgetsetApiDTO.getScopeType());
        }
        answer.setSharedWithAllUsers(widgetsetApiDTO.isSharedWithAllUsers());
        if (widgetsetApiDTO.getWidgets() == null) {
            throw new IllegalArgumentException("widgets definiton for " + widgetsetApiDTO.getUuid() +
                    "is null.");
        }
        Gson gson = new Gson();
        String widgetsString = gson.toJson(widgetsetApiDTO.getWidgets());
        answer.setWidgets(widgetsString);

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
        answer.setUuid(Long.toString(widgetset.getOid()));
        if (widgetset.hasOwnerOid()) {
            answer.setUsername(Long.toString(widgetset.getOwnerOid()));
        }
        if (widgetset.hasCategory()) {
            answer.setCategory(widgetset.getCategory());
        }
        if (widgetset.hasScope()) {
            answer.setScope(widgetset.getScope());
        }
        if (widgetset.hasScopeType()) {
            answer.setScopeType(widgetset.getScopeType());
        }
        answer.setSharedWithAllUsers(widgetset.getSharedWithAllUsers());
        if (!widgetset.hasWidgets()) {
            throw new IllegalArgumentException("widgets definiton for " + widgetset.getOid() +
                    "is null.");
        }
        WidgetApiDTO[] widgets = GSON.fromJson(widgetset.getWidgets(), WidgetApiDTO[].class);
        answer.setWidgets(Arrays.asList(widgets));

        return answer;
    }
}
