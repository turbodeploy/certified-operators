package com.vmturbo.api.component.external.api.mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.widget.WidgetApiDTO;
import com.vmturbo.api.dto.widget.WidgetsetApiDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.widgets.Widgets;
import com.vmturbo.common.protobuf.widgets.Widgets.Widgetset;
/**
 * Mappings between external REST API {@link WidgetsetApiDTO} and protobuf {@link Widgetset}.
 **/
public class WidgetsetMapper {

    // Jackson converter to go to and from JSON strings
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger logger = LogManager.getLogger();

    private final GroupMapper groupMapper;

    private final GroupServiceBlockingStub groupRpcService;

    public WidgetsetMapper(@Nonnull final GroupMapper groupMapper,
                           @Nonnull final GroupServiceBlockingStub groupRpcService) {
        this.groupMapper = Objects.requireNonNull(groupMapper);
        this.groupRpcService = Objects.requireNonNull(groupRpcService);
    }

    /**
     * Convert an External API {@link WidgetsetApiDTO} to an internal protobuf {@link Widgetset}.
     * @param widgetsetApiDTO the external API {@link WidgetsetApiDTO} to convert
     * @return an internal protobuf {@link Widgetset} initialized from the given {@link WidgetsetApiDTO}
     */
    @Nonnull
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
    @Nonnull
    public Widgets.WidgetsetInfo fromUiWidgetsetApiDTO(WidgetsetApiDTO widgetsetApiDTO) {
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
    @Nonnull
    public WidgetsetApiDTO toUiWidgetset(Widgetset widgetset) {
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
            answer.setWidgets(postProcessWidgets(widgets));
        } else {
            logger.warn("Widgetset {} has no info to be converted.", widgetset.getOid());
        }

        return answer;
    }

    /**
     * Apply post-processing to the list of widgets de-serialized from the JSON strings saved
     * to persistent storage. This post-processing is where we fill in any information that was
     * not saved, but can be derived from saved properties.
     *
     * @param widgets The de-serialized {@link WidgetApiDTO}s. This input will get modified.
     * @return A list of {@link WidgetApiDTO}s that can be returned to the client.
     */
    @Nonnull
    @VisibleForTesting
    List<WidgetApiDTO> postProcessWidgets(@Nonnull final WidgetApiDTO... widgets) {
        // A multimap of (group oid) -> (widget API DTOs scoped to the group)
        // This is to get all referenced groups in a single RPC call later.
        // We need the referenced groups so we can replace the "BaseApiDTO"s saved in the widget
        // with a full GroupApiDTO - including things like the group entity types.
        final Multimap<Long, WidgetApiDTO> groupScopedWidgets = HashMultimap.create();

        final List<WidgetApiDTO> retList = Arrays.asList(widgets);

        retList.forEach(widget -> {
            // Collect group UUIDs into the multimap.
            if (GroupMapper.GROUP_CLASSES.contains(widget.getScope().getClassName())) {
                final String scopeUuid = widget.getScope().getUuid();
                try {
                    groupScopedWidgets.put(Long.parseLong(scopeUuid), widget);
                } catch (NumberFormatException e) {
                    // TODO (roman, Dec 21 2018): Widgets may be scoped to "magic" UUIDs - maybe
                    // we should consider using the MagicScopeGateway here.
                    logger.error("Unable to format UUID {} as an oid for widget {}",
                        scopeUuid, widget.getDisplayName());
                }
            }
        });

        if (!groupScopedWidgets.isEmpty()) {
            try {
                // Get all groups referenced by the widgets.
                groupRpcService.getGroups(GetGroupsRequest.newBuilder()
                                .setGroupFilter(GroupFilter.newBuilder()
                                        .addAllId(groupScopedWidgets.keySet())
                                        )
                                .build()
                    )
                    .forEachRemaining(group -> {
                        final GroupApiDTO groupApiDTO = groupMapper.toGroupApiDto(group);
                        // For each widget scoped to this group, replace the base scope DTO with the
                        // group's DTO.
                        groupScopedWidgets.get(group.getId()).forEach(widgetWithGroupScope ->
                            widgetWithGroupScope.setScope(groupApiDTO));
                    });
            } catch (StatusRuntimeException e) {
                logger.error("Failed to retrieve the groups that the following widgets are " +
                    " scoped to. Widgets may not render properly: {}",
                        groupScopedWidgets.values()
                            .stream()
                            .map(WidgetApiDTO::getClassName)
                            .collect(Collectors.joining(", ")));
            }
        }

        return retList;
    }
}
