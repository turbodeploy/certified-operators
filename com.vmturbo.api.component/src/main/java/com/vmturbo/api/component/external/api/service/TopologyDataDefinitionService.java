package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.TopologyDataDefinitionMapper;
import com.vmturbo.api.dto.topologydefinition.TopoDataDefContextBasedApiDTO;
import com.vmturbo.api.dto.topologydefinition.TopologyDataDefinitionApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.ITopologyDefinitionService;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.CreateTopologyDataDefinitionRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.CreateTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.DeleteTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.GetTopologyDataDefinitionsRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinitionEntry;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinitionID;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.UpdateTopologyDataDefinitionRequest;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.UpdateTopologyDataDefinitionResponse;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionServiceGrpc.TopologyDataDefinitionServiceBlockingStub;

/**
 * Topology Data Definition service implementation.
 */
public class TopologyDataDefinitionService implements ITopologyDefinitionService {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyDataDefinitionServiceBlockingStub topologyDataDefinitionServiceBlockingStub;
    private final TopologyDataDefinitionMapper topologyDataDefinitionMapper;
    private final boolean enableContextBasedAtd;

    /**
     * Constructor for {@link TopologyDataDefinitionService}.
     *
     * @param topologyDataDefinitionServiceBlockingStub blocking stub for the service
     * @param topologyDataDefinitionMapper mapper for XL and API DTO objects
     * @param enableContextBasedAtd enable or disable context-based ATDs.
     */
    public TopologyDataDefinitionService(@Nonnull final TopologyDataDefinitionServiceBlockingStub topologyDataDefinitionServiceBlockingStub,
                                         @Nonnull final TopologyDataDefinitionMapper topologyDataDefinitionMapper,
                                         final boolean enableContextBasedAtd) {
        this.topologyDataDefinitionServiceBlockingStub = topologyDataDefinitionServiceBlockingStub;
        this.topologyDataDefinitionMapper = topologyDataDefinitionMapper;
        this.enableContextBasedAtd = enableContextBasedAtd;
    }

    /**
     * Get all topology definitions.
     *
     * @return list of topology definitions API DTOs
     */
    @Override
    public List<TopologyDataDefinitionApiDTO> getAllTopologyDefinitions() {
        GetTopologyDataDefinitionsRequest request = GetTopologyDataDefinitionsRequest.getDefaultInstance();
        Iterator<GetTopologyDataDefinitionResponse> responses
                = topologyDataDefinitionServiceBlockingStub.getAllTopologyDataDefinitions(request);
        List<TopologyDataDefinitionApiDTO> definitions = new ArrayList<>();
        while (responses.hasNext()) {
            GetTopologyDataDefinitionResponse response = responses.next();
            if (!response.hasTopologyDataDefinition() || !response.getTopologyDataDefinition().hasDefinition()) {
                logger.warn("No topology definition found for response: {}", response.toString());
                continue;
            }
            TopologyDataDefinitionEntry entry = response.getTopologyDataDefinition();
            if (isContextBasedDefinition(entry) && !enableContextBasedAtd) {
                // Do not return context based ATDs if feature flag is disabled
                continue;
            }
            checkId(entry);
            try {
                TopologyDataDefinitionApiDTO dto = topologyDataDefinitionMapper.convertTopologyDataDefinition(
                        entry.getDefinition());
                dto.setUuid(String.valueOf(entry.getId()));
                definitions.add(dto);
            } catch (Exception e) {
                logger.warn("Cannot convert topology definition to API DTO: {}",
                        response.getTopologyDataDefinition().getDefinition().toString(), e);
            }
        }

        return definitions;
    }

    /**
     * Checks {@link TopologyDataDefinitionEntry} on context based.
     *
     * @param entry topology definition entry to check on context based
     * @return Returns {@code true} if it's a context based definition.
     */
    private static boolean isContextBasedDefinition(@Nonnull TopologyDataDefinitionEntry entry) {
        return entry.hasDefinition()
                && entry.getDefinition().hasManualEntityDefinition()
                && entry.getDefinition().getManualEntityDefinition().getContextBased();
    }

    /**
     * Get topology definition by ID.
     *
     * @param id id of topology definition as a string
     * @return topology definition API DTO
     * @throws UnknownObjectException exception if id is incorrect
     */
    @Override
    public TopologyDataDefinitionApiDTO getTopologyDefinition(String id) throws UnknownObjectException {
        GetTopologyDataDefinitionResponse response =
                topologyDataDefinitionServiceBlockingStub.getTopologyDataDefinition(TopologyDataDefinitionID.newBuilder()
                        .setId(parseId(id)).build());
        if (!response.hasTopologyDataDefinition() || !response.getTopologyDataDefinition().hasDefinition()
                || (isContextBasedDefinition(response.getTopologyDataDefinition()) && !enableContextBasedAtd) ) {
            String errorText = String.format("Cannot find topology data definition by ID: %s", id);
            logger.error(errorText);
            throw new UnknownObjectException(errorText);
        }
        return entryToApiDTO(response.getTopologyDataDefinition());
    }

    /**
     * Create topology definition.
     *
     * @param topologyDataDefinitionApiDTO topology definition API DTO
     * @return created topology definition API DTO
     * @throws OperationFailedException exception if operating is failed
     * @throws IllegalArgumentException exception if topology definition API DTO is inconsistent
     */
    @Override
    public TopologyDataDefinitionApiDTO createTopologyDefinition(TopologyDataDefinitionApiDTO topologyDataDefinitionApiDTO)
            throws OperationFailedException {
        CreateTopologyDataDefinitionResponse response = CreateTopologyDataDefinitionResponse.getDefaultInstance();
        try {
            response = topologyDataDefinitionServiceBlockingStub.createTopologyDataDefinition(
                    CreateTopologyDataDefinitionRequest.newBuilder()
                            .setTopologyDataDefinition(topologyDataDefinitionMapper
                                    .convertTopologyDataDefinitionApiDTO(topologyDataDefinitionApiDTO))
                            .build()
            );
        } catch (Exception e) {
            logger.error(e);
            throw new OperationFailedException("Create operation failed", e);
        }
        if (!response.hasTopologyDataDefinition() || !response.getTopologyDataDefinition().hasDefinition()) {
            final String errorText = String.format("Cannot create topology data definition: %s",
                    topologyDataDefinitionApiDTO.toString());
            logger.error(errorText);
            throw new OperationFailedException(errorText);
        }
        return entryToApiDTO(response.getTopologyDataDefinition());
    }

    /**
     * Update topology definition.
     *
     * @param id id of topology definition as a string
     * @param topologyDataDefinitionApiDTO topology definition API DTO
     * @return updated topology definition API DTO
     * @throws UnknownObjectException exception if id is incorrect
     * @throws OperationFailedException exception if operating is failed
     * @throws IllegalArgumentException exception if topology definition API DTO is inconsistent
     */
    @Override
    public TopologyDataDefinitionApiDTO editTopologyDefinition(String id, TopologyDataDefinitionApiDTO topologyDataDefinitionApiDTO)
            throws UnknownObjectException, OperationFailedException {
        // Verify that topology definition exists
        getTopologyDefinition(id);
        UpdateTopologyDataDefinitionResponse response = UpdateTopologyDataDefinitionResponse.getDefaultInstance();
        try {
            response = topologyDataDefinitionServiceBlockingStub.updateTopologyDataDefinition(
                    UpdateTopologyDataDefinitionRequest.newBuilder()
                            .setId(parseId(id))
                            .setTopologyDataDefinition(topologyDataDefinitionMapper
                                    .convertTopologyDataDefinitionApiDTO(topologyDataDefinitionApiDTO))
                            .build()
            );
        } catch (Exception e) {
            logger.error(e);
            throw new OperationFailedException("Update operation failed", e);
        }
        if (!response.hasUpdatedTopologyDataDefinition()
                || !response.getUpdatedTopologyDataDefinition().hasDefinition()) {
            final String errorText = String.format("Cannot update topology definition by ID: %s", id);
            logger.error(errorText);
            throw new UnknownObjectException(errorText);
        }
        return entryToApiDTO(response.getUpdatedTopologyDataDefinition());
    }

    /**
     * Delete topology definition.
     *
     * @param id id of topology definition as a string
     * @throws UnknownObjectException exception if id is incorrect
     * @throws OperationFailedException exception if operating is failed
     */
    @Override
    public void deleteTopologyDefinition(String id) throws UnknownObjectException, OperationFailedException {
        // Verify that topology definition exists
        getTopologyDefinition(id);
        DeleteTopologyDataDefinitionResponse response = DeleteTopologyDataDefinitionResponse.getDefaultInstance();
        try {
            response = topologyDataDefinitionServiceBlockingStub.deleteTopologyDataDefinition(
                    TopologyDataDefinitionID.newBuilder()
                            .setId(parseId(id))
                            .build()
            );
        } catch (Exception e) {
            logger.error(e);
            throw new OperationFailedException("Deletion operation failed", e);
        }
        if (!response.hasDeleted() || !response.getDeleted()) {
            final String errorText = String.format("Cannot delete topology definition by ID: %s", id);
            logger.error(errorText);
            throw new UnknownObjectException(errorText);
        }
    }

    /**
     * Transforms entry to API DTO.
     *
     * @param entry {@link TopologyDataDefinitionEntry}
     * @return {@link TopologyDataDefinitionApiDTO}
     */
    private TopologyDataDefinitionApiDTO entryToApiDTO(TopologyDataDefinitionEntry entry) {
        checkId(entry);
        TopologyDataDefinitionApiDTO apiDTO
                = topologyDataDefinitionMapper.convertTopologyDataDefinition(
                entry.getDefinition()
        );
        apiDTO.setUuid(String.valueOf(entry.getId()));
        return apiDTO;
    }

    /**
     * Transforms entry to context-based API DTO.
     *
     * @param entry {@link TopologyDataDefinitionEntry}
     * @return {@link TopoDataDefContextBasedApiDTO}
     */
    private TopoDataDefContextBasedApiDTO entryToContextBasedApiDTO(
                                                TopologyDataDefinitionEntry entry) {
        checkId(entry);
        TopoDataDefContextBasedApiDTO apiDTO
                = topologyDataDefinitionMapper.convertTopologyContextBasedDataDefinition(
                entry.getDefinition()
        );
        apiDTO.setUuid(String.valueOf(entry.getId()));
        return apiDTO;
    }

    /**
     * Checks if {@link TopologyDataDefinitionEntry} has ID.
     *
     * @param entry {@link TopologyDataDefinitionEntry}
     * @throws IllegalStateException exception if entry does not have id
     */
    private void checkId(TopologyDataDefinitionEntry entry) throws IllegalStateException {
        if (!entry.hasId()) {
            final String errorText = String.format("Definition does not have ID: %s",
                    entry.toString());
            logger.error(errorText);
            throw new IllegalStateException(errorText);
        }
    }

    /**
     * Parse Id from string.
     *
     * @param id id of topology definition as a string
     * @return id of topology definition as a long
     * @throws UnknownObjectException exception if id cannot be parsed
     */
    private long parseId(String id) throws UnknownObjectException {
        try {
            return Long.parseLong(id);
        } catch (NumberFormatException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new UnknownObjectException(String.format("Cannot parse topology data definition ID: %s", id), e);
        }
    }

    /**
     * Get all context-based topology definitions.
     *
     * @return list of context-based topology definitions API DTOs
     */
    @Override
    public List<TopoDataDefContextBasedApiDTO> getAllContextBasedTopologyDefinitions() {
        GetTopologyDataDefinitionsRequest request = GetTopologyDataDefinitionsRequest.getDefaultInstance();
        Iterator<GetTopologyDataDefinitionResponse> responses
                = topologyDataDefinitionServiceBlockingStub.getAllTopologyDataDefinitions(request);
        List<TopoDataDefContextBasedApiDTO> definitions = new ArrayList<>();
        while (responses.hasNext()) {
            GetTopologyDataDefinitionResponse response = responses.next();
            if (!response.hasTopologyDataDefinition() || !response.getTopologyDataDefinition().hasDefinition()) {
                logger.warn("No topology definition found for response: {}", response.toString());
                continue;
            }
            TopologyDataDefinitionEntry entry = response.getTopologyDataDefinition();
            if (!isContextBasedDefinition(entry)) {
                // Do not return non context based ATDs until they won't be implemented
                continue;
            }
            checkId(entry);
            try {
                TopoDataDefContextBasedApiDTO dto =
                    topologyDataDefinitionMapper.convertTopologyContextBasedDataDefinition(
                        entry.getDefinition());
                    dto.setUuid(String.valueOf(entry.getId()));
                    dto.setContextBased(true);
                    definitions.add(dto);
            } catch (Exception e) {
                logger.warn("Cannot convert topology definition to API DTO: {}",
                        response.getTopologyDataDefinition().getDefinition().toString(), e);
            }
        }

        return definitions;
    }

    /**
     * Get context-based topology definition by ID.
     *
     * @param id id of context-based topology definition as a string
     * @return context-based topology definition API DTO
     * @throws UnknownObjectException exception if id is incorrect
     */
    @Override
    public TopoDataDefContextBasedApiDTO getContextBasedTopologyDefinition(String id)
            throws UnknownObjectException {
        GetTopologyDataDefinitionResponse response =
                topologyDataDefinitionServiceBlockingStub.getTopologyDataDefinition(TopologyDataDefinitionID.newBuilder()
                        .setId(parseId(id)).build());
        if (!response.hasTopologyDataDefinition() || !response.getTopologyDataDefinition().hasDefinition()) {
            String errorText = String.format("Cannot find topology data definition by ID: %s", id);
            logger.error(errorText);
            throw new UnknownObjectException(errorText);
        }
        return entryToContextBasedApiDTO(response.getTopologyDataDefinition());
    }

    /**
     * Create context-based topology definition.
     *
     * @param topologyDataDefinitionApiDTO context-based topology definition API DTO
     * @return created context-based topology definition API DTO
     * @throws OperationFailedException exception if operating is failed
     * @throws IllegalArgumentException exception if context-based topology definition API DTO is
     * inconsistent.
     */
    @Override
    public TopoDataDefContextBasedApiDTO createContextBasedTopologyDefinition(
                                         TopoDataDefContextBasedApiDTO topologyDataDefinitionApiDTO)
            throws OperationFailedException {
        CreateTopologyDataDefinitionResponse response = CreateTopologyDataDefinitionResponse.getDefaultInstance();
        try {
            response = topologyDataDefinitionServiceBlockingStub.createTopologyDataDefinition(
                    CreateTopologyDataDefinitionRequest.newBuilder()
                            .setTopologyDataDefinition(topologyDataDefinitionMapper
                                    .convertTopologyDataDefinitionApiDTO(topologyDataDefinitionApiDTO))
                            .build()
            );
        } catch (Exception e) {
            logger.error(e);
            throw new OperationFailedException("Create operation failed", e);
        }
        if (!response.hasTopologyDataDefinition() || !response.getTopologyDataDefinition().hasDefinition()) {
            final String errorText = String.format("Cannot create topology data definition: %s",
                    topologyDataDefinitionApiDTO.toString());
            logger.error(errorText);
            throw new OperationFailedException(errorText);
        }
        return entryToContextBasedApiDTO(response.getTopologyDataDefinition());
    }
}
