package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.TopologyDataDefinitionMapper;
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

    private static final Logger LOGGER = LogManager.getLogger();

    private final TopologyDataDefinitionServiceBlockingStub tddStub;
    private final TopologyDataDefinitionMapper tddMapper;

    /**
     * Constructor for {@link TopologyDataDefinitionService}.
     *
     * @param tddStub blocking stub for the Topology Data Definition service
     * @param tddMapper mapper for XL and API DTO objects
     */
    public TopologyDataDefinitionService(@Nonnull final TopologyDataDefinitionServiceBlockingStub tddStub,
                                         @Nonnull final TopologyDataDefinitionMapper tddMapper) {
        this.tddStub = tddStub;
        this.tddMapper = tddMapper;
    }

    private boolean hasDefinition(@Nonnull GetTopologyDataDefinitionResponse response) {
        return response.hasTopologyDataDefinition()
            && response.getTopologyDataDefinition().hasDefinition();
    }

    /**
     * Get all topology definitions.
     *
     * @return list of topology definitions API DTOs
     */
    @Override
    public List<TopologyDataDefinitionApiDTO> getAllTopologyDefinitions() {
        Iterator<GetTopologyDataDefinitionResponse> responses = tddStub
            .getAllTopologyDataDefinitions(GetTopologyDataDefinitionsRequest.getDefaultInstance());
        List<TopologyDataDefinitionApiDTO> definitions = new ArrayList<>();
        while (responses.hasNext()) {
            GetTopologyDataDefinitionResponse response = responses.next();
            if (!hasDefinition(response)) {
                LOGGER.warn("No topology definition found for response: {}", response.toString());
                continue;
            }
            TopologyDataDefinitionEntry entry = response.getTopologyDataDefinition();
            checkId(entry);
            try {
                TopologyDataDefinitionApiDTO dto = tddMapper.convertTopologyDataDefinition(
                        entry.getDefinition());
                dto.setUuid(String.valueOf(entry.getId()));
                definitions.add(dto);
            } catch (Exception e) {
                LOGGER.warn("Cannot convert topology definition to API DTO: {}",
                        response.getTopologyDataDefinition().getDefinition().toString(), e);
            }
        }

        return definitions;
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
        try {
            final TopologyDataDefinitionID definitionID = TopologyDataDefinitionID
                .newBuilder().setId(parseId(id)).build();
            final GetTopologyDataDefinitionResponse response = tddStub
                .getTopologyDataDefinition(definitionID);
            if (!hasDefinition(response)) {
                throw definitionNotFound(id);
            }
            return entryToApiDTO(response.getTopologyDataDefinition());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(io.grpc.Status.NOT_FOUND.getCode())) {
                throw definitionNotFound(id);
            } else {
                throw e;
            }
        }
    }

    private UnknownObjectException definitionNotFound(@Nullable String id) {
        String errorText = String.format("Cannot find topology data definition by ID: %s", id);
        LOGGER.error(errorText);
        return new UnknownObjectException(errorText);
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
        CreateTopologyDataDefinitionResponse response;
        try {
            response = tddStub.createTopologyDataDefinition(
                    CreateTopologyDataDefinitionRequest.newBuilder()
                            .setTopologyDataDefinition(tddMapper
                                    .convertTopologyDataDefinitionApiDTO(topologyDataDefinitionApiDTO))
                            .build()
            );
        } catch (Exception e) {
            LOGGER.error(e);
            throw new OperationFailedException("Create operation failed", e);
        }
        if (!response.hasTopologyDataDefinition() || !response.getTopologyDataDefinition().hasDefinition()) {
            final String errorText = String.format("Cannot create topology data definition: %s",
                    topologyDataDefinitionApiDTO.toString());
            LOGGER.error(errorText);
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
        UpdateTopologyDataDefinitionResponse response;
        try {
            response = tddStub.updateTopologyDataDefinition(
                    UpdateTopologyDataDefinitionRequest.newBuilder()
                            .setId(parseId(id))
                            .setTopologyDataDefinition(tddMapper
                                    .convertTopologyDataDefinitionApiDTO(topologyDataDefinitionApiDTO))
                            .build()
            );
        } catch (Exception e) {
            LOGGER.error(e);
            throw new OperationFailedException("Update operation failed", e);
        }
        if (!response.hasUpdatedTopologyDataDefinition()
                || !response.getUpdatedTopologyDataDefinition().hasDefinition()) {
            final String errorText = String.format("Cannot update topology definition by ID: %s", id);
            LOGGER.error(errorText);
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
            response = tddStub.deleteTopologyDataDefinition(
                TopologyDataDefinitionID.newBuilder().setId(parseId(id)).build()
            );
        } catch (Exception e) {
            LOGGER.error(e);
            throw new OperationFailedException("Deletion operation failed", e);
        }
        if (!response.hasDeleted() || !response.getDeleted()) {
            final String errorText = String.format("Cannot delete topology definition by ID: %s", id);
            LOGGER.error(errorText);
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
                = tddMapper.convertTopologyDataDefinition(
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
            LOGGER.error(errorText);
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
            LOGGER.error(e.getLocalizedMessage(), e);
            throw new UnknownObjectException(String.format("Cannot parse topology data definition ID: %s", id), e);
        }
    }

}
