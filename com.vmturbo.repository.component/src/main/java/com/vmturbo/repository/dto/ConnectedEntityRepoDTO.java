package com.vmturbo.repository.dto;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_EMPTY)
public class ConnectedEntityRepoDTO {
    private Long connectedEntityId;

    private Integer connectedEntityType;

    private Integer connectionType;

    public Long getConnectedEntityId() {
        return connectedEntityId;
    }

    public void setConnectedEntityId(Long connectedEntityId) {
        this.connectedEntityId = connectedEntityId;
    }

    public Integer getConnectedEntityType() {
        return connectedEntityType;
    }

    public void setConnectedEntityType(Integer connectedEntityType) {
        this.connectedEntityType = connectedEntityType;
    }

    public Integer getConnectionType() {
        return connectionType;
    }

    public void setConnectionType(Integer connectionType) {
        this.connectionType = connectionType;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof ConnectedEntityRepoDTO)) return false;

        final ConnectedEntityRepoDTO that = (ConnectedEntityRepoDTO) o;

        if (!Objects.equals(connectionType, that.connectionType)) return false;
        if (!Objects.equals(connectedEntityType, that.connectedEntityType)) return false;
        return Objects.equals(connectedEntityId, that.connectedEntityId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectedEntityId, connectedEntityType, connectionType);
    }

    @Override
    public String toString() {
        return "ConnectedEntityRepoDTO{" +
                "connectedEntityId=" + connectedEntityId +
                ", connectedEntityType=" + connectedEntityType +
                ", connectionType=" + connectionType +
                '}';
    }
}