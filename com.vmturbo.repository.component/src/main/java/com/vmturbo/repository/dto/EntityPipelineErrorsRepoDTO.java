package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.EntityPipelineErrors;

/**
 * The repository-specific POJO equivalent to {@link EntityPipelineErrors}.
 */
@JsonInclude(Include.NON_EMPTY)
public class EntityPipelineErrorsRepoDTO {

    private Integer stitchingErrors;

    @Nonnull
    public static EntityPipelineErrorsRepoDTO fromProtobuf(@Nonnull final EntityPipelineErrors pipelineErrors) {
        EntityPipelineErrorsRepoDTO dto = new EntityPipelineErrorsRepoDTO();
        dto.stitchingErrors = pipelineErrors.getStitchingErrors();
        return dto;
    }

    public Integer getStitchingErrors() {
        return stitchingErrors;
    }

    public void setStitchingErrors(Integer stitchingErrors) {
        this.stitchingErrors = stitchingErrors;
    }

    @Nonnull
    public EntityPipelineErrors toProtobuf() {
        final EntityPipelineErrors.Builder bldr = EntityPipelineErrors.newBuilder();
        if (stitchingErrors != null) {
            bldr.setStitchingErrors(stitchingErrors);
        }
        return bldr.build();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (other == this) return true;
        if (other instanceof EntityPipelineErrorsRepoDTO) {
            return Objects.equals(((EntityPipelineErrorsRepoDTO) other).stitchingErrors, stitchingErrors);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(stitchingErrors);
    }

    @Override
    public String toString() {
        return "EntityPipelineErrorsRepoDTO{" +
            "stitchingErrors=" + stitchingErrors +
        '}';
    }
}
