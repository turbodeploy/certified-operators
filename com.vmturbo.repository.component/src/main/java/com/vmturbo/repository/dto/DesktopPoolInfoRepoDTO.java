package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo.VmWithSnapshot;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo.VmWithSnapshot.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolAssignmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolCloneType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolProvisionType;

/**
 * Class that encapsulates the desktop pool data from TopologyEntityDTO.TypeSpecificInfo.
 */
@JsonInclude(Include.NON_EMPTY)
public class DesktopPoolInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private DesktopPoolAssignmentType assignmentType;
    private DesktopPoolCloneType cloneType;
    private DesktopPoolProvisionType provisionType;
    private Long vmReferenceId;
    private Long templateReferenceId;
    private String snapshot;

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull TypeSpecificInfo typeSpecificInfo,
            @Nonnull ServiceEntityRepoDTO serviceEntityRepoDTO) {
        if (!typeSpecificInfo.hasDesktopPool()) {
            return;
        }
        final DesktopPoolInfo desktopPoolInfo = typeSpecificInfo.getDesktopPool();
        if (desktopPoolInfo.hasAssignmentType()) {
            setAssignmentType(desktopPoolInfo.getAssignmentType());
        }
        if (desktopPoolInfo.hasCloneType()) {
            setCloneType(desktopPoolInfo.getCloneType());
        }
        if (desktopPoolInfo.hasProvisionType()) {
            setProvisionType(desktopPoolInfo.getProvisionType());
        }
        if (desktopPoolInfo.hasVmWithSnapshot()) {
            final VmWithSnapshot vmWithSnapshot = desktopPoolInfo.getVmWithSnapshot();
            setVmReferenceId(vmWithSnapshot.getVmReferenceId());
            if (vmWithSnapshot.hasSnapshot()) {
                setSnapshot(vmWithSnapshot.getSnapshot());
            }
        }
        setTemplateReferenceId(desktopPoolInfo.hasTemplateReferenceId() ?
                desktopPoolInfo.getTemplateReferenceId() : null);
        serviceEntityRepoDTO.setDesktopPoolInfoRepoDTO(this);
    }

    @Nonnull
    @Override
    public TypeSpecificInfo createTypeSpecificInfo() {
        final DesktopPoolInfo.Builder builder = DesktopPoolInfo.newBuilder();
        if (getAssignmentType() != null) {
            builder.setAssignmentType(getAssignmentType());
        }
        if (getProvisionType() != null) {
            builder.setProvisionType(getProvisionType());
        }
        if (getCloneType() != null) {
            builder.setCloneType(getCloneType());
        }
        if (getTemplateReferenceId() != null) {
            builder.setTemplateReferenceId(getTemplateReferenceId());
        }
        if (getVmReferenceId() != null) {
            final Builder vmWithSnapshotBuilder = VmWithSnapshot.newBuilder();
            vmWithSnapshotBuilder.setVmReferenceId(getVmReferenceId());
            if (getSnapshot() != null) {
                vmWithSnapshotBuilder.setSnapshot(getSnapshot());
            }
            builder.setVmWithSnapshot(vmWithSnapshotBuilder.build());
        }
        return TypeSpecificInfo.newBuilder().setDesktopPool(builder).build();
    }

    public DesktopPoolAssignmentType getAssignmentType() {
        return assignmentType;
    }

    public void setAssignmentType(DesktopPoolAssignmentType assignmentType) {
        this.assignmentType = assignmentType;
    }

    public DesktopPoolCloneType getCloneType() {
        return cloneType;
    }

    public void setCloneType(DesktopPoolCloneType cloneType) {
        this.cloneType = cloneType;
    }

    public DesktopPoolProvisionType getProvisionType() {
        return provisionType;
    }

    public void setProvisionType(DesktopPoolProvisionType provisionType) {
        this.provisionType = provisionType;
    }

    public Long getVmReferenceId() {
        return vmReferenceId;
    }

    public void setVmReferenceId(Long vmReferenceId) {
        this.vmReferenceId = vmReferenceId;
    }

    public Long getTemplateReferenceId() {
        return templateReferenceId;
    }

    public void setTemplateReferenceId(Long templateReferenceId) {
        this.templateReferenceId = templateReferenceId;
    }

    public String getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(String snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DesktopPoolInfoRepoDTO that = (DesktopPoolInfoRepoDTO)o;
        return Objects.equals(assignmentType, that.assignmentType) &&
                Objects.equals(cloneType, that.cloneType) &&
                Objects.equals(provisionType, that.provisionType) &&
                Objects.equals(vmReferenceId, that.vmReferenceId) &&
                Objects.equals(templateReferenceId, that.templateReferenceId) &&
                Objects.equals(snapshot, that.snapshot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(assignmentType, cloneType, provisionType, vmReferenceId,
                templateReferenceId, snapshot);
    }

    @Override
    public String toString() {
        return  DesktopPoolInfoRepoDTO.class.getSimpleName() + '{' + "assignmentType='" + assignmentType + '\'' +
                ", cloneType='" + cloneType + '\'' + ", provisionType='" + provisionType + '\'' +
                ", vmReferenceId=" + vmReferenceId + ", templateReferenceId=" +
                templateReferenceId + ", snapshot='" + snapshot + '\'' + '}';
    }
}
