package com.vmturbo.repository.dto;

import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Enums;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolAssignmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolCloneType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolProvisionType;

/**
 * Class that encapsulates the desktop pool data from TopologyEntityDTO.TypeSpecificInfo.
 */
@JsonInclude(Include.NON_EMPTY)
public class DesktopPoolInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private String assignmentType;
    private String cloneType;
    private String provisionType;
    private Long vmReferenceId;
    private Long templateReferenceId;

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull TypeSpecificInfo typeSpecificInfo,
            @Nonnull ServiceEntityRepoDTO serviceEntityRepoDTO) {
        if (!typeSpecificInfo.hasDesktopPool()) {
            return;
        }
        final DesktopPoolInfo desktopPoolInfo = typeSpecificInfo.getDesktopPool();
        if (desktopPoolInfo.hasAssignmentType()) {
            setAssignmentType(desktopPoolInfo.getAssignmentType().name());
        }
        if (desktopPoolInfo.hasCloneType()) {
            setCloneType(desktopPoolInfo.getCloneType().name());
        }
        if (desktopPoolInfo.hasProvisionType()) {
            setProvisionType(desktopPoolInfo.getProvisionType().name());
        }
        setVmReferenceId(
                desktopPoolInfo.hasVmReferenceId() ? desktopPoolInfo.getVmReferenceId() : null);
        setTemplateReferenceId(desktopPoolInfo.hasTemplateReferenceId() ?
                desktopPoolInfo.getTemplateReferenceId() : null);
        serviceEntityRepoDTO.setDesktopPoolInfoRepoDTO(this);
    }

    @Nonnull
    @Override
    public TypeSpecificInfo createTypeSpecificInfo() {
        final DesktopPoolInfo.Builder builder = DesktopPoolInfo.newBuilder();
        setEnumValue(DesktopPoolAssignmentType.class, getAssignmentType(),
                builder::setAssignmentType);
        setEnumValue(DesktopPoolCloneType.class, getCloneType(), builder::setCloneType);
        setEnumValue(DesktopPoolProvisionType.class, getProvisionType(), builder::setProvisionType);
        if (getTemplateReferenceId() != null) {
            builder.setTemplateReferenceId(getTemplateReferenceId());
        }
        if (getVmReferenceId() != null) {
            builder.setVmReferenceId(getVmReferenceId());
        }
        return TypeSpecificInfo.newBuilder().setDesktopPool(builder).build();
    }

    public String getAssignmentType() {
        return assignmentType;
    }

    public void setAssignmentType(String assignmentType) {
        this.assignmentType = assignmentType;
    }

    public String getCloneType() {
        return cloneType;
    }

    public void setCloneType(String cloneType) {
        this.cloneType = cloneType;
    }

    public String getProvisionType() {
        return provisionType;
    }

    public void setProvisionType(String provisionType) {
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
                Objects.equals(templateReferenceId, that.templateReferenceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(assignmentType, cloneType, provisionType, vmReferenceId,
                templateReferenceId);
    }

    @Override
    public String toString() {
        return DesktopPoolInfoRepoDTO.class.getSimpleName() + '{' + "assignmentType='" +
                assignmentType + '\'' + ", cloneType='" + cloneType + '\'' + ", provisionType='" +
                provisionType + '\'' + ", vmReferenceId=" + vmReferenceId +
                ", templateReferenceId=" + templateReferenceId + '}';
    }

    private static <T extends Enum<T>> void setEnumValue(@Nonnull Class<T> enumClass,
            @Nullable String value, @Nonnull Consumer<? super T> consumer) {
        if (value != null) {
            Enums.getIfPresent(enumClass, value).toJavaUtil().ifPresent(consumer);
        }
    }
}
