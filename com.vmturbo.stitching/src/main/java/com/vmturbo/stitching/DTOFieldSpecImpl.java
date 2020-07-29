/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.stitching;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityField;

/**
 * {@link DTOFieldSpecImpl} wrapper around protobuf {@link EntityField} instance.
 */
public class DTOFieldSpecImpl implements DTOFieldSpec {
    private final EntityField field;

    /**
     * Creates {@link DTOFieldSpecImpl} instance.
     *
     * @param field that will be wrapped.
     */
    public DTOFieldSpecImpl(EntityField field) {
        this.field = field;
    }

    @Nonnull
    @Override
    public String getFieldName() {
        return field.getFieldName();
    }

    @Nonnull
    @Override
    public List<String> getMessagePath() {
        // path may be empty since the field may be in EntityDTO directly, no nested layers
        return field.getMessagePathList();
    }

    @Override
    public String toString() {
        return String.format("%s [fieldName=%s, messagePath=%s]", getClass().getSimpleName(),
                        getFieldName(), getMessagePath());
    }
}
