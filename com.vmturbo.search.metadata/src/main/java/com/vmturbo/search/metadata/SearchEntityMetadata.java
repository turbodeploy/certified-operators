package com.vmturbo.search.metadata;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.searchquery.CommodityField;
import com.vmturbo.api.dto.searchquery.EntityField;
import com.vmturbo.api.dto.searchquery.PrimitiveEntityField;
import com.vmturbo.api.enums.CommodityType;
import com.vmturbo.api.enums.EntityType;

/**
 * Enumeration for search db column mappings.
 */
public enum SearchEntityMetadata {

    /**
     * Virtual Machine mappings.
     */
    VIRTUAL_MACHINE(EntityType.VIRTUAL_MACHINE, getVirtualMachineMetadata());

    private final EntityType entityType;

    private final Map<EntityField, SearchEntityMetadataMapping> metadataMappingMap;

    /**
     * Create a SearchEntityMetadata, containing column mappings for reading/writing to searchDB.
     *
     * @param entityType entityType which mappings belong to
     * @param metadataMappingMap mappings for the entityType
     */
    SearchEntityMetadata(@Nonnull EntityType entityType,
            Map<EntityField, SearchEntityMetadataMapping> metadataMappingMap) {
        this.entityType = entityType;
        this.metadataMappingMap = metadataMappingMap;
    }

    public Map<EntityField, SearchEntityMetadataMapping> getMetadataMappingMap() {
        return this.metadataMappingMap;
    }

    /**
     * Returns all relevant column mappings for Virtual Machine.
     *
     * @return Virtual Machine mappings
     */
    private static Map<EntityField, SearchEntityMetadataMapping> getVirtualMachineMetadata() {
        return new HashMap<EntityField, SearchEntityMetadataMapping>() {{
            //PRIMITIVES
            put(PrimitiveEntityField.oid(), SearchEntityMetadataMapping.PRIMITIVE_OID);
            put(PrimitiveEntityField.name(), SearchEntityMetadataMapping.PRIMITIVE_NAME);
            put(PrimitiveEntityField.entitySeverity(), SearchEntityMetadataMapping.PRIMITIVE_SEVERITY);
            put(PrimitiveEntityField.entityState(), SearchEntityMetadataMapping.PRIMITIVE_STATE);
            put(PrimitiveEntityField.entityState(), SearchEntityMetadataMapping.PRIMITIVE_ENVIRONMENT_TYPE);

            //COMMODITIES
            put(CommodityField.used(CommodityType.VCPU), SearchEntityMetadataMapping.COMMODITY_VCPU_USED);
        }};
    }
}
