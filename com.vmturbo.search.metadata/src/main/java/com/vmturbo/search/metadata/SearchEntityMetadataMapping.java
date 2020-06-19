package com.vmturbo.search.metadata;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.dto.searchquery.CommodityFieldApiDTO.CommodityAttribute;
import com.vmturbo.api.dto.searchquery.RelatedEntityFieldApiDTO.RelatedEntitiesProperty;
import com.vmturbo.api.enums.CommodityType;
import com.vmturbo.api.enums.EntitySeverity;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * Class to house mapping information used for ingestion and reading the Search Entity DB.
 */
public enum SearchEntityMetadataMapping {

    /**
     * Basic fields.
     */
    PRIMITIVE_OID("oid", SearchEntityFieldDataType.INTEGER, null,
            entity -> Optional.of(entity.getOid())),

    PRIMITIVE_ENTITY_TYPE("type", SearchEntityFieldDataType.ENUM, EntityType.class,
            entity -> Optional.of(entity.getEntityType())),

    PRIMITIVE_NAME("name", SearchEntityFieldDataType.STRING, null,
            entity -> Optional.of(entity.getDisplayName())),

    PRIMITIVE_STATE("state", SearchEntityFieldDataType.ENUM, EntityState.class,
            entity -> Optional.of(entity.getEntityState())),

    PRIMITIVE_ENVIRONMENT_TYPE("environment", SearchEntityFieldDataType.ENUM, EnvironmentType.class,
            entity -> Optional.of(entity.getEnvironmentType())),

    PRIMITIVE_SEVERITY("severity", SearchEntityFieldDataType.ENUM, EntitySeverity.class),

    /**
     * Related action.
     */
    RELATED_ACTION("num_actions", SearchEntityFieldDataType.INTEGER, null),

    /**
     * Entity type specific fields.
     */
    PRIMITIVE_GUEST_OS_TYPE("attrs", "guest_os_type", SearchEntityFieldDataType.ENUM, OSType.class,
            entity -> Optional.of(entity.getTypeSpecificInfo().getVirtualMachine().getGuestOsInfo().getGuestOsType())),

    /**
     * Commodities.
     */
    COMMODITY_VCPU_USED("attrs", "vcpu_used", CommodityType.VCPU, CommodityAttribute.USED,
            CommodityTypeUnits.VCPU, SearchEntityFieldDataType.DECIMAL),

    COMMODITY_VCPU_UTILIZATION("attrs", "vcpu_utilization", CommodityType.VCPU, CommodityAttribute.UTILIZATION,
            CommodityTypeUnits.VCPU, SearchEntityFieldDataType.DECIMAL),

    COMMODITY_CPU_USED("attrs", "cpu_used", CommodityType.CPU, CommodityAttribute.USED,
            CommodityTypeUnits.CPU, SearchEntityFieldDataType.DECIMAL),

    COMMODITY_CPU_UTILIZATION("attrs", "cpu_utilization", CommodityType.CPU, CommodityAttribute.UTILIZATION,
            CommodityTypeUnits.CPU, SearchEntityFieldDataType.DECIMAL),

    /**
     * Related entities.
     */
    RELATED_HOST("attrs", "related_host", Collections.singleton(EntityType.PHYSICAL_MACHINE),
            RelatedEntitiesProperty.NAMES, SearchEntityFieldDataType.STRING),

    RELATED_DATA_CENTER("attrs", "related_dc", Collections.singleton(EntityType.DATACENTER),
            RelatedEntitiesProperty.NAMES, SearchEntityFieldDataType.STRING),

    NUM_WORKLOADS("attrs", "num_workloads", ImmutableSet.of(EntityType.VIRTUAL_MACHINE,
            EntityType.APPLICATION, EntityType.DATABASE),
            RelatedEntitiesProperty.COUNT, SearchEntityFieldDataType.INTEGER);

    private final String columnName;

    // key of the json obj, if this is a jsonB column
    private String jsonKeyName;

    // type of the value for this field
    private final SearchEntityFieldDataType searchEntityFieldDataType;

    private Class<? extends Enum<?>> enumClass;

    private Function<TopologyEntityDTO, Optional<Object>> topoFieldFunction;

    private Set<EntityType> relatedEntityTypes;

    private RelatedEntitiesProperty relatedEntityProperty;

    private CommodityType commodityType;

    private CommodityAttribute commodityAttribute;

    private CommodityTypeUnits commodityUnit;


    /**
     * Constructor of {@link SearchEntityMetadataMapping} for normal table column fields which are
     * not available on {@link TopologyEntityDTO}, such as: severity, related action.
     *
     * @param columnName db column name
     * @param searchEntityFieldDataType data type of the field
     * @param enumClass enum class if data type is enum
     */
    SearchEntityMetadataMapping(@Nonnull String columnName,
                                @Nonnull SearchEntityFieldDataType searchEntityFieldDataType,
                                @Nullable Class<? extends Enum<?>> enumClass) {
        this.columnName = Objects.requireNonNull(columnName);
        this.searchEntityFieldDataType = Objects.requireNonNull(searchEntityFieldDataType);
        this.enumClass = enumClass;
    }

    /**
     * Constructor of {@link SearchEntityMetadataMapping} for normal table column fields which are
     * available on {@link TopologyEntityDTO}, like: oid, name, entityType, etc.
     *
     * @param columnName db column name
     * @param topoFieldFunction function of how to get value from TopologyEntityDTO
     * @param searchEntityFieldDataType data structure descriptor of column data.
     * @param enumClass Enum Class for {@link SearchEntityFieldDataType#ENUM} data
     */
    SearchEntityMetadataMapping(@Nonnull String columnName,
                                @Nonnull SearchEntityFieldDataType searchEntityFieldDataType,
                                @Nullable Class<? extends Enum<?>> enumClass,
                                @Nonnull Function<TopologyEntityDTO, Optional<Object>> topoFieldFunction) {
        this.columnName = Objects.requireNonNull(columnName);
        this.searchEntityFieldDataType = Objects.requireNonNull(searchEntityFieldDataType);
        this.topoFieldFunction = Objects.requireNonNull(topoFieldFunction);
        this.enumClass = enumClass;
    }

    /**
     * Constructor of {@link SearchEntityMetadataMapping} for attributes in the jsonb table column
     * which are available on {@link TopologyEntityDTO}. This usually comes from the
     * {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo}, like: guest_os_type.
     *
     * @param columnName db column name
     * @param jsonKeyName key name inside the jsonb column
     * @param searchEntityFieldDataType data structure descriptor of column data.
     * @param enumClass Enum Class for {@link SearchEntityFieldDataType#ENUM} data
     * @param topoFieldFunction function of how to get value from TopologyEntityDTO
     */
    SearchEntityMetadataMapping(@Nonnull String columnName,
                                @Nonnull String jsonKeyName,
                                @Nonnull SearchEntityFieldDataType searchEntityFieldDataType,
                                @Nullable Class<? extends Enum<?>> enumClass,
                                @Nonnull Function<TopologyEntityDTO, Optional<Object>> topoFieldFunction) {
        this.columnName = Objects.requireNonNull(columnName);
        this.jsonKeyName = Objects.requireNonNull(jsonKeyName);
        this.searchEntityFieldDataType = Objects.requireNonNull(searchEntityFieldDataType);
        this.enumClass = enumClass;
        this.topoFieldFunction = Objects.requireNonNull(topoFieldFunction);
    }

    /**
     * Constructor of {@link SearchEntityMetadataMapping} for commodity data, which will be put
     * into the jsonb column.
     *
     * @param columnName db column name
     * @param jsonKeyName key name inside the jsonb column
     * @param commodityType commodityType of data
     * @param commodityAttribute subproperty of commodityType when configured
     * @param commodityUnit Units for relevant {@link CommodityType} data
     * @param searchEntityFieldDataType data structure descriptor of column data
     */
    SearchEntityMetadataMapping(@Nonnull String columnName,
                                @Nonnull String jsonKeyName,
                                @Nonnull CommodityType commodityType,
                                @Nonnull CommodityAttribute commodityAttribute,
                                @Nonnull CommodityTypeUnits commodityUnit,
                                @Nonnull SearchEntityFieldDataType searchEntityFieldDataType) {
        this.columnName = Objects.requireNonNull(columnName);
        this.jsonKeyName = Objects.requireNonNull(jsonKeyName);
        this.commodityType = Objects.requireNonNull(commodityType);
        this.commodityAttribute = Objects.requireNonNull(commodityAttribute);
        this.commodityUnit = Objects.requireNonNull(commodityUnit);
        this.searchEntityFieldDataType = Objects.requireNonNull(searchEntityFieldDataType);
    }

    /**
     * Constructor of {@link SearchEntityMetadataMapping} for related entity data. This is put into
     * a jsonb table column, which contains key/value pairs.
     *
     * @param columnName db column name
     * @param jsonKeyName key name inside the jsonb column
     * @param relatedEntityTypes set of related entity types
     * @param relatedEntityProperty property of related entity
     * @param searchEntityFieldDataType data type of the field
     */
    SearchEntityMetadataMapping(@Nonnull String columnName,
                                @Nonnull String jsonKeyName,
                                @Nonnull Set<EntityType> relatedEntityTypes,
                                @Nonnull RelatedEntitiesProperty relatedEntityProperty,
                                @Nonnull SearchEntityFieldDataType searchEntityFieldDataType) {
        this.columnName = Objects.requireNonNull(columnName);
        this.jsonKeyName = Objects.requireNonNull(jsonKeyName);
        this.relatedEntityTypes = Objects.requireNonNull(relatedEntityTypes);
        this.relatedEntityProperty = Objects.requireNonNull(relatedEntityProperty);
        this.searchEntityFieldDataType = Objects.requireNonNull(searchEntityFieldDataType);
    }

    //TODO: constructors for RelatedGroupFieldMapping

    /**
     * Corresponds to DB column name.
     *
     * @return columnName
     */
    public String getColumnName() {
        return columnName;
    }

    public String getJsonKeyName() {
        return jsonKeyName;
    }

    /**
     * Data structure descriptor of column data.
     *
     * @return SearchEntityFieldDataType
     */
    public SearchEntityFieldDataType getSearchEntityFieldDataType() {
        return searchEntityFieldDataType;
    }

    public Function<TopologyEntityDTO, Optional<Object>> getTopoFieldFunction() {
        return topoFieldFunction;
    }

    /**
     * Enum Class for {@link SearchEntityFieldDataType#ENUM} data.
     *
     * @return enum Class
     */
    public Class<? extends Enum<?>> getEnumClass() {
        return enumClass;
    }

    /**
     * {@link CommodityType} of data.
     *
     * @return CommodityType
     */
    public CommodityType getCommodityType() {
        return commodityType;
    }

    /**
     * Subproperty of CommodityType when configured/relevant.
     *
     * @return CommodityAttribute
     */
    public CommodityAttribute getCommodityAttribute() {
        return commodityAttribute;
    }

    /**
     * Units for relevant {@link CommodityType} data.
     *
     * @return CommodityTypeUnits
     */
    public CommodityTypeUnits getCommodityUnit() {
        return commodityUnit;
    }

    public Set<EntityType> getRelatedEntityTypes() {
        return relatedEntityTypes;
    }

    public RelatedEntitiesProperty getRelatedEntityProperty() {
        return relatedEntityProperty;
    }
}
