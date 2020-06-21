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
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO.Type;
import com.vmturbo.api.dto.searchquery.RelatedEntityFieldApiDTO.RelatedEntitiesProperty;
import com.vmturbo.api.enums.CommodityType;
import com.vmturbo.api.enums.EntitySeverity;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * Class to house mapping information used for ingestion and reading the Search Entity DB.
 */
public enum SearchEntityMetadataMapping {

    /**
     * Basic fields.
     */
    PRIMITIVE_OID("oid", Type.INTEGER, null,
            entity -> Optional.of(entity.getOid())),

    PRIMITIVE_ENTITY_TYPE("type", Type.ENUM, EntityType.class,
            entity -> Optional.of(EntityDTO.EntityType.forNumber(entity.getEntityType()))),

    PRIMITIVE_NAME("name", Type.TEXT, null,
            entity -> Optional.of(entity.getDisplayName())),

    PRIMITIVE_STATE("state", Type.ENUM, EntityState.class,
            entity -> Optional.of(entity.getEntityState())),

    PRIMITIVE_ENVIRONMENT_TYPE("environment", Type.ENUM, EnvironmentType.class,
            entity -> Optional.of(entity.getEnvironmentType())),

    PRIMITIVE_SEVERITY("severity", Type.ENUM, EntitySeverity.class),

    /**
     * Related action.
     */
    RELATED_ACTION("num_actions", Type.INTEGER, null),

    /**
     * Entity type specific fields.
     */
    PRIMITIVE_GUEST_OS_TYPE("attrs", "guest_os_type", Type.ENUM, OSType.class,
            entity -> Optional.of(entity.getTypeSpecificInfo().getVirtualMachine().getGuestOsInfo().getGuestOsType())),

    /**
     * Commodities.
     */
    COMMODITY_VCPU_USED("attrs", "vcpu_used", CommodityType.VCPU, CommodityAttribute.USED,
            CommodityTypeUnits.VCPU, Type.NUMBER),

    COMMODITY_VCPU_UTILIZATION("attrs", "vcpu_utilization", CommodityType.VCPU, CommodityAttribute.UTILIZATION,
            CommodityTypeUnits.VCPU, Type.NUMBER),

    COMMODITY_CPU_USED("attrs", "cpu_used", CommodityType.CPU, CommodityAttribute.USED,
            CommodityTypeUnits.CPU, Type.NUMBER),

    COMMODITY_CPU_UTILIZATION("attrs", "cpu_utilization", CommodityType.CPU, CommodityAttribute.UTILIZATION,
            CommodityTypeUnits.CPU, Type.NUMBER),

    /**
     * Related entities.
     */
    RELATED_HOST("attrs", "related_host", Collections.singleton(EntityType.PHYSICAL_MACHINE),
            RelatedEntitiesProperty.NAMES, Type.MULTI_TEXT),

    RELATED_DATA_CENTER("attrs", "related_dc", Collections.singleton(EntityType.DATACENTER),
            RelatedEntitiesProperty.NAMES, Type.MULTI_TEXT),

    NUM_WORKLOADS("attrs", "num_workloads", ImmutableSet.of(EntityType.VIRTUAL_MACHINE,
            EntityType.APPLICATION, EntityType.DATABASE),
            RelatedEntitiesProperty.COUNT, Type.MULTI_TEXT),
    ;

    /** name of the column in db table */
    @Nonnull
    private final String columnName;

    // key of the json obj, if this is a jsonB column
    @Nullable
    private String jsonKeyName;

    // type of the value for this field
    @Nonnull private Type apiDatatype;

    // enum if
    @Nullable private Class<? extends Enum<?>> enumClass;

    @Nullable private Function<TopologyEntityDTO, Optional<Object>> topoFieldFunction;

    @Nullable private Set<EntityType> relatedEntityTypes;

    @Nullable private RelatedEntitiesProperty relatedEntityProperty;

    @Nullable private CommodityType commodityType;

    @Nullable private CommodityAttribute commodityAttribute;

    @Nullable private CommodityTypeUnits commodityUnit;


    /**
     * Constructor of {@link SearchEntityMetadataMapping} for normal table column fields which are
     * not available on {@link TopologyEntityDTO}, such as: severity, related action.
     *
     * @param columnName db column name
     * @param apiDatatype data type of the field
     * @param enumClass enum class if data type is enum
     */
    SearchEntityMetadataMapping(@Nonnull String columnName,
                                @Nonnull Type apiDatatype,
                                @Nullable Class<? extends Enum<?>> enumClass) {
        this.columnName = Objects.requireNonNull(columnName);
        this.apiDatatype = Objects.requireNonNull(apiDatatype);
        this.enumClass = enumClass;
    }

    /**
     * Constructor of {@link SearchEntityMetadataMapping} for normal table column fields which are
     * available on {@link TopologyEntityDTO}, like: oid, name, entityType, etc.
     *
     * @param columnName db column name
     * @param topoFieldFunction function of how to get value from TopologyEntityDTO
     * @param apiDatatype data structure descriptor of column data.
     * @param enumClass Enum Class for {@link Type#ENUM} data
     * @return SearchEntityMetaDataMapping
     */
    SearchEntityMetadataMapping(@Nonnull String columnName,
                                @Nonnull Type apiDatatype,
                                @Nullable Class<? extends Enum<?>> enumClass,
                                @Nonnull Function<TopologyEntityDTO, Optional<Object>> topoFieldFunction) {
        this.columnName = Objects.requireNonNull(columnName);
        this.apiDatatype = Objects.requireNonNull(apiDatatype);
        this.topoFieldFunction = Objects.requireNonNull(topoFieldFunction);
        this.enumClass = enumClass;
    }

    /**
     * Constructor of {@link SearchEntityMetadataMapping} for attributes in the jsonb table column
     * which are available on {@link TopologyEntityDTO}. This usually comes from the
     * {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo}, like: guest_os_type.
     *
     * @param columnName db column name
     * @param topoFieldFunction
     * @param apiDatatype data structure descriptor of column data.
     * @param enumClass Enum Class for {@link Type#ENUM} data
     * @return SearchEntityMetaDataMapping
     */
    SearchEntityMetadataMapping(@Nonnull String columnName,
                                @Nonnull String jsonKeyName,
                                @Nonnull Type apiDatatype,
                                @Nullable Class<? extends Enum<?>> enumClass,
                                @Nonnull Function<TopologyEntityDTO, Optional<Object>> topoFieldFunction) {
        this.columnName = Objects.requireNonNull(columnName);
        this.jsonKeyName = Objects.requireNonNull(jsonKeyName);
        this.apiDatatype = Objects.requireNonNull(apiDatatype);
        this.enumClass = enumClass;
        this.topoFieldFunction = Objects.requireNonNull(topoFieldFunction);
    }

    /**
     * Constructor of {@link SearchEntityMetadataMapping} for commodity data, which will be put
     * into the jsonb column.
     *
     * @param columnName db column name
     * @param jsonKeyName db json column key
     * @param commodityType commodityType of data
     * @param commodityAttribute subproperty of commodityType when configured
     * @param commodityUnit Units for relevant {@link CommodityType} data
     * @param apiDatatype data structure descriptor of column data
     * @return SearchEntityMetaDataMapping
     */
    SearchEntityMetadataMapping(@Nonnull String columnName,
                                @Nonnull String jsonKeyName,
                                @Nonnull CommodityType commodityType,
                                @Nonnull CommodityAttribute commodityAttribute,
                                @Nonnull CommodityTypeUnits commodityUnit,
                                @Nonnull Type apiDatatype) {
        this.columnName = Objects.requireNonNull(columnName);
        this.jsonKeyName = Objects.requireNonNull(jsonKeyName);
        this.commodityType = Objects.requireNonNull(commodityType);
        this.commodityAttribute = Objects.requireNonNull(commodityAttribute);
        this.commodityUnit = Objects.requireNonNull(commodityUnit);
        this.apiDatatype = Objects.requireNonNull(apiDatatype);
    }

    /**
     * Constructor of {@link SearchEntityMetadataMapping} for related entity data. This is put into
     * a jsonb table column, which contains key/value pairs.
     *
     * @param columnName db column name
     * @param jsonKeyName db json column key
     * @param relatedEntityTypes subproperty of commodityType when configured
     * @param relatedEntityProperty Units for relevant {@link CommodityType} data
     * @param apiDatatype api data structure descriptor of column data
     * @return SearchEntityMetaDataMapping
     */
    SearchEntityMetadataMapping(@Nonnull String columnName,
                                @Nonnull String jsonKeyName,
                                @Nonnull Set<EntityType> relatedEntityTypes,
                                @Nonnull RelatedEntitiesProperty relatedEntityProperty,
                                @Nonnull Type apiDatatype) {
        this.columnName = Objects.requireNonNull(columnName);
        this.jsonKeyName = Objects.requireNonNull(jsonKeyName);
        this.relatedEntityTypes = Objects.requireNonNull(relatedEntityTypes);
        this.relatedEntityProperty = Objects.requireNonNull(relatedEntityProperty);
        this.apiDatatype = Objects.requireNonNull(apiDatatype);
    }

    //TODO: constructors for RelatedGroupFieldMapping

    /**
     * Corresponds to DB column name.
     *
     * @return columnName
     */
    @Nonnull
    public String getColumnName() {
        return columnName;
    }

    @Nullable
    public String getJsonKeyName() {
        return jsonKeyName;
    }

    /**
     * Data structure descriptor of column data.
     *
     * @return Type
     */
    public Type getApiDatatype() {
        return apiDatatype;
    }

    public Function<TopologyEntityDTO, Optional<Object>> getTopoFieldFunction() {
        return topoFieldFunction;
    }

    /**
     * Enum Class for {@link Type#ENUM} data.
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
    @Nullable
    public CommodityAttribute getCommodityAttribute() {
        return commodityAttribute;
    }

    /**
     * Units for relevant {@link CommodityType} data.
     *
     * @return CommodityTypeUnits
     */
    @Nullable
    public CommodityTypeUnits getCommodityUnit() {
        return commodityUnit;
    }

    @Nullable
    public Set<EntityType> getRelatedEntityTypes() {
        return relatedEntityTypes;
    }

    @Nullable
    public RelatedEntitiesProperty getRelatedEntityProperty() {
        return relatedEntityProperty;
    }

    /**
     * Units for relevant {@link CommodityType} data.
     *
     * @return string representation of {@link SearchEntityMetadataMapping#commodityUnit},
     *          if not set returns null
     */
    public String getUnitsString() {
        return Objects.isNull(commodityUnit)? null :commodityUnit.toString();
    }
}
