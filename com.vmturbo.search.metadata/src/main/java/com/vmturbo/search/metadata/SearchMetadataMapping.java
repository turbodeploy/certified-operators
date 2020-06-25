package com.vmturbo.search.metadata;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.dto.searchquery.AggregateCommodityFieldApiDTO.Aggregation;
import com.vmturbo.api.dto.searchquery.CommodityFieldApiDTO.CommodityAttribute;
import com.vmturbo.api.dto.searchquery.FieldValueApiDTO.Type;
import com.vmturbo.api.dto.searchquery.MemberFieldApiDTO;
import com.vmturbo.api.dto.searchquery.MemberFieldApiDTO.Property;
import com.vmturbo.api.dto.searchquery.RelatedEntityFieldApiDTO.RelatedEntitiesProperty;
import com.vmturbo.api.dto.searchquery.RelatedGroupFieldApiDTO.RelatedGroupFieldName;
import com.vmturbo.api.enums.CommodityType;
import com.vmturbo.api.enums.EntitySeverity;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.EntityType;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.GroupType;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.SelectionCriteriaCase;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.search.metadata.utils.MetadataMappingUtils;

/**
 * Class to house mapping information used for ingestion and reading the Search Entity DB, used for
 * both entity and group.
 */
public enum SearchMetadataMapping {

    /**
     * Basic fields for entity.
     */
    PRIMITIVE_OID("oid", Type.INTEGER, null,
            entity -> Optional.of(entity.getOid())),

    PRIMITIVE_ENTITY_TYPE("type", Type.ENUM, EntityType.class,
            entity -> Optional.of(entity.getEntityType())),

    PRIMITIVE_NAME("name", Type.TEXT, null,
            entity -> Optional.of(entity.getDisplayName())),

    PRIMITIVE_STATE("state", Type.ENUM, EntityState.class,
            entity -> Optional.of(entity.getEntityState())),

    PRIMITIVE_ENVIRONMENT_TYPE("environment", Type.ENUM, EnvironmentType.class,
            entity -> Optional.of(entity.getEnvironmentType())),

    /**
     * Severity. Used for both entity and group.
     */
    PRIMITIVE_SEVERITY("severity", Type.ENUM, EntitySeverity.class),

    /**
     * Related action count. Used for both entity and group.
     */
    RELATED_ACTION_COUNT("num_actions", Type.INTEGER, null),

    /**
     * Related group field, used for entity. Member type of the related group must be provided,
     * to help finding the related group for an entity. For example, for a VM, it will find related
     * host, then find cluster which contains the host.
     */
    RELATED_COMPUTE_HOST_CLUSTER_NAME("attrs", "related_cluster", GroupType.COMPUTE_HOST_CLUSTER,
            EntityType.PHYSICAL_MACHINE, RelatedGroupFieldName.NAMES, Type.MULTI_TEXT),

    RELATED_STORAGE_CLUSTER_NAME("attrs", "related_storage_cluster", GroupType.STORAGE_CLUSTER,
            EntityType.STORAGE, RelatedGroupFieldName.NAMES, Type.MULTI_TEXT),

    /**
     * Entity type specific fields.
     */
    PRIMITIVE_ATTACHMENT_STATE("attrs", "attachment_state", Type.ENUM, AttachmentState.class,
            entity -> Optional.of(entity.getTypeSpecificInfo().getVirtualVolume().getAttachmentState())),

    PRIMITIVE_CONNECTED_NETWORKS("attrs", "connected_networks", Type.MULTI_TEXT, null,
            entity -> Optional.of(entity.getTypeSpecificInfo().getVirtualMachine().getConnectedNetworksList())),

    PRIMITIVE_CPU_MODEL("attrs", "cpu_model", Type.TEXT, null,
            entity -> Optional.of(entity.getTypeSpecificInfo().getPhysicalMachine().getCpuModel())),

    PRIMITIVE_GUEST_OS_TYPE("attrs", "guest_os_type", Type.ENUM, OSType.class,
            entity -> Optional.of(entity.getTypeSpecificInfo().getVirtualMachine().getGuestOsInfo().getGuestOsType())),

    PRIMITIVE_MODEL("attrs", "model", Type.TEXT, null,
            entity -> Optional.of(entity.getTypeSpecificInfo().getPhysicalMachine().getModel())),

    PRIMITIVE_PM_NUM_CPUS("attrs", "num_cpus", Type.NUMBER, null,
            entity -> Optional.of(entity.getTypeSpecificInfo().getPhysicalMachine().getNumCpus())),

    PRIMITIVE_TIMEZONE("attrs", "timezone", Type.TEXT, null,
            entity -> Optional.of(entity.getTypeSpecificInfo().getPhysicalMachine().getTimezone())),

    PRIMITIVE_VM_NUM_CPUS("attrs", "num_cpus", Type.NUMBER, null,
            entity -> Optional.of(entity.getTypeSpecificInfo().getVirtualMachine().getNumCpus())),

    /**
     * Commodities for entity.
     */
    COMMODITY_BALLOONING_PERCENTILE("attrs", "ballooning_percentile", CommodityType.BALLOONING, CommodityAttribute.PERCENTILE,
            CommodityTypeUnits.BALLOONING, Type.NUMBER),

    COMMODITY_BALLOONING_UTILIZATION("attrs", "ballooning_utilization", CommodityType.BALLOONING, CommodityAttribute.UTILIZATION,
            CommodityTypeUnits.BALLOONING, Type.NUMBER),

    COMMODITY_VCPU_PERCENTILE_UTILIZATION("attrs", "vcpu_percentile_utilization", CommodityType.VCPU,
            CommodityAttribute.PERCENTILE, CommodityTypeUnits.VCPU, Type.NUMBER),

    COMMODITY_CPU_USED("attrs", "cpu_used", CommodityType.CPU, CommodityAttribute.USED,
            CommodityTypeUnits.CPU, Type.NUMBER),

    COMMODITY_CPU_UTILIZATION("attrs", "cpu_utilization", CommodityType.CPU, CommodityAttribute.UTILIZATION,
            CommodityTypeUnits.CPU, Type.NUMBER),

    COMMODITY_IO_THROUGHPUT_USED("attrs", "io_throughput_used", CommodityType.IO_THROUGHPUT, CommodityAttribute.USED,
            CommodityTypeUnits.IO_THROUGHPUT, Type.NUMBER),

    COMMODITY_IO_THROUGHPUT_UTILIZATION("attrs", "io_throughput_utilization", CommodityType.IO_THROUGHPUT, CommodityAttribute.UTILIZATION,
            CommodityTypeUnits.NET_THROUGHPUT, Type.NUMBER),

    COMMODITY_NET_THROUGHPUT_USED("attrs", "net_throughput_used", CommodityType.NET_THROUGHPUT, CommodityAttribute.USED,
            CommodityTypeUnits.IO_THROUGHPUT, Type.NUMBER),

    COMMODITY_NET_THROUGHPUT_UTILIZATION("attrs", "net_throughput_utilization", CommodityType.NET_THROUGHPUT, CommodityAttribute.UTILIZATION,
            CommodityTypeUnits.NET_THROUGHPUT, Type.NUMBER),

    COMMODITY_MEM_CAPACITY("attrs", "mem_capacity", CommodityType.MEM, CommodityAttribute.CAPACITY,
            CommodityTypeUnits.MEM, Type.NUMBER),

    COMMODITY_MEM_USED("attrs", "mem_used", CommodityType.MEM, CommodityAttribute.USED,
            CommodityTypeUnits.MEM, Type.NUMBER),

    COMMODITY_MEM_UTILIZATION("attrs", "mem_utilization", CommodityType.MEM, CommodityAttribute.UTILIZATION,
            CommodityTypeUnits.MEM, Type.NUMBER),

    COMMODITY_STORAGE_AMOUNT_CAPACITY("attrs", "storage_amount_capacity", CommodityType.STORAGE_AMOUNT, CommodityAttribute.CAPACITY,
            CommodityTypeUnits.STORAGE_AMOUNT, Type.NUMBER),

    COMMODITY_SWAPPING_PERCENTILE("attrs", "swapping_percentile", CommodityType.SWAPPING, CommodityAttribute.PERCENTILE,
            CommodityTypeUnits.SWAPPING, Type.NUMBER),

    COMMODITY_SWAPPING_UTILIZATION("attrs", "swapping_utilization", CommodityType.SWAPPING, CommodityAttribute.UTILIZATION,
            CommodityTypeUnits.SWAPPING, Type.NUMBER),

    COMMODITY_VCPU_USED("attrs", "vcpu_used", CommodityType.VCPU, CommodityAttribute.USED,
            CommodityTypeUnits.VCPU, Type.NUMBER),

    COMMODITY_VCPU_UTILIZATION("attrs", "vcpu_utilization", CommodityType.VCPU, CommodityAttribute.UTILIZATION,
            CommodityTypeUnits.VCPU, Type.NUMBER),

    COMMODITY_VMEM_CAPACITY("attrs", "vmem_capacity", CommodityType.VMEM, CommodityAttribute.CAPACITY,
            CommodityTypeUnits.VMEM, Type.NUMBER),

    COMMODITY_VMEM_USED("attrs", "vmem_used", CommodityType.VMEM, CommodityAttribute.USED,
            CommodityTypeUnits.VMEM, Type.NUMBER),

    COMMODITY_VMEM_UTILIZATION("attrs", "vmem_utilization", CommodityType.VMEM, CommodityAttribute.UTILIZATION,
            CommodityTypeUnits.VMEM, Type.NUMBER),

    COMMODITY_VSTORAGE_USED("attrs", "vstorage_used", CommodityType.VSTORAGE, CommodityAttribute.USED,
            CommodityTypeUnits.VSTORAGE, Type.NUMBER),

    COMMODITY_VSTORAGE_UTILIZATION("attrs", "vstorage_utilization", CommodityType.VSTORAGE, CommodityAttribute.UTILIZATION,
            CommodityTypeUnits.VSTORAGE, Type.NUMBER),

    /**
     * Related entities.
     */
    RELATED_ACCOUNT("attrs", "related_account", Collections.singleton(EntityType.BUSINESS_ACCOUNT),
            RelatedEntitiesProperty.NAMES, Type.MULTI_TEXT),

    RELATED_APPLICATION("attrs", "related_application", Collections.singleton(EntityType.APPLICATION),
            RelatedEntitiesProperty.NAMES, Type.MULTI_TEXT),

    RELATED_DISKARRAY("attrs", "related_diskarray", Collections.singleton(EntityType.DISKARRAY),
            RelatedEntitiesProperty.NAMES, Type.MULTI_TEXT),

    RELATED_HOST("attrs", "related_host", Collections.singleton(EntityType.PHYSICAL_MACHINE),
            RelatedEntitiesProperty.NAMES, Type.MULTI_TEXT),

    RELATED_DATA_CENTER("attrs", "related_dc", Collections.singleton(EntityType.DATACENTER),
            RelatedEntitiesProperty.NAMES, Type.MULTI_TEXT),

    RELATED_REGION("attrs", "related_region", Collections.singleton(EntityType.REGION),
            RelatedEntitiesProperty.NAMES, Type.MULTI_TEXT),

    RELATED_STORAGE("attrs", "related_storage", Collections.singleton(EntityType.STORAGE),
            RelatedEntitiesProperty.NAMES, Type.MULTI_TEXT),

    RELATED_STORAGE_TIER("attrs", "related_storage_tier", Collections.singleton(EntityType.STORAGE_TIER),
            RelatedEntitiesProperty.NAMES, Type.MULTI_TEXT),

    RELATED_SWITCH("attrs", "related_switch", Collections.singleton(EntityType.SWITCH),
            RelatedEntitiesProperty.NAMES, Type.MULTI_TEXT),

    RELATED_VM("attrs", "related_vm", Collections.singleton(EntityType.VIRTUAL_MACHINE),
            RelatedEntitiesProperty.NAMES, Type.MULTI_TEXT),

    NUM_VMS("attrs", "num_vms", Collections.singleton(EntityType.VIRTUAL_MACHINE),
            RelatedEntitiesProperty.COUNT, Type.NUMBER),

    NUM_WORKLOADS("attrs", "num_workloads",
        ImmutableSet.of(EntityType.VIRTUAL_MACHINE, EntityType.APPLICATION, EntityType.DATABASE),
        RelatedEntitiesProperty.COUNT, Type.INTEGER),

    /**
     * Basic fields for group.
     */
    PRIMITIVE_GROUP_OID("oid", group -> Optional.of(group.getId()), Type.INTEGER, null),

    PRIMITIVE_GROUP_TYPE("type", group -> Optional.of(group.getDefinition().getType()),
            Type.ENUM, EntityType.class),

    PRIMITIVE_GROUP_NAME("name", group -> Optional.of(group.getDefinition().getDisplayName()),
            Type.TEXT, null),

    PRIMITIVE_GROUP_DYNAMIC("attrs", "dynamic", group -> Optional.of(
            group.getDefinition().getSelectionCriteriaCase() != SelectionCriteriaCase.STATIC_GROUP_MEMBERS),
            Type.BOOLEAN, null),

    PRIMITIVE_GROUP_ORIGIN("attrs", "origin", MetadataMappingUtils::getOrigin, Type.TEXT, null),

    PRIMITIVE_GROUP_MEMBER_TYPES("attrs", "member_types",
            group -> Optional.of(MetadataMappingUtils.getDirectMemberTypes(group.getDefinition())),
            Type.MULTI_TEXT, null),

    PRIMITIVE_GROUP_INDIRECT_MEMBER_TYPES("attrs", "indirect_member_types",
            group -> Optional.of(MetadataMappingUtils.getIndirectMemberTypes(group)),
            Type.MULTI_TEXT, null),

    /**
     * Member count in this group, used for regular groups.
     * For static group, it can be found on {@link Grouping} directly,
     * but for dynamic group it can only be fetched from group component. Thus this can not be
     * handled by a function on Grouping directly.
     */
    DIRECT_MEMBER_COUNT("attrs", "member_count", null, Property.COUNT, true, Type.INTEGER),

    // member hosts count, different from related entities count below (only used by cluster for now)
    DIRECT_MEMBER_COUNT_PM("attrs", "host_count", EntityType.PHYSICAL_MACHINE, Property.COUNT, true,
            Type.INTEGER),
    // related vms count (only used by cluster for now)
    RELATED_MEMBER_COUNT_VM("attrs", "vm_count", ImmutableSet.of(EntityType.VIRTUAL_MACHINE),
            Property.COUNT, Type.INTEGER),
    // related storages count (only used by cluster for now)
    RELATED_MEMBER_COUNT_ST("attrs", "st_count", ImmutableSet.of(EntityType.STORAGE),
            Property.COUNT, Type.INTEGER),

    /**
     * Commodities for group. For now, this is only used by cluster, and is only for leaf entities
     * in the group (not related entities, like vms related to a cluster).
     */
    GROUP_COMMODITY_CPU_UTILIZATION_TOTAL("attrs", "cpu_utilization", EntityType.PHYSICAL_MACHINE,
            CommodityType.CPU, CommodityAttribute.UTILIZATION, Aggregation.TOTAL,
            CommodityTypeUnits.CPU, Type.NUMBER),

    GROUP_COMMODITY_MEM_UTILIZATION_TOTAL("attrs", "mem_utilization", EntityType.PHYSICAL_MACHINE,
            CommodityType.MEM, CommodityAttribute.UTILIZATION, Aggregation.TOTAL,
            CommodityTypeUnits.MEM, Type.NUMBER);


    // name of the column in db table
    private final String columnName;
    // key of the json obj, if this is a jsonB column
    private String jsonKeyName;
    // type of the value for this field
    private final Type apiDatatype;
    // enum class if Type is enum
    private Class<? extends Enum<?>> enumClass;
    // entity fields
    private Function<TopologyEntityDTO, Optional<Object>> topoFieldFunction;
    // related entity
    private Set<EntityType> relatedEntityTypes;
    private RelatedEntitiesProperty relatedEntityProperty;
    // related group
    private GroupType relatedGroupType;
    private RelatedGroupFieldName relatedGroupProperty;
    // commodity
    private CommodityType commodityType;
    private CommodityAttribute commodityAttribute;
    private CommodityTypeUnits commodityUnit;
    // group fields
    private Function<Grouping, Optional<Object>> groupFieldFunction;
    private Aggregation commodityAggregation;
    // used for group members
    private EntityType memberType;
    private boolean direct;
    private MemberFieldApiDTO.Property memberProperty;

    /**
     * Constructor of {@link SearchMetadataMapping} for normal table column fields which are
     * not available on {@link TopologyEntityDTO}, such as: severity, related action.
     *
     * @param columnName db column name
     * @param apiDatatype data type of the field
     * @param enumClass enum class if data type is enum
     */
    SearchMetadataMapping(@Nonnull String columnName,
                          @Nonnull Type apiDatatype,
                          @Nullable Class<? extends Enum<?>> enumClass) {
        this.columnName = Objects.requireNonNull(columnName);
        this.apiDatatype = Objects.requireNonNull(apiDatatype);
        this.enumClass = enumClass;
    }

    /**
     * Constructor of {@link SearchMetadataMapping} for related group field.
     *
     * @param columnName db column name
     * @param jsonKeyName key name inside the jsonb column
     * @param relatedGroupType type of related group
     * @param relatedEntityTypeInGroup related entity type in group
     * @param relatedGroupProperty property of related group
     * @param apiDatatype data type of the field
     */
    SearchMetadataMapping(@Nonnull String columnName,
                          @Nonnull String jsonKeyName,
                          @Nonnull GroupType relatedGroupType,
                          @Nonnull EntityType relatedEntityTypeInGroup,
                          @Nonnull RelatedGroupFieldName relatedGroupProperty,
                          @Nonnull Type apiDatatype) {
        this.columnName = Objects.requireNonNull(columnName);
        this.jsonKeyName = Objects.requireNonNull(jsonKeyName);
        this.relatedGroupType = Objects.requireNonNull(relatedGroupType);
        this.memberType = Objects.requireNonNull(relatedEntityTypeInGroup);
        this.relatedGroupProperty = Objects.requireNonNull(relatedGroupProperty);
        this.apiDatatype = Objects.requireNonNull(apiDatatype);
    }

    /**
     * Constructor of {@link SearchMetadataMapping} for normal table column fields which are
     * available on {@link TopologyEntityDTO}, like: oid, name, entityType, etc.
     *
     * @param columnName db column name
     * @param apiDatatype data structure descriptor of column data.
     * @param enumClass Enum Class for {@link Type#ENUM} data
     * @param topoFieldFunction function of how to get value from TopologyEntityDTO
     */
    SearchMetadataMapping(@Nonnull String columnName,
                          @Nonnull Type apiDatatype,
                          @Nullable Class<? extends Enum<?>> enumClass,
                          @Nonnull Function<TopologyEntityDTO, Optional<Object>> topoFieldFunction) {
        this.columnName = Objects.requireNonNull(columnName);
        this.apiDatatype = Objects.requireNonNull(apiDatatype);
        this.topoFieldFunction = Objects.requireNonNull(topoFieldFunction);
        this.enumClass = enumClass;
    }

    /**
     * Constructor of {@link SearchMetadataMapping} for attributes in the jsonb table column
     * which are available on {@link TopologyEntityDTO}. This usually comes from the
     * {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo}, like: guest_os_type.
     *
     * @param columnName db column name
     * @param jsonKeyName key inside db json column
     * @param apiDatatype data structure descriptor of column data.
     * @param enumClass Enum Class for {@link Type#ENUM} data
     * @param topoFieldFunction function of how to get value from TopologyEntityDTO
     */
    SearchMetadataMapping(@Nonnull String columnName,
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
     * Constructor of {@link SearchMetadataMapping} for commodity data, which will be put
     * into the jsonb column.
     *
     * @param columnName db column name
     * @param jsonKeyName db json column key
     * @param commodityType commodityType of data
     * @param commodityAttribute subproperty of commodityType when configured
     * @param commodityUnit Units for relevant {@link CommodityType} data
     * @param apiDatatype data structure descriptor of column data
     */
    SearchMetadataMapping(@Nonnull String columnName,
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
     * Constructor of {@link SearchMetadataMapping} for attributes in the jsonb table column
     * which are available on {@link Grouping}.
     *
     * @param columnName db column name
     * @param jsonKeyName key name inside the jsonb column
     * @param apiDatatype data structure descriptor of column data.
     * @param enumClass Enum Class for {@link Type#ENUM} data
     * @param groupFieldFunction function of how to get value from {@link Grouping}
     */
    SearchMetadataMapping(@Nonnull String columnName,
                          @Nonnull String jsonKeyName,
                          @Nonnull Function<Grouping, Optional<Object>> groupFieldFunction,
                          @Nonnull Type apiDatatype,
                          @Nullable Class<? extends Enum<?>> enumClass) {
        this.columnName = Objects.requireNonNull(columnName);
        this.jsonKeyName = Objects.requireNonNull(jsonKeyName);
        this.groupFieldFunction = Objects.requireNonNull(groupFieldFunction);
        this.apiDatatype = Objects.requireNonNull(apiDatatype);
        this.enumClass = enumClass;
    }

    /**
     * Constructor of {@link SearchMetadataMapping} for normal table column fields which are
     * available on {@link Grouping}, like: oid, name, entityType, etc.
     *
     * @param columnName db column name
     * @param apiDatatype data structure descriptor of column data.
     * @param groupFieldFunction function of how to get value from {@link Grouping}
     * @param enumClass enum class
     */
    SearchMetadataMapping(@Nonnull String columnName,
                          @Nonnull Function<Grouping, Optional<Object>> groupFieldFunction,
                          @Nonnull Type apiDatatype,
                          @Nullable Class<? extends Enum<?>> enumClass) {
        this.columnName = Objects.requireNonNull(columnName);
        this.groupFieldFunction = Objects.requireNonNull(groupFieldFunction);
        this.apiDatatype = Objects.requireNonNull(apiDatatype);
        this.enumClass = enumClass;
    }

    /**
     * Constructor of {@link SearchMetadataMapping} for group member field.
     *
     * @param columnName db column name
     * @param jsonKeyName key name inside the jsonb column
     * @param memberType member type of the group
     * @param memberProperty property of the member
     * @param direct direct member or indirect
     * @param apiDatatype data structure descriptor of column data
     */
    SearchMetadataMapping(@Nonnull String columnName,
                          @Nonnull String jsonKeyName,
                          @Nullable EntityType memberType,
                          @Nonnull Property memberProperty,
                          boolean direct,
                          @Nonnull Type apiDatatype) {
        this.columnName = Objects.requireNonNull(columnName);
        this.jsonKeyName = Objects.requireNonNull(jsonKeyName);
        this.memberType = memberType;
        this.memberProperty = memberProperty;
        this.direct = direct;
        this.apiDatatype = Objects.requireNonNull(apiDatatype);
    }

    /**
     * Constructor of {@link SearchMetadataMapping} for group related member field.
     *
     * @param columnName db column name
     * @param jsonKeyName key name inside the jsonb column
     * @param relatedEntityTypes types of related entities
     * @param memberProperty property of the member
     * @param apiDatatype data structure descriptor of column data
     */
    SearchMetadataMapping(@Nonnull String columnName,
                          @Nonnull String jsonKeyName,
                          @Nullable Set<EntityType> relatedEntityTypes,
                          @Nonnull Property memberProperty,
                          @Nonnull Type apiDatatype) {
        this.columnName = Objects.requireNonNull(columnName);
        this.jsonKeyName = Objects.requireNonNull(jsonKeyName);
        this.relatedEntityTypes = Objects.requireNonNull(relatedEntityTypes);
        this.memberProperty = memberProperty;
        this.apiDatatype = Objects.requireNonNull(apiDatatype);
    }

    /**
     * Constructor of {@link SearchMetadataMapping} for group commodity data, which will be
     * put into the jsonb column.
     *
     * @param columnName db column name
     * @param jsonKeyName key name inside the jsonb column
     * @param memberType type of the member of the group
     * @param commodityType commodityType of data
     * @param commodityAttribute subproperty of commodityType when configured
     * @param commodityAggregation aggregation of commodity
     * @param commodityUnit Units for relevant {@link CommodityType} data
     * @param apiDatatype data structure descriptor of column data
     */
    SearchMetadataMapping(@Nonnull String columnName,
                          @Nonnull String jsonKeyName,
                          @Nonnull EntityType memberType,
                          @Nonnull CommodityType commodityType,
                          @Nonnull CommodityAttribute commodityAttribute,
                          @Nonnull Aggregation commodityAggregation,
                          @Nonnull CommodityTypeUnits commodityUnit,
                          @Nonnull Type apiDatatype) {
        this.columnName = Objects.requireNonNull(columnName);
        this.jsonKeyName = Objects.requireNonNull(jsonKeyName);
        this.memberType = Objects.requireNonNull(memberType);
        this.commodityType = Objects.requireNonNull(commodityType);
        this.commodityAttribute = Objects.requireNonNull(commodityAttribute);
        this.commodityAggregation = Objects.requireNonNull(commodityAggregation);
        this.commodityUnit = Objects.requireNonNull(commodityUnit);
        this.apiDatatype = Objects.requireNonNull(apiDatatype);
    }

    /**
     * Constructor of {@link SearchMetadataMapping} for related entity data. This is put into
     * a jsonb table column, which contains key/value pairs.
     *
     * @param columnName db column name
     * @param jsonKeyName db json column key
     * @param relatedEntityTypes types of related entities
     * @param relatedEntityProperty related entity property
     * @param apiDatatype api data structure descriptor of column data
     */
    SearchMetadataMapping(@Nonnull String columnName,
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

    public Function<Grouping, Optional<Object>> getGroupFieldFunction() {
        return groupFieldFunction;
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
     * Type of the aggregation for commodities in a group.
     *
     * @return {@link Aggregation}
     */
    public Aggregation getCommodityAggregation() {
        return commodityAggregation;
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
     * @return string representation of {@link SearchMetadataMapping#commodityUnit},
     *          if not set returns null
     */
    public String getUnitsString() {
        return Objects.isNull(commodityUnit) ? null : commodityUnit.toString();
    }

    public GroupType getRelatedGroupType() {
        return relatedGroupType;
    }

    public RelatedGroupFieldName getRelatedGroupProperty() {
        return relatedGroupProperty;
    }

    public EntityType getMemberType() {
        return memberType;
    }

    public Property getMemberProperty() {
        return memberProperty;
    }

    public boolean isDirect() {
        return direct;
    }
}
