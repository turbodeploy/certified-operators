package com.vmturbo.search.metadata;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.searchquery.CommodityField.CommodityAttribute;
import com.vmturbo.api.enums.CommodityType;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;

/**
 * Class to house mapping information used for ingestion and reading the Search Entity DB.
 */
public enum SearchEntityMetadataMapping {

    PRIMITIVE_OID("oid", SearchEntityFieldDataType.INTEGER, "oid", null),

    PRIMITIVE_NAME("name", SearchEntityFieldDataType.STRING, "displayName", null),

    PRIMITIVE_STATE("state", SearchEntityFieldDataType.ENUM, "entityState", EntityState.class),

    PRIMITIVE_SEVERITY("severity", SearchEntityFieldDataType.ENUM, "severity", EntityState.class),

    PRIMITIVE_ENVIRONMENT_TYPE("environmentType", SearchEntityFieldDataType.ENUM, "environmentType", EnvironmentType.class),

    COMMODITY_VCPU_USED("vcpu_used", CommodityType.VCPU, CommodityAttribute.USED, CommodityTypeUnits.VCPU, SearchEntityFieldDataType.DECIMAL, "topo");

    @Nonnull
    private final String columnName;

    @Nonnull
    private SearchEntityFieldDataType searchEntityFieldDataType;

    @Nonnull
    private String topoPath; //TODO: This will likely be replaced with getter methods for extraction

    private Class<? extends Enum<?>> enumClass;

    private CommodityType commodityType;

    private CommodityAttribute commoditySubTyoe;

    private CommodityTypeUnits units;

    /**
     * Constructor of {@link SearchEntityMetadataMapping} with primitive data.
     *
     * @param columnName db column name
     * @param topoPath
     * @param searchEntityFieldDataType data structure descriptor of column data.
     * @param enumClass Enum Class for {@link SearchEntityFieldDataType#ENUM} data
     * @return SearchEntityMetaDataMapping
     */
    SearchEntityMetadataMapping(@Nonnull String columnName,
            @Nonnull SearchEntityFieldDataType searchEntityFieldDataType,
            @Nonnull String topoPath,
            @Nullable Class<? extends Enum<?>> enumClass) {
        this.columnName = Objects.requireNonNull(columnName);
        this.searchEntityFieldDataType = Objects.requireNonNull(searchEntityFieldDataType);
        this.topoPath = Objects.requireNonNull(topoPath);
        this.enumClass = enumClass;

    }

    //TODO: constructors for RelatedEntityFieldMapping
    //TODO: constructors for RelatedGroupFieldMapping
    //TODO: constructors for RelatedActionFieldMapping

    /**
     * Constructor of {@link SearchEntityMetadataMapping} with commodity data.
     *
     * @param columnName db column name
     * @param commodityType commodityType of data
     * @param commoditySubType subproperty of commodityType when configured
     * @param units Units for relevant {@link CommodityType} data
     * @param searchEntityFieldDataType data structure descriptor of column data
     * @return SearchEntityMetaDataMapping
     */
    SearchEntityMetadataMapping(
            @Nonnull String columnName,
            @Nonnull CommodityType commodityType,
            @Nonnull CommodityAttribute commoditySubType,
            @Nonnull CommodityTypeUnits units,
            @Nonnull SearchEntityFieldDataType searchEntityFieldDataType,
            @Nonnull String topoPath) {
        this.columnName = Objects.requireNonNull(columnName);
        this.searchEntityFieldDataType = Objects.requireNonNull(searchEntityFieldDataType);
        this.commodityType = Objects.requireNonNull(commodityType);
        this.commoditySubTyoe = Objects.requireNonNull(commoditySubType);
        this.units = Objects.requireNonNull(units);
        this.topoPath = Objects.requireNonNull(topoPath);
    }

    /**
     * Corresponds to DB column name.
     *
     * @return columnName
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Data structure descriptor of column data.
     *
     * @return SearchEntityFieldDataType
     */
    public SearchEntityFieldDataType getSearchEntityFieldDataType() {
        return searchEntityFieldDataType;
    }

    public String getTopoPath() {
        return topoPath;
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
    public CommodityAttribute getCommoditySubTyoe() {
        return commoditySubTyoe;
    }

    /**
     * Units for relevant {@link CommodityType} data.
     *
     * @return CommodityTypeUnits
     */
    public CommodityTypeUnits getUnits() {
        return units;
    }
}
