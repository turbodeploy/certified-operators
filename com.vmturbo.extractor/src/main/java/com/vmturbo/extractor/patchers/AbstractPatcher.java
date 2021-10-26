package com.vmturbo.extractor.patchers;

import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher.PatchCase;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialEntityInfo;

/**
 * Base class for entity information patchers.
 *
 * @param <DataProviderT> information source
 */
public abstract class AbstractPatcher<DataProviderT> implements EntityRecordPatcher<DataProviderT> {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Convert an enum to database value.
     * In search context enums are stored as integer ordinals.
     * In reporting/exporter contexts enums are mapped to declared database enumtypes.
     *
     * @param <SrcT> platform type
     * @param <DstEnumT> database enum type
     * @param value enum or ordinal value
     * @param patchCase export context
     * @param srcClass class of platform enum
     * @param nonSearchEnumMapper converter of platform enum value to db enum value
     * @return value for the db, null if failed to convert
     */
    @VisibleForTesting
    @Nullable
    static <SrcT, DstEnumT extends Enum<DstEnumT>> Object
                    getEnumDbValue(@Nullable Object value, @Nonnull PatchCase patchCase,
                                    @Nonnull Class<SrcT> srcClass,
                                    @Nullable Function<SrcT, DstEnumT> nonSearchEnumMapper) {
        if (srcClass.isInstance(value) || Number.class.isInstance(value)) {
            @SuppressWarnings("unchecked")
            SrcT src = (SrcT)value;
            if (patchCase == PatchCase.SEARCH) {
                if (Enum.class.isAssignableFrom(srcClass)) {
                    return ((Enum)src).ordinal();
                } else {
                    return src;
                }
            } else if (nonSearchEnumMapper != null) {
                return nonSearchEnumMapper.apply(src);
            } else {
                logger.error("Enum to db mapping for value " + value + " is not specified");
            }
        } else {
            logger.error("Source value " + value + " is not of expected type " + srcClass.getSimpleName());
        }
        return null;
    }

    /**
     * Fetch the additional attributes in a free-form map (for json).
     * Type-specific values, tags, targets.
     *
     * @param e data source
     * @return map of field name to value
     */
    @Nonnull
    public Map<String, Object> extractAttrs(@Nonnull DataProviderT e) {
        PartialEntityInfo info = new PartialEntityInfo(0);
        extractAttrs(info, e);
        return info.getAttrs();
    }

    /**
     * Extract attributes from given entity.
     *
     * @param recordInfo holder of data to persist
     * @param e topology entity
     */
    protected void extractAttrs(@Nonnull PartialEntityInfo recordInfo, @Nonnull DataProviderT e) {}
}
