package com.vmturbo.extractor.topology.attributes;

import java.sql.Timestamp;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.ModelDefinitions.HistoricalAttributes;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.schema.enums.AttrType;

/**
 * A {@link HistoricalAttributeProcessor} is responsible for extracting values for individual
 * attributes from a broadcast, detecting whether its changed, and creating a historical
 * attribute {@link Record} from these values. These records are then written into the
 * database by the {@link HistoricalAttributeWriter}.
 *
 * @param <V> The value type.
 */
abstract class HistoricalAttributeProcessor<V> {
    private final Column<V> valueColumn;

    private final AttrType attrType;

    /**
     * Reference to the (entity id) -> (hash) map from the {@link HistoricalAttributeProcessorFactory}.
     * Contains the hashes from previous broadcasts, and lets us figure out if the attribute
     * value changed.
     */
    private final Long2LongMap hashByEntityIdMap;

    /**
     * We build up the map of new hashes during entity processing, and fold this map into
     * the original map (the one that lives across broadcasts) when topology processing is
     * complete.
     */
    private final Long2LongMap newHashesMap = new Long2LongOpenHashMap();

    private final Consumer<Long2LongMap> successHashesConsumer;

    protected HistoricalAttributeProcessor(Column<V> valueColumn, AttrType attrType,
            Long2LongMap hashByEntityIdMap,
            Consumer<Long2LongMap> successHashesConsumer) {
        this.valueColumn = valueColumn;
        this.attrType = attrType;
        this.hashByEntityIdMap = hashByEntityIdMap;
        this.successHashesConsumer = successHashesConsumer;
    }

    @Nullable
    public Record processEntity(TopologyEntityDTO entity, TopologyInfo topologyInfo) {
        V value = extractValue(entity);
        if (value != null) {
            long oldHash = hashByEntityIdMap.get(entity.getOid());
            long newHash = hash(value);
            if (oldHash != newHash) {
                newHashesMap.put(entity.getOid(), newHash);
                Record record = makeRecordTemplate(entity, topologyInfo);
                record.set(valueColumn, value);
                return record;
            }
        }
        return null;
    }

    public void onSuccess() {
        successHashesConsumer.accept(newHashesMap);
    }

    private long hash(V value) {
        long hash = valueColumn.getColType().xxxHash(value);
        return hash == 0 ? 1 : hash;
    }

    private Record makeRecordTemplate(TopologyEntityDTO entity, TopologyInfo topologyInfo) {
        Record record = new Record(HistoricalAttributes.TABLE);
        record.set(HistoricalAttributes.TIME, new Timestamp(topologyInfo.getCreationTime()));
        record.set(HistoricalAttributes.ENTITY_OID, entity.getOid());
        record.set(HistoricalAttributes.TYPE, attrType);
        return record;
    }

    @Nullable
    abstract V extractValue(TopologyEntityDTO entity);
}
