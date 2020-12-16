package com.vmturbo.components.common.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.BiMap;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * Interface and some implementations of data packs, which map distinct values of a given type to
 * distinct int values. A primary goal of this is to create space-efficient data structures that can
 * use integer values (generally unboxed) to reference instances of the target type.
 *
 * <p>A data pack functions in many ways similarly to a {@link BiMap}, but its internal structures
 * are carefully designed to minimize storage overhead, including avoidance of boxed
 * primitives.</p>
 */

public class DataPacks {
    private DataPacks() {
    }

    /**
     * Interface for a data pack.
     *
     * @param <T> type of values stored in the data pack.
     */
    public interface IDataPack<T> {
        /** value returned as the index for a value that's not in the data pack. */
        int MISSING = -1;

        /**
         * Look up a value in the data pack, and if not found, add it as a side-effect.
         *
         * <p>If the data pack is frozen and the value is not present, {@link #MISSING} is
         * returned.</p>
         *
         * @param value value to be looked up
         * @return data pack index for this value
         */
        default int toIndex(T value) {
            return toIndex(value, true);
        }

        /**
         * Look up a value in the data pack, and if not found, maybe add it as a side-effect.
         *
         * <p>If the data pack is frozen and the value is not present {@link #MISSING} is returned
         * regardless of the value of the `addIfMissing` parameter.
         *
         * @param value        value to be looked up
         * @param addIfMissing true to add this value if it's not already present
         * @return data pack index for this value, or MISSING if not found and not added
         */
        int toIndex(T value, boolean addIfMissing);

        /**
         * Get the value stored at the given index in the data pack.
         *
         * @param index data pack index
         * @return value stored at that index
         * @throws ArrayIndexOutOfBoundsException if the index is < 0 or >= size
         */
        T fromIndex(int index) throws ArrayIndexOutOfBoundsException;

        /**
         * Remove all values from the data pack.
         */
        void clear();

        /**
         * If the underlying data structure supports it, attempt to optimize its storage without
         * altering the data pack's content.
         */
        void trim();

        /**
         * Freeze the data pack from additional updates, and optionally disable lookups by value.
         *
         * <p>Once a data pack is frozen, it will no longer accept new values. The {@link
         * #toIndex(Object)} methods will never add a value and will return {@link #MISSING} if the
         * indicated value is not present.</p>
         *
         * <p>If the `disableValueLookup` parameter is true, the data required to support value
         * lookups will be discarded to reduce space overhead, and the {@link #toIndex(Object)}
         * methods will always return {@link #MISSING}.</p>
         *
         * <p>It is not possible to unfreeze a frozen data pack.</p>
         *
         * <p>Freezing a data pack implicitly casues it to be trimmed as well.</p>
         *
         * <p>A frozen data pack <b>can</b> be cleared!</p>
         *
         * @param disableValueLookup true to discard data need for value lookups
         */
        void freeze(boolean disableValueLookup);

        /**
         * Test whether this data pack is frozen.
         *
         * @return true if the data pack is frozen
         */
        boolean isFrozen();

        /**
         * Check whether this data pack is capable of performing value lookups. That will always
         * be the case unless the pack was frozen and `disableValueLookup` parameter was set to
         * true.
         *
         * @return true if this data pack can perform value lookups.
         */
        boolean canLookupValues();

        /**
         * Get the number of values currently stored in the data pack.
         *
         * @return data pack size
         */
        int size();
    }

    /**
     * Generic implementation of a data pack.
     *
     * @param <T> type of data pack entries
     */
    public static class DataPack<T> implements IDataPack<T> {

        // this is the list of values in the data pack, in insertion order. List indexes are the
        // index values provided by the data pack.
        protected final List<T> valueList = createValueList();
        // This is a map relating data pack values to their indexes.
        protected final Map<T, Integer> baseValueMap = createValueMap();
        protected final Map<T, Integer> valueMap = Collections.synchronizedMap(baseValueMap);
        // true when this data pack is frozen
        protected boolean frozen;
        protected boolean valueLookupsDisabled;

        @Override
        public int toIndex(T value, boolean addIfMissing) {
            int index = valueMap.getOrDefault(value, MISSING);
            if (index == MISSING && addIfMissing && !frozen) {
                synchronized (this) {
                    // check again in case it snuck in before we synchronized
                    index = valueMap.getOrDefault(value, MISSING);
                    if (index == MISSING) {
                        index = valueList.size();
                        valueList.add(value);
                        valueMap.put(value, index);
                    }
                }
            }
            return index;
        }

        @Override
        public T fromIndex(int index) {
            return valueList.get(index);
        }

        @Override
        public synchronized void clear() {
            valueList.clear();
            valueMap.clear();
            trim();
        }

        @Override
        public void trim() {
            ((ArrayList<?>)valueList).trimToSize();
            ((Object2IntOpenHashMap<T>)baseValueMap).trim();
        }

        /**
         * Create the values list for this data pack.
         *
         * <p>By default we just use an {@link ArrayList}</p>
         *
         * <p>If you override this, there's a good chance you should override {@link #trim()}, and
         * perhaps {@link #clear()}.</p>
         *
         * @return the values list
         */
        protected List<T> createValueList() {
            return new ArrayList<>();
        }

        /**
         * Create the values map for this data pack.
         *
         * <p>By default we create a fastutil {@link Object2IntMap}</p>
         *
         * <p>If you override this, there's a good chance you should override {@link #trim()}, and
         * perhaps {@link #clear()}.</p>
         *
         * @return the values map
         */
        protected Map<T, Integer> createValueMap() {
            return new Object2IntOpenHashMap<>();
        }

        @Override
        public int size() {
            return valueList.size();
        }

        @Override
        public void freeze(final boolean disableValueLookup) {
            synchronized (this) {
                this.frozen = true;
                if (disableValueLookup) {
                    this.valueLookupsDisabled = true;
                    valueMap.clear();
                }
            }
            trim();
        }

        @Override
        public boolean isFrozen() {
            return frozen;
        }

        @Override
        public boolean canLookupValues() {
            return !valueLookupsDisabled;
        }
    }

    /**
     * A data pack of (non-null) long values.
     */
    public static class LongDataPack extends DataPack<Long> {
        @Override
        public List<Long> createValueList() {
            return new LongArrayList();
        }

        @Override
        protected Map<Long, Integer> createValueMap() {
            return new Long2IntOpenHashMap();
        }

        @Override
        public void trim() {
            ((LongArrayList)valueList).trim();
            ((Long2IntOpenHashMap)baseValueMap).trim();
        }
    }

    /**
     * A data pack of {@link TopologyDTO.CommodityType} values.
     *
     * <p>This is generally more compact than a `DataPack&lt;CommodityType&gt;` would be. Where the
     * latter would include object overhead for each distinct {@link CommodityType} value, as well
     * as for each distinct commodity key (or worse, if the JDK is not performming string
     * deduplication), here we incur object overhead only for distinct commodity keys.</p>
     */
    public static class CommodityTypeDataPack implements IDataPack<CommodityType> {
        // overall data pack. Each commodity type is encoded as a long by placing the SDK type
        // number in the high-order half, and the commodity string index in the low-order half
        private final LongDataPack longPack = new LongDataPack();
        // String data pack for commodity keys
        private final DataPack<String> keyPack = new DataPack<>();
        private boolean frozen;

        @Override
        public synchronized int toIndex(final CommodityType value, final boolean addIfMissing) {
            final int keyIndex = keyPack.toIndex(value.getKey(), addIfMissing);
            if (keyIndex == MISSING) {
                return MISSING;
            }
            long packed = packInts(value.getType(), keyIndex);
            return longPack.toIndex(packed, addIfMissing);
        }

        @Override
        public synchronized CommodityType fromIndex(final int index) {
            int[] packedInts = unpackInts((long)longPack.fromIndex(index));
            return CommodityType.newBuilder()
                    .setType(packedInts[0])
                    .setKey(keyPack.fromIndex(packedInts[1]))
                    .build();
        }

        @Override
        public synchronized void clear() {
            longPack.clear();
            keyPack.clear();
            trim();
        }

        @Override
        public int size() {
            return longPack.size();
        }

        @Override
        public void trim() {
            longPack.trim();
            keyPack.trim();
        }

        @Override
        public synchronized void freeze(final boolean disableValueLookup) {
            this.frozen = true;
            longPack.freeze(disableValueLookup);
            keyPack.freeze(disableValueLookup);
        }

        @Override
        public boolean isFrozen() {
            return frozen;
        }

        @Override
        public boolean canLookupValues() {
            return longPack.canLookupValues();
        }
    }

    private static final long INT_MASK = (1L << Integer.SIZE) - 1;

    /**
     * Utility method to unpack to int values from a long.
     *
     * @param packed long value
     * @return pair of int values, high-order first
     */
    public static int[] unpackInts(long packed) {
        return new int[]{(int)(packed >>> Integer.SIZE), (int)(packed & INT_MASK)};
    }

    /**
     * Utility to pack two int values into a long value.
     *
     * @param high int value to store in high-order bytes of packed long
     * @param low  int value to store in low-order bytes of packed long
     * @return packed long value
     */
    public static long packInts(int high, int low) {
        return (((long)high) << Integer.SIZE)
                | (((long)low) & INT_MASK);
    }
}
