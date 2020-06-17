package com.vmturbo.extractor.models;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

/**
 * Class to represent columns in database tables.
 *
 * @param <T> (Java) type of data stored in a column
 */
public class Column<T> {

    /**
     * Mapping from Java classes to database types to be used in column definitions in DDL.
     *
     * <p>These are needed when creating temporary tables in some record sinks</p>
     */
    public static final Map<Class<?>, String> DB_TYPE_MAP = ImmutableMap.<Class<?>, String>builder()
            .put(String.class, "string")
            .put(Integer.class, "int4")
            .put(Short.class, "int2")
            .put(Long.class, "int8")
            .put(Float.class, "float4")
            .put(Double.class, "float8")
            .put(Boolean.class, "boolean")
            .put(String[].class, "string[]")
            .put(Integer[].class, "int4[]")
            .put(Short[].class, "int2[]")
            .put(Long[].class, "int8[]")
            .put(Float[].class, "float4[]")
            .put(Double[].class, "float8[]")
            .put(Boolean[].class, "boolean[]")
            .put(Timestamp.class, "timestamptz")
            .put(JsonString.class, "jsonb")
            .build();
    private static final byte[] NULL_HASH_VALUE = {0};
    private static final byte[] TRUE_BYTE_ARRAY = {1};
    private static final byte[] FALSE_BYTE_ARRAY = {0};
    private final String name;
    private final Class<T> type;
    private final CsvRenderer csvRenderer;
    private final XxHashBytesRenderer hashBytesRenderer;

    /**
     * Internal constructor to create a new column.
     *
     * <p>Client code should make use of the {@link #of(String, Class)} method to obtain a
     * builder.</p>
     *
     * @param name              name of column
     * @param type              (Java) type of column data
     * @param csvRenderer       function to render a column value for inclusion in a CSV record
     * @param hashBytesRenderer function to compute a byte array to be fed to a record hash
     *                          function
     */
    private Column(String name, Class<T> type, CsvRenderer csvRenderer,
            XxHashBytesRenderer hashBytesRenderer) {
        this.name = name;
        this.type = type;
        this.csvRenderer = csvRenderer;
        this.hashBytesRenderer = hashBytesRenderer;
    }

    /**
     * Create a new builder for a column with a given name and type.
     *
     * @param name column name
     * @param type {@link Class} corresponding to column type
     * @param <X>  column type
     * @return new builder
     */
    public static <X> Column.Builder<X> of(String name, Class<X> type) {
        return new Column.Builder<>(name, type);
    }

    /**
     * Create a new int column builder with default functions.
     *
     * @param name column name
     * @return new builder
     */
    public static Column.Builder<Integer> intColumn(final String name) {
        return Column.of(name, Integer.class).withHashFunc(i -> toBytes((int)i));
    }

    /**
     * Create a new short column builder with default functions.
     *
     * @param name name of column
     * @return new builder
     */
    public static Column.Builder<Short> shortColumn(final String name) {
        return Column.of(name, Short.class).withHashFunc(i -> toBytes((short)i));
    }

    /**
     * Create a new long column builder with default functions.
     *
     * @param name column name
     * @return new builder
     */
    public static Column.Builder<Long> longColumn(final String name) {
        return Column.of(name, Long.class).withHashFunc(i -> toBytes((long)i));
    }

    /**
     * Create a new double column builder with standard functions.
     *
     * @param name column name
     * @return new builder
     */
    public static Column.Builder<Double> doubleColumn(final String name) {
        return Column.of(name, Double.class).withHashFunc(f -> toBytes(Double.doubleToLongBits((double)f)));
    }
    /**
     * Create a new float column builder with standard functions.
     *
     * @param name column name
     * @return new builder
     */

    public static Column.Builder<Float> floatColumn(final String name) {
        return Column.of(name, Float.class).withHashFunc(f -> toBytes(Float.floatToIntBits((float)f)));
    }

    /**
     * Create a new String column builder with standard functions.
     *
     * @param name column name
     * @return new builder
     */
    public static Column.Builder<String> stringColumn(final String name) {
        return Column.of(name, String.class).withCsvFunc(s -> quoteString((String)s));
    }

    /**
     * Create a new boolean column builder with standard functions.
     *
     * @param name column name
     * @return new builder
     */
    public static Column.Builder<Boolean> boolColumn(final String name) {
        return Column.of(name, Boolean.class).withHashFunc(b ->
                (boolean)b ? TRUE_BYTE_ARRAY : FALSE_BYTE_ARRAY);
    }

    /**
     * Create a new int[] column builder with standard functions.
     *
     * @param name column name
     * @return new builder
     */
    public static Column.Builder<Integer[]> intArrayColumn(final String name) {
        return Column.of(name, Integer[].class)
                .withCsvFunc(array -> Column.arrayToCsv((Integer[])array))
                .withHashFunc(ia -> toBytes((Integer[])ia));
    }

    /**
     * Create a new long[] column builder with standard functions.
     *
     * @param name column name
     * @return new builder
     */
    public static Column.Builder<Long[]> longArrayColumn(final String name) {
        return Column.of(name, Long[].class)
                .withCsvFunc(array -> Column.arrayToCsv((Long[])array))
                .withHashFunc(la -> toBytes((Long[])la));
    }

    /**
     * Create a new float[] column builder.
     *
     * @param name column name
     * @return new builder
     */
    public static Column.Builder<Float[]> floatArrayColumn(final String name) {
        return Column.of(name, Float[].class)
                .withCsvFunc(array -> Column.arrayToCsv((Float[])array))
                .withHashFunc(fa -> toBytes((Float[])fa));
    }

    /**
     * Create a new boolean[] column builder with standard functions.
     *
     * @param name column name
     * @return new builder
     */
    public static Column.Builder<Boolean[]> boolArrayColumn(final String name) {
        return Column.of(name, Boolean[].class)
                .withCsvFunc(array -> Column.arrayToCsv((Boolean[])array))
                .withHashFunc(ba -> toBytes((Boolean[])ba));
    }

    /**
     * Create a new String[] column builder with standard functions.
     *
     * @param name column name
     * @return new builder
     */
    public static Column.Builder<String[]> stringArrayColumn(final String name) {
        return Column.of(name, String[].class)
                .withCsvFunc(array -> Column.arrayToCsv((String[])array))
                .withHashFunc(sa -> toBytes((String[])sa));
    }

    /**
     * Create a {@link Timestamp} column builder with default functions.
     *
     * @param name column name
     * @return new column
     */
    public static Column.Builder<Timestamp> timestampColumn(final String name) {
        return Column.of(name, Timestamp.class).withHashFunc(ts -> toBytes((Timestamp)ts));
    }

    /**
     * Create a new JSON column builder with standard functions.
     *
     * @param name column name
     * @return new builder
     */
    public static Column.Builder<JsonString> jsonColumn(final String name) {
        return Column.of(name, JsonString.class).withCsvFunc(json -> quoteString(json.toString()));
    }

    /**
     * Hash function for long[] column.
     *
     * @param la column value
     * @return concatenation of bytes for individual element values
     */
    public static byte[] toBytes(Long[] la) {
        // we'll need room for each long value
        byte[] bytes = new byte[Long.BYTES * la.length];
        for (int i = 0; i < la.length; i++) {
            // nulls retain their initial zero values
            if (la[i] != null) {
                // others are rendered as bytes and copied to correct position in the result
                System.arraycopy(toBytes(la[i]), 0, bytes, Long.BYTES * i, Long.BYTES);
            }
        }
        return bytes;
    }

    /**
     * Hash function for int columns.
     *
     * @param i column value
     * @return bytes laid out in big-endian order
     */
    private static byte[] toBytes(int i) {
        return new byte[]{
                (byte)((i >> 24) & 0xff),
                (byte)((i >> 16) & 0xff),
                (byte)((i >> 8) & 0xff),
                (byte)(i & 0xff)
        };
    }

    /**
     * Hash function for a short column.
     *
     * @param i column value
     * @return bytes laid out in big-endian order
     */
    private static byte[] toBytes(final short i) {
        return new byte[]{
                (byte)((i >> 8) & 0xff),
                (byte)(i & 0xff)
        };
    }

    /**
     * Hash function for a long column.
     *
     * @param i column value
     * @return bytes laid out in big-endian order
     */
    static byte[] toBytes(final long i) {
        return new byte[]{
                (byte)((i >> 56) & 0xff),
                (byte)((i >> 48) & 0xff),
                (byte)((i >> 40) & 0xff),
                (byte)((i >> 32) & 0xff),
                (byte)((i >> 24) & 0xff),
                (byte)((i >> 16) & 0xff),
                (byte)((i >> 8) & 0xff),
                (byte)(i & 0xff)
        };
    }

    /**
     * Hash function for an int array.
     *
     * @param ia int array value
     * @return concatenation of bytes for individual element values
     */
    public static byte[] toBytes(Integer[] ia) {
        byte[] bytes = new byte[Integer.BYTES * ia.length];
        for (int i = 0; i < ia.length; i++) {
            if (ia[i] != null) {
                System.arraycopy(toBytes(ia[i]), 0, bytes, Integer.BYTES * i, Integer.BYTES);
            }
        }
        return bytes;
    }

    /**
     * Hasn function for a boolean[] column.
     *
     * @param ba column value
     * @return concatenation of individual element values
     */
    public static byte[] toBytes(Boolean[] ba) {
        byte[] bytes = new byte[ba.length];
        for (int i = 0; i < ba.length; i++) {
            bytes[i] = ba[i] == null ? (byte)2 : ba[i] ? (byte)1 : (byte)0;
        }
        return bytes;
    }

    /**
     * Hash function for a float[] column.
     *
     * @param fa column value
     * @return concatenation of bytes for individual element values
     */
    public static byte[] toBytes(Float[] fa) {
        byte[] bytes = new byte[Float.BYTES * fa.length];
        for (int i = 0; i < fa.length; i++) {
            if (fa[i] != null) {
                System.arraycopy(toBytes(Float.floatToIntBits(fa[i])), 0,
                        bytes, Float.BYTES * i, Float.BYTES);
            }
        }
        return bytes;
    }

    /**
     * Hash function for a String[] column.
     *
     * @param sa column value
     * @return concatenation of bytes for individual element values
     */
    public static byte[] toBytes(String[] sa) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            for (final String s : sa) {
                baos.write(s.getBytes(UTF_8));
                baos.write(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return baos.toByteArray();
    }

    /**
     * Hash function for {@link Timestamp} column.
     *
     * @param ts column value
     * @return timestamp as milliseconds since epoch, laid out in big-endian order
     */
    public static byte[] toBytes(Timestamp ts) {
        return toBytes(ts.getTime());
    }

    /**
     * Represent an array column value in CSV format.
     *
     * <p>We use the Postgres array literal syntax and quote the literal.</p>
     *
     * @param array array value
     * @param <T>   type of array elements
     * @return CSV representation
     */
    public static <T> String arrayToCsv(T[] array) {
        return quoteString(toArrayLiteral(array));
    }

    /**
     * Quote a string value for CSV.
     *
     * <p>We double embedded quotes and quote the result.</p>
     *
     * @param o string value
     * @return CSV representation
     */
    private static String quoteString(final String o) {
        return o != null ? "\"" + o.replaceAll("\"", "\"\"") + "\"" : null;
    }

    /**
     * Render an array value as a Postgres array literal.
     *
     * @param values array values
     * @return array literal representation
     */
    private static String toArrayLiteral(Object[] values) {
        String content = Arrays.stream(values)
                .map(Column::toArrayValue)
                .collect(Collectors.joining(","));
        return "{" + content + "}";
    }

    /**
     * Render an array element value in a manner suitable for inclusion in a Postgres array
     * literal.
     *
     * @param o element value
     * @return rendered value
     */
    private static String toArrayValue(Object o) {
        if (o == null) {
            // null elements appear as the word NULL
            return "NULL";
        } else if (o instanceof String) {
            // strings have embedded quotes escaped with backslash, and are then quoted
            return "\"" + ((String)o).replaceAll("\"", "\\\"") + "\"";
        } else {
            // everything else just uses built-in string rendering
            return o.toString();
        }
    }

    public String getName() {
        return name;
    }

    public Class<T> getType() {
        return type;
    }

    /**
     * Get the DB type corresponding to this column's Java type.
     *
     * @return DB type name
     */
    public String getDbType() {
        return DB_TYPE_MAP.get(type);
    }

    /**
     * Render this column's value in a manner suitable for inclusion in a CSV record.
     *
     * @param value column value
     * @return CSV string
     */
    public String toCsvValue(Object value) {
        return value != null ? csvRenderer.toCsv(value) : "";
    }

    /**
     * Compute a byte[] representation of the given column value, for use in calculating a hash
     * value for a record.
     *
     * @param value column value
     * @return bytes to be used in hash
     */
    public byte[] toHashValue(Object value) {
        return value != null ? hashBytesRenderer.toHashXxBytes(value) : NULL_HASH_VALUE;
    }

    /**
     * Builder class for columns.
     *
     * @param <T> column type
     */
    public static class Builder<T> {

        private final String name;
        private final Class<T> type;
        private final List<Consumer<Builder<T>>> transforms;
        private CsvRenderer csvRenderer = Object::toString;
        private XxHashBytesRenderer hashBytesRenderer = o -> o.toString().getBytes(UTF_8);

        /**
         * Create a builder for a column with a given name and type.
         *
         * @param name column name
         * @param type (Java) type of column
         */
        private Builder(String name, Class<T> type) {
            this(name, type, new ArrayList<>());
        }

        /**
         * Create a new builder with a given name and type, and with a set of transforms.
         *
         * @param name       column name
         * @param type       column type
         * @param transforms transforms to be applied
         */
        private Builder(String name, Class<T> type, List<Consumer<Builder<T>>> transforms) {
            this.name = name;
            this.type = type;
            this.transforms = transforms;
        }

        /**
         * Specify a csv rendering function for this column.
         *
         * @param csvRenderer csv function
         * @return this builder
         */
        public Builder<T> withCsvFunc(CsvRenderer csvRenderer) {
            return withTransform(b -> b.csvRenderer = csvRenderer);
        }

        /**
         * Specify a hash bytes function for this column.
         *
         * @param hashBytesRenderer hash bytes function
         * @return this builder
         */
        public Builder<T> withHashFunc(XxHashBytesRenderer hashBytesRenderer) {
            return withTransform(b -> b.hashBytesRenderer = hashBytesRenderer);
        }

        /**
         * Add a transform to this builder.
         *
         * @param transform transform
         * @return this builder
         */
        private Builder<T> withTransform(Consumer<Builder<T>> transform) {
            transforms.add(transform);
            return this;
        }

        /**
         * Build this column.
         *
         * @return new column
         */
        public Column<T> build() {
            for (final Consumer<Builder<T>> transform : transforms) {
                transform.accept(this);
            }
            return new Column<>(name, type, csvRenderer, hashBytesRenderer);
        }

        public String getName() {
            return name;
        }

        /**
         * Create a new builder copying the current builder but specifying a different column name.
         *
         * @param alias name for new builder
         * @return new column builder
         */
        public Builder<T> asAliased(String alias) {
            return new Builder<>(alias, type, transforms);
        }
    }

    /**
     * A wrapper for string values for columns that use the Postgres 'jsonb' column type.
     */
    public static class JsonString {

        private final String json;

        /**
         * Create a new instance.
         *
         * @param json JSON value
         */
        public JsonString(final String json) {
            this.json = json;
        }

        @Override
        public String toString() {
            return json;
        }
    }

    /**
     * Interface for a function that renders a column value to a form suitable for inclusion in a
     * CSV string representing a containing table row.
     */
    @FunctionalInterface
    public interface CsvRenderer {
        /**
         * Render the column value for CSV.
         *
         * @param value column value
         * @return CSV rendering
         */
        String toCsv(Object value);
    }

    /**
     * Interface for a function that renders a column value as a byte array suitable for
     * incorporation into an XxHash value for a containing table row.
     *
     * <p>See {@link HashUtil} for a brief explanation of why we hash column values.</p>
     */
    @FunctionalInterface
    public interface XxHashBytesRenderer {
        /**
         * Render the column value for XxHash.
         *
         * @param value column valuenn
         * @return bytes for XxHash
         */
        byte[] toHashXxBytes(Object value);
    }

}
