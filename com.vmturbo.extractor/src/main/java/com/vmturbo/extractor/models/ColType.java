package com.vmturbo.extractor.models;

import static java.nio.charset.StandardCharsets.UTF_16BE;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.jpountz.xxhash.StreamingXXHash64;

import org.apache.commons.codec.binary.Hex;
import org.jooq.EnumType;

import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.schema.enums.ActionCategory;
import com.vmturbo.extractor.schema.enums.ActionMode;
import com.vmturbo.extractor.schema.enums.ActionState;
import com.vmturbo.extractor.schema.enums.ActionType;
import com.vmturbo.extractor.schema.enums.AttrType;
import com.vmturbo.extractor.schema.enums.CostCategory;
import com.vmturbo.extractor.schema.enums.CostSource;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.schema.enums.FileType;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.schema.enums.SavingsType;
import com.vmturbo.extractor.schema.enums.Severity;
import com.vmturbo.extractor.schema.enums.TerminalState;

/**
 * Column types that we support in our model definitions.
 */
public enum ColType {
    /** String column. */
    STRING("varchar"),
    /** String array column. */
    STRING_ARRAY("varchar[]"),
    /** Long column. */
    LONG("int8"),
    /** Long array column. */
    LONG_ARRAY("int8[]"),
    /** Long set column - like long array but order-independent hashing. */
    LONG_SET("int8[]"),
    /** Integer column. */
    INT("int4"),
    /** Integer array column. */
    INT_ARRAY("int4[]"),
    /** Short column. */
    SHORT("int2"),
    /** Short array column. */
    SHORT_ARRAY("int2[]"),
    /** Double column. */
    DOUBLE("float8"),
    /** Double array column. */
    DOUBLE_ARRAY("float8[]"),
    /** Float column. */
    FLOAT("float4"),
    /** Float array column. */
    FLOAT_ARRAY("float4[]"),
    /** Boolean column. */
    BOOL("boolean"),
    /** JSON string column - like string but corresponds to JSONB postscript type. */
    JSON("jsonb"),
    /** Timestamp column. */
    TIMESTAMP("timestamptz"),
    /** OffsetDateTime column. */
    OFFSET_DATE_TIME("timestamptz"),
    /**
     * entity_type column. the type selected here doesn't really matter, getName here means the type
     * name (like entity_type) registered in db, not the member name (APPLICATION). Unfortunately
     * Jooq doesn't generate a static method to get type name, and getName() returns same type name
     * for all enum members. Same for other enums below.
     */
    ENTITY_TYPE(EntityType.values()[0].getName()),
    /** environment_type column. */
    ENVIRONMENT_TYPE(EnvironmentType.values()[0].getName()),
    /** entity_state column. */
    ENTITY_STATE(EntityState.values()[0].getName()),

    /** severity column. */
    SEVERITY(Severity.values()[0].getName()),

    /**
     * Commodity type column.
     */
    METRIC_TYPE(MetricType.values()[0].getName()),

    /**
     * Action type column.
     */
    ACTION_TYPE(ActionType.values()[0].getName()),
    /**
     * Action final state column.
     */
    FINAL_STATE(TerminalState.values()[0].getName()),
    /**
     * Action category column.
     */
    ACTION_CATEGORY(ActionCategory.values()[0].getName()),

    /**
     * Action state column.
     */
    ACTION_STATE(ActionState.values()[0].getName()),

    /**
     * Action mode column.
     */
    ACTION_MODE(ActionMode.values()[0].getName()),

    /** attr_type column of historical attributes. */
    ATTR_TYPE(AttrType.values()[0].getName()),

    /** cost categiry column. */
    COST_CATEGORY(CostCategory.values()[0].getName()),

    /** cost source column. */
    COST_SOURCE(CostSource.values()[0].getName()),

    /** file type column. */
    FILE_TYPE(FileType.values()[0].getName()),

    /** savings type column. */
    SAVINGS_TYPE(SavingsType.values()[0].getName());

    static final byte[] NULL_BYTE_ARRAY = {0};
    static final byte[] TRUE_BYTE_ARRAY = {1};
    static final byte[] FALSE_BYTE_ARRAY = {0};
    private final String postgresType;

    ColType(String postgresType) {
        this.postgresType = postgresType;
    }

    /**
     * Get the postgres database type that should be used for this column type.
     *
     * @return name of postgres type
     */
    public String getPostgresType() {
        return postgresType;
    }

    /**
     * Compute bytes that should be contributed by this column value into a hash value for a
     * containing record.
     *
     * @param value value of column
     * @return bytes to be contributed to hash calculation
     */
    public byte[] toBytes(Object value) {
        return toBytes(value, this);
    }

    private static byte[] toBytes(Object value, ColType colType) {
        if (value == null) {
            return NULL_BYTE_ARRAY;
        }
        switch (colType) {
            case STRING:
                return ((String)value).getBytes(UTF_8);
            case STRING_ARRAY: {
                // we create bytes from UTC-16 encoding, so we can accurately size a byte buffer
                // we stick a zero-char in between (i.e. two zero bytes) to mark boundaries
                final String[] strings = (String[])value;
                final long size = Arrays.stream(strings).mapToLong(s -> 2 * s.length() + 2).sum();
                final ByteBuffer bb = ByteBuffer.allocate((int)size);
                Arrays.stream(strings).map(s -> s.getBytes(UTF_16BE)).forEach(bytes -> {
                    bb.put(bytes);
                    bb.put(new byte[]{0, 0});
                });
                return bb.array();
            }
            case LONG:
                return ByteBuffer.allocate(Long.BYTES).putLong((Long)value).array();
            case LONG_ARRAY: {
                final Long[] longs = (Long[])value;
                final ByteBuffer bb = ByteBuffer.allocate(longs.length * Long.BYTES);
                for (final Long aLong : longs) {
                    bb.putLong(aLong);
                }
                return bb.array();
            }
            case LONG_SET: {
                long xor = 0;
                final Long[] longs = (Long[])value;
                for (final long aLong : longs) {
                    xor += LONG.xxxHash(aLong);
                }
                return LONG.toBytes(xor);
            }
            case INT:
                return ByteBuffer.allocate(Integer.BYTES).putInt((Integer)value).array();
            case INT_ARRAY: {
                final Integer[] ints = (Integer[])value;
                final ByteBuffer bb = ByteBuffer.allocate(ints.length * Integer.BYTES);
                for (final Integer anInt : ints) {
                    bb.putInt(anInt);
                }
                return bb.array();
            }
            case SHORT:
                return ByteBuffer.allocate(Short.BYTES).putShort((Short)value).array();
            case SHORT_ARRAY: {
                final Short[] shorts = (Short[])value;
                final ByteBuffer bb = ByteBuffer.allocate(shorts.length * Short.BYTES);
                for (final Short aShort : shorts) {
                    bb.putShort(aShort);
                }
                return bb.array();
            }
            case DOUBLE:
                return ByteBuffer.allocate(Double.BYTES).putDouble((Double)value).array();
            case DOUBLE_ARRAY: {
                final Double[] doubles = (Double[])value;
                final ByteBuffer bb = ByteBuffer.allocate(doubles.length * Double.BYTES);
                for (final Double aDouble : doubles) {
                    bb.putDouble(aDouble);
                }
                return bb.array();
            }
            case FLOAT:
                return ByteBuffer.allocate(Float.BYTES).putFloat((Float)value).array();
            case FLOAT_ARRAY: {
                final Float[] floats = (Float[])value;
                final ByteBuffer bb = ByteBuffer.allocate(floats.length * Float.BYTES);
                for (final Float aFloat : floats) {
                    bb.putFloat(aFloat);
                }
                return bb.array();
            }
            case BOOL:
                return (Boolean)value ? TRUE_BYTE_ARRAY : FALSE_BYTE_ARRAY;
            case JSON:
                return value.toString().getBytes(UTF_8);
            case TIMESTAMP:
                return ByteBuffer.allocate(Long.BYTES + Integer.BYTES).putLong(((Timestamp)value).getTime())
                        .putInt(((Timestamp)value).getNanos()).array();
            case OFFSET_DATE_TIME: {
                Instant t = ((OffsetDateTime)value).toInstant();
                return ByteBuffer.allocate(Long.BYTES + Integer.BYTES)
                        .putLong(t.getEpochSecond()).putInt(t.getNano()).array();
            }
            case ENTITY_TYPE:
            case ENVIRONMENT_TYPE:
            case SEVERITY:
            case ENTITY_STATE:
            case ACTION_TYPE:
            case FINAL_STATE:
            case ACTION_STATE:
            case ACTION_MODE:
            case ACTION_CATEGORY:
            case ATTR_TYPE:
            case FILE_TYPE:
                return ((EnumType)value).getLiteral().getBytes(UTF_8);
            default:
                throw new IllegalArgumentException("Unknown column type: " + colType.name());
        }
    }

    /**
     * Obtain the value that gave rise to the indicated bytes, if possible.
     *
     * <p>This is not a method that's used in production, but it's used in some tests.</p>
     *
     * @param bytes bytes produced from some value, using {@link #toBytes(Object)}
     * @return that value, if it can be determined
     */
    public Object fromBytes(byte[] bytes) {
        return fromBytes(bytes, this);
    }

    private static Object fromBytes(byte[] bytes, ColType colType) {
        if (bytes == NULL_BYTE_ARRAY) {
            return null;
        }
        switch (colType) {
            case STRING:
                return new String(bytes, UTF_8);
            case STRING_ARRAY: {
                // remember: UTC_16 encoding, separated by zero-chars
                List<String> strings = new ArrayList<>();
                int start = 0;
                while (start < bytes.length) {
                    for (int i = start + 1; i < bytes.length; i += 2) {
                        if (bytes[i - 1] == 0 && bytes[i] == 0) {
                            strings.add(new String(Arrays.copyOfRange(bytes, start, i - 1), UTF_16BE));
                            start = i + 1;
                            break;
                        }
                    }
                }
                return strings.toArray(new String[0]);
            }
            case LONG:
                return ByteBuffer.wrap(bytes).asLongBuffer().get();
            case LONG_ARRAY: {
                final Long[] longs = new Long[bytes.length / Long.BYTES];
                ByteBuffer bb = ByteBuffer.wrap(bytes);
                for (int i = 0; i < longs.length; i++) {
                    longs[i] = bb.getLong();
                }
                return longs;
            }
            case INT:
                return ByteBuffer.wrap(bytes).asIntBuffer().get();
            case INT_ARRAY: {
                final Integer[] ints = new Integer[bytes.length / Integer.BYTES];
                final ByteBuffer bb = ByteBuffer.wrap(bytes);
                for (int i = 0; i < ints.length; i++) {
                    ints[i] = bb.getInt();
                }
                return ints;
            }
            case SHORT:
                return ByteBuffer.wrap(bytes).asShortBuffer().get();
            case SHORT_ARRAY: {
                final Short[] shorts = new Short[bytes.length / Short.BYTES];
                final ByteBuffer bb = ByteBuffer.wrap(bytes);
                for (int i = 0; i < shorts.length; i++) {
                    shorts[i] = bb.getShort();
                }
                return shorts;
            }
            case DOUBLE:
                return ByteBuffer.wrap(bytes).asDoubleBuffer().get();
            case DOUBLE_ARRAY: {
                final Double[] doubles = new Double[bytes.length / Double.BYTES];
                final ByteBuffer bb = ByteBuffer.wrap(bytes);
                for (int i = 0; i < doubles.length; i++) {
                    doubles[i] = bb.getDouble();
                }
                return doubles;
            }
            case FLOAT:
                return ByteBuffer.wrap(bytes).asFloatBuffer().get();
            case FLOAT_ARRAY: {
                final Float[] floats = new Float[bytes.length / Float.BYTES];
                final ByteBuffer bb = ByteBuffer.wrap(bytes);
                for (int i = 0; i < floats.length; i++) {
                    floats[i] = bb.getFloat();
                }
                return floats;
            }
            case BOOL: {
                if (bytes == TRUE_BYTE_ARRAY) {
                    return Boolean.TRUE;
                } else if (bytes == FALSE_BYTE_ARRAY) {
                    return Boolean.FALSE;
                } else {
                    throw new IllegalArgumentException("Invalid bytes for boolean: " + Hex.encodeHexString(bytes));
                }
            }
            case JSON:
                return new JsonString(new String(bytes, UTF_8));
            case TIMESTAMP: {
                ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
                Timestamp result = new Timestamp(byteBuffer.getLong());
                result.setNanos(byteBuffer.getInt());
                return result;
            }
            case OFFSET_DATE_TIME: {
                ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
                final long secs = byteBuffer.getLong();
                final int nanos = byteBuffer.getInt();
                Instant result = Instant.ofEpochSecond(secs, nanos);
                return OffsetDateTime.ofInstant(result, ZoneOffset.UTC);
            }
            case ENTITY_TYPE:
                return EntityType.valueOf(new String(bytes, UTF_8));
            case ENVIRONMENT_TYPE:
                return EnvironmentType.valueOf(new String(bytes, UTF_8));
            case SEVERITY:
                return Severity.valueOf(new String(bytes, UTF_8));
            case ENTITY_STATE:
                return EntityState.valueOf(new String(bytes, UTF_8));
            case ACTION_TYPE:
                return ActionType.valueOf(new String(bytes, UTF_8));
            case FINAL_STATE:
                return TerminalState.valueOf(new String(bytes, UTF_8));
            case ACTION_CATEGORY:
                return ActionCategory.valueOf(new String(bytes, UTF_8));
            case ATTR_TYPE:
                return AttrType.valueOf(new String(bytes, UTF_8));
            default:
                throw new IllegalArgumentException("Unknown column type: " + colType.name());
        }
    }

    /**
     * Compute the XXHash value for a given column value.
     *
     * <p>This is not actually used, generally, for computing hashes, since that's done a record
     * at a time, with each column's contribution obtained using {@link #toBytes(Object)}. Where
     * the bytes contributed by one value depends on the hash values of parts of that value, this
     * method gets used. That's the case currently for LONG_SET.</p>
     *
     * @param value Value to be hashed
     * @return hash value computed for value
     */
    public long xxxHash(Object value) {
        return toXxxHash(value, this);
    }

    private static long toXxxHash(Object value, ColType colType) {
        if (colType == LONG_SET) {
            long xor = 0L;
            for (final long x : (long[])value) {
                xor ^= toXxxHash(x, LONG);
            }
            return toXxxHash(xor, LONG);
        } else {
            final StreamingXXHash64 hash = HashUtil.XX_HASH_FACTORY.newStreamingHash64(HashUtil.XX_HASH_SEED);
            final byte[] bytes = colType.toBytes(value);
            hash.update(bytes, 0, bytes.length);
            return hash.getValue();
        }
    }

    /**
     * Compute the csv representation of a column value, suitable for inclusion in a row sent
     * in CSV form to a postgres COPY operation.
     *
     * @param value value to be rendered
     * @return csv rendering of value
     */
    public String toCsv(Object value) {
        return toCsv(value, this);
    }

    private static String toCsv(Object value, ColType colType) {
        if (value == null) {
            return "";
        }
        switch (colType) {
            case STRING:
                return quoteString((String)value);
            case STRING_ARRAY:
                return arrayToCsv(Arrays.stream((String[])value));
            case LONG:
                return Long.toString((Long)value);
            case LONG_ARRAY:
            case LONG_SET:
                return arrayToCsv(Arrays.stream((Long[])value));
            case INT:
                return Integer.toString((Integer)value);
            case INT_ARRAY:
                return arrayToCsv(Arrays.stream((Integer[])value));
            case SHORT:
                return Short.toString((Short)value);
            case SHORT_ARRAY:
                // no ShortStream in Java
                return arrayToCsv(Arrays.stream((Short[])value));
            case DOUBLE:
                return Double.toString((Double)value);
            case DOUBLE_ARRAY:
                return arrayToCsv(Arrays.stream((Double[])value));
            case FLOAT:
                return Float.toString(((Float)value));
            case FLOAT_ARRAY:
                // no FloatStream in Java either
                return arrayToCsv(Arrays.stream((Float[])value));
            case BOOL:
                return Boolean.toString((Boolean)value);
            case JSON:
                return quoteString(value.toString());
            case TIMESTAMP:
                return value.toString();
            case OFFSET_DATE_TIME:
                return value.toString();
            case ENTITY_TYPE:
            case ENVIRONMENT_TYPE:
            case SEVERITY:
            case ENTITY_STATE:
            case ACTION_TYPE:
            case FINAL_STATE:
            case ACTION_STATE:
            case ACTION_MODE:
            case ACTION_CATEGORY:
            case METRIC_TYPE:
            case ATTR_TYPE:
            case COST_CATEGORY:
            case COST_SOURCE:
            case FILE_TYPE:
            case SAVINGS_TYPE:
                return ((EnumType)value).getLiteral();
            default:
                throw new IllegalArgumentException("Unknown column type: " + colType.name());
        }
    }

    /**
     * Represent an array column value in CSV format.
     *
     * <p>We use the Postgres array literal syntax and quote the literal.</p>
     *
     * @param values the values that belong to the array
     * @return CSV representation
     */
    public static String arrayToCsv(Stream<?> values) {
        return quoteString(toArrayLiteral(values));
    }

    /**
     * Quote a string value for CSV.
     *
     * <p>We double embedded quotes and quote the result.</p>
     *
     * @param o string value
     * @return CSV representation
     */
    static String quoteString(final String o) {
        return o != null ? "\"" + o.replaceAll("\"", "\"\"") + "\"" : null;
    }

    /**
     * Render an array value as a Postgres array literal.
     *
     * @param values array values
     * @return array literal representation
     */
    private static String toArrayLiteral(Stream<?> values) {
        String content = values
                .map(ColType::toArrayValue)
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
}
