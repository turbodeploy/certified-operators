package com.vmturbo.sql.utils.jooq;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.Record;

/**
 * Class to create a proxy key from columns in a record, potentially including nullable columns
 * that cannot appear in unique or primary key declarations due to their nullability.
 *
 * <p>All generated key values are formed by concatenating string values for constituent fields
 * from a given record, with "-" for null values, and then rendered as an md5 hash.</p>
 */
public class ProxyKey {
    private ProxyKey() {}

    /**
     * Create a key value for the given fields' values from the given record.
     *
     * <p>This method returns the key as a string value, prior to md5 hash calculation.</p>
     *
     * @param record record from which values will be taken
     * @param fields fields that constitute the proxy key
     * @return proxy key value
     */
    public static String getKey(Record record, Field<?>... fields) {
        return Arrays.stream(fields)
                .map(f -> fieldValue(f, record))
                .map(v -> v != null ? v.toString() : "-")
                .collect(Collectors.joining(""));
    }

    /**
     * Get a proxy key value as an md5 hash in byte[] form.
     *
     * @param record record from which to obtain field values
     * @param fields fields that constitute the proxy key
     * @return proxy key value
     */
    public static byte[] getKeyAsBytes(Record record, Field<?>... fields) {
        return DigestUtils.md5(getKey(record, fields));
    }

    /**
     * Get a proxy key value as an md5 hash in hex-string form.
     *
     * @param record record from which to obtain field values
     * @param fields field sthat constitute the proxy key
     * @return proxy key value
     */
    public static String getKeyAsHex(Record record, Field<?>... fields) {
        return Hex.encodeHexString(getKeyAsBytes(record, fields));
    }

    private static <T> T fieldValue(Field<T> f, Record record) {
        return f instanceof Param ? ((Param<T>)f).getValue()
                                  : record.getValue(f);
    }
}
