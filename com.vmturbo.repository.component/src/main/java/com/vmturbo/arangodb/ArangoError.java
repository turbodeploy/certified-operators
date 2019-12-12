package com.vmturbo.arangodb;

/**
 * Some convenience constants representing various ArangoDB error codes. This is a subset of what's
 * available at <a href="https://www.arangodb.com/docs/stable/appendix-error-codes.html">the official ArangoDB Documentation</a>
 *
 * <p>If you want additional error code constants, take them from that document.</p>
 */
public final class ArangoError {
    // these are only 12xx errors -- you need to look at the docs to see the others.
    public static final int ERROR_ARANGO_CONFLICT = 1200;
    public static final int ERROR_ARANGO_DATADIR_INVALID = 1201;
    public static final int ERROR_ARANGO_DOCUMENT_NOT_FOUND = 1202;
    public static final int ERROR_ARANGO_DATA_SOURCE_NOT_FOUND = 1203;
    public static final int ERROR_ARANGO_COLLECTION_PARAMETER_MISSING = 1204;
    public static final int ERROR_ARANGO_DOCUMENT_HANDLE_BAD = 1205;
    public static final int ERROR_ARANGO_MAXIMAL_SIZE_TOO_SMALL = 1206;
    public static final int ERROR_ARANGO_DUPLICATE_NAME = 1207;
    public static final int ERROR_ARANGO_ILLEGAL_NAME = 1208;
    public static final int ERROR_ARANGO_NO_INDEX = 1209;
    public static final int ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED = 1210;
    public static final int ERROR_ARANGO_INDEX_NOT_FOUND = 1212;
    public static final int ERROR_ARANGO_CROSS_COLLECTION_REQUEST = 1213;
    public static final int ERROR_ARANGO_INDEX_HANDLE_BAD = 1214;
    public static final int ERROR_ARANGO_DOCUMENT_TOO_LARGE = 1216;
    public static final int ERROR_ARANGO_COLLECTION_NOT_UNLOADED = 1217;
    public static final int ERROR_ARANGO_COLLECTION_TYPE_INVALID = 1218;
    public static final int ERROR_ARANGO_ATTRIBUTE_PARSER_FAILED = 1220;
    public static final int ERROR_ARANGO_DOCUMENT_KEY_BAD = 1221;
    public static final int ERROR_ARANGO_DOCUMENT_KEY_UNEXPECTED = 1222;
    public static final int ERROR_ARANGO_DATADIR_NOT_WRITABLE = 1224;
    public static final int ERROR_ARANGO_OUT_OF_KEYS = 1225;
    public static final int ERROR_ARANGO_DOCUMENT_KEY_MISSING = 1226;
    public static final int ERROR_ARANGO_DOCUMENT_TYPE_INVALID = 1227;
    public static final int ERROR_ARANGO_DATABASE_NOT_FOUND = 1228;
    public static final int ERROR_ARANGO_DATABASE_NAME_INVALID = 1229;
    public static final int ERROR_ARANGO_USE_SYSTEM_DATABASE = 1230;
    public static final int ERROR_ARANGO_INVALID_KEY_GENERATOR = 1232;
    public static final int ERROR_ARANGO_INVALID_EDGE_ATTRIBUTE = 1233;
    public static final int ERROR_ARANGO_INDEX_CREATION_FAILED = 1235;
    public static final int ERROR_ARANGO_WRITE_THROTTLE_TIMEOUT = 1236;
    public static final int ERROR_ARANGO_COLLECTION_TYPE_MISMATCH = 1237;
    public static final int ERROR_ARANGO_COLLECTION_NOT_LOADED = 1238;
    public static final int ERROR_ARANGO_DOCUMENT_REV_BAD = 1239;
    public static final int ERROR_ARANGO_INCOMPLETE_READ = 1240;

    private ArangoError() {}
}
