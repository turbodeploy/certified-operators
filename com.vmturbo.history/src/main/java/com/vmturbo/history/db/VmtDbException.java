package com.vmturbo.history.db;

import java.util.List;

import com.google.common.collect.Lists;

/**
 * Exception related to database operations.
 *
 * <p>This class has facilities to analyze the exception and provide enhanced capabilities.</p>
 */
@SuppressWarnings("serial")
public class VmtDbException extends Exception {//implements Iterable<VmtDbException>{
	private static final int CONTAINTER = 0;

	public static final int UNSUPPORTED_DIALECT = -1;

	public static final int CONN_POOL_STARTUP = 100;
	public static final int CONN_POOL_DOWN = 101;
	public static final int CONN_POOL_GET_CONN_ERR = 102;

	public static final int INSERT_ERR = 201;
	public static final int BATCH_INSERT_ERR = 202;
	public static final int ROLLBACK_ERR = 203;
	public static final int DAILY_ROLLUP_INSERT_ERR = 204;
	public static final int DAILY_ROLLUP_DELETE_ERR = 205;
	public static final int UPDATE_ERR = 206;
	public static final int DELETE_ERR = 207;
	public static final int READ_ERR = 208;

	public static final int SQL_EXEC_ERR = 301;

	public static final int REPORT_GEN_ERR = 401;
	public static final int REPORT_INSTANCE_NOT_FOUND = 402;
	public static final int REPORT_GEN_ERR_REPORT_TOOK_TOO_LONG = 403;

	public static final int REPAIR__REPAIR_FAILED = 1000;
	public static final int REPAIR__CANT_WRITE_TO_DISK = 1001;
	public static final int REPAIR__CANT_RESTART_DB = 1002;
	public static final int REPAIR__CANT_CONNECT = 1003;

	public static final int MIGRATION_ERR = 1100;
	public static final int PROPERTY_NULL = 1101;

	private List<VmtDbException> contained = Lists.newArrayList();

    public List<VmtDbException> getContained() {
        return contained;
    }

	public boolean container(){
		return code==CONTAINTER;
	}


	protected int code;
	protected String resource; // table/view/function-name/report-template-name

    /**
     * Create a new instance containing other instances.
     *
     * <p>If any of the contained instances is a container, its contained instances are added to
     * this container, but not recusively.</p>
     *
     * @param dbes {@link VmtDbException} instances to be contained
     */
    public VmtDbException(List<VmtDbException> dbes) {
        this(CONTAINTER);
        for (VmtDbException vmtDbException : dbes) {
            if (vmtDbException.code == CONTAINTER) {
                contained.addAll(vmtDbException.getContained());
            } else {
                contained.add(vmtDbException);
            }
        }
    }

	public VmtDbException(int code) {
		super(getErrMsg(code));
		this.code = code;
	}

	public VmtDbException(int code, String resource) {
		super(getErrMsg(code, resource));
		this.code = code;
		this.resource = resource;
	}

	public VmtDbException(int code, Throwable cause) {
		super(getErrMsg(code), cause);
		this.code = code;
	}

	public VmtDbException(int code, String resource, Throwable cause) {
		super(getErrMsg(code, resource), cause);
		this.code = code;
		this.resource = resource;
	}

	public int getCode() {
		return code;
	}

	public void setResource(String rsrc) {
		resource = rsrc;
	}

	public String getResource() {
		return resource;
	}

	private static String getErrMsg(int code){
		return getErrMsg(code, null);
	}

	private static String getErrMsg(int code, String resource){
		switch (code) {
		case CONTAINTER: return "Multiple errors";
		case UNSUPPORTED_DIALECT: return "Unsupported SQL dialect";
		case CONN_POOL_STARTUP: return "Error initializing connection pool";
		case CONN_POOL_DOWN: return "Connection pool is shutdown";
		case CONN_POOL_GET_CONN_ERR: return "Error retrieving db connection from pool";

		case BATCH_INSERT_ERR: return "Batch insert exception while insertion of data to DB";
		case INSERT_ERR: return "Error during insertion to the DB";
		case DAILY_ROLLUP_INSERT_ERR: return "Error rolling up table: "+resource;
		case DAILY_ROLLUP_DELETE_ERR: return "Error deleting old statistics from table: "+resource;
		case ROLLBACK_ERR: return "An error occurred while writing to table " + resource
				+ " and the automatic rollback failed.";
		case UPDATE_ERR: return "Error during updating to the DB";
		case DELETE_ERR: return "Error during deleting from " + resource;
		case READ_ERR: return "Error during reading from " + resource;

		case SQL_EXEC_ERR: return "Error executing sql query";

		case REPORT_GEN_ERR: return "Report generation error"
				+ (resource==null ? "" : ": "+resource);
		case REPORT_GEN_ERR_REPORT_TOOK_TOO_LONG:  return "Report took too long to generate: "
				+ (resource==null ? "" : ": "+resource);
		case REPORT_INSTANCE_NOT_FOUND: return "Cannot find scoped instance"
				+ (resource==null ? "" : ": "+resource);

		case REPAIR__REPAIR_FAILED: return "Database table "+resource+" is crashed and automatic repair failed. ";
		case REPAIR__CANT_WRITE_TO_DISK: return "Instance disk is full or write-locked, automatic table repair failed. ";
		case REPAIR__CANT_RESTART_DB: return "Database is not responding and automatic database restart failed. ";
		case REPAIR__CANT_CONNECT: return "Can't connect to the database.";
		case MIGRATION_ERR: return "Migration error";
		case PROPERTY_NULL: return "Property shouldn't be empty.";
		}
		return "";
	}
}
