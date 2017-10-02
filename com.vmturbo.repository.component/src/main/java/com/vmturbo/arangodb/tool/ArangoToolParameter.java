package com.vmturbo.arangodb.tool;

/**
 * Contains parameter strings for running arango tools.
 */
public enum ArangoToolParameter {
    SERVER_ENDPOINT("--server.endpoint"),
    SERVER_USERNAME("--server.username"),
    SERVER_PASSWORD("--server.password"),
    SERVER_DATABASE("--server.database"),
    OVERWRITE("--overwrite"),
    CREATE_DATABASE("--create-database"),
    SYSTEM_COLLECTIONS("--include-system-collections"),
    INPUT_DIRECTORY("--input-directory"),
    OUTPUT_DIRECTORY("--output-directory");


    private final String parameter;

    private ArangoToolParameter(final String parameter) {
        this.parameter = parameter;
    }

    public String getParameter() {
        return parameter;
    }
}
