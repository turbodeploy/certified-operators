package com.vmturbo.common.protobuf.group;

/**
 * Used when the GroupFetcher cannot fetch groups for some reason.
 */
public class GroupFetchingException extends Exception {

    public GroupFetchingException(String message) {
        super(message);
    }

    public GroupFetchingException(String message, Throwable cause){
        super(message, cause);
    }

    public GroupFetchingException(Throwable cause){
        super(cause);
    }

}
