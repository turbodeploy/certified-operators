package com.vmturbo.gson;

/**
 * Interface for declaring the use of gson callback events provided by the CallbackTriggeringTypeAdapterFactor.
 * The following callbacks are available:
 * <ul>
 *     <li>preSerialize -- called before the object is serialized</li>
 *     <li>postSerialize -- called after serialization</li>
 *     <li>postDeserialize -- called after the object has been deserialized. We are using this to repopulate transient data.</li>
 * </ul>
 * There is a default implementation for each of these that does nothing. So you only need to implement the events
 * you are interested in.
 */
public interface GsonCallbacks {
    /**
     * preSerialize will be called on an object just before it is serialized.
     * <p>
     * There is a default no-op implementation, so only override this if you want to use it.
     */
    default public void preSerialize() {}

    /**
     * postSerialize will be called on an object after it has been serialized. An example of when you might want to do
     * is if your object is actively used and you want to support some kind of pause / resume functionality.
     * <p>
     * There is a default no-op implementation, so only override this if you want to use it.
     */
    default public void postSerialize() {}

    /**
     * postDeserialize will be called on an object that has just completed deserialization. An example of when you
     * might want to hook into the deserialization event is if you need to recreate transient data, perform internal
     * validation, etc.
     * <p>
     * There is a default no-op implementation of this event, so only override this when you want to use it.
     */
    default public void postDeserialize() {}
}
