package com.vmturbo.gson;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

/**
 * CallbackTriggeringTypeAdapterFactory is a Gson TypeAdapterFactory that adds support for event callbacks on the
 * object being serialized or deserialized. The events available are specified in the {@link GsonCallbacks} interface,
 * and include pre- and post-serialization and post-deserialization events. Objects that implement this interface will
 * have those callbacks triggered when this Type Adapter Factory is installed.
 * <p>
 * This implementation is based on usage of Gson's "delegate adapter", which allows you to hook custom functionality into
 * the Gson serialization / deserialization flow.
 * <p>
 * To use this factory, install it into the Gson factory list, e.g.:
 * </p>
 * {@code Gson gson = new GsonBuilder().registerTypeAdapterFactory(new CallbackTriggeringTypeAdapterFactory()).create(); }
 *
 * @see <a href="https://static.javadoc.io/com.google.code.gson/gson/2.8.0/com/google/gson/Gson.html#getDelegateAdapter-com.google.gson.TypeAdapterFactory-com.google.gson.reflect.TypeToken-">Gson Delegate Adapter</a>
 */
public class CallbackTriggeringTypeAdapterFactory implements TypeAdapterFactory {

    /**
     * Creates the custom type adapter that will trigger event callbacks on Objects that implement the {@link GsonCallbacks}
     * interface.
     *
     * @param gson the Gson instance
     * @param type the TypeToken representing the type being (de)serialized
     * @return the custom TypeAdapter instance
     */
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
        final TypeAdapter<T> delegate = gson.getDelegateAdapter(this, type);
        return new TypeAdapter<T>() {
            public void write(JsonWriter out, T value) throws IOException {
                boolean supportsCallbacks = value instanceof GsonCallbacks;
                if ( supportsCallbacks ) { // fire the pre-serialization callback
                    ((GsonCallbacks) value).preSerialize();
                }

                delegate.write(out, value);

                if ( supportsCallbacks ) { // fire the post-serialization callback
                    ((GsonCallbacks) value).postSerialize();
                }
            }
            public T read(JsonReader in) throws IOException {
                T retVal = delegate.read(in);

                // run the post-deserialization callback
                if ( retVal instanceof GsonCallbacks ) {
                    ((GsonCallbacks) retVal).postDeserialize();
                }
                return retVal;
            }
        };
    }
}
