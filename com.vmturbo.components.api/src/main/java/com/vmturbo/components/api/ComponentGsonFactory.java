package com.vmturbo.components.api;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.LongSerializationPolicy;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

/**
 * A utility class to create uniform {@link Gson} instances for components to
 * use to serialize and deserialize their responses.
 *
 * Components and component clients need a matching server-side {@link Gson} in order
 * to communicate properly.
 */
public class ComponentGsonFactory {

    private ComponentGsonFactory() {}

    /**
     * Create a {@link Gson} object which pretty-prints protobufs.
     *
     * @return The {@link Gson} object.
     */
    public static Gson createGson() {
        return createGsonFormatter(true);
    }

    /**
     * Create a {@link Gson} object which prints protobufs in one line.
     *
     * @return The {@link Gson} object.
     */
    public static Gson createGsonNoPrettyPrint() {
        return createGsonFormatter(false);
    }

    /**
     * Create a {@link Gson} formatter.
     * @param pretty when true - prints "pretty" protobufs (multiple lines, indented),
     *     when false - prints a protobuf in one line
     * @return The {@link Gson} object
     */
    private static Gson createGsonFormatter(boolean pretty) {
        GsonBuilder builder = new GsonBuilder()
                .registerTypeHierarchyAdapter(AbstractMessage.class, new ProtoAdapter())
                .registerTypeAdapterFactory(new GsonPostProcessEnabler())
                .registerTypeAdapter(BiMap.class, new BiMapDeserializer())
                .registerTypeHierarchyAdapter(Table.class, new TableAdapter())
                /* The default serialization policy can lead to rounding errors
                 * when using Javascript to access the UI (from, say, swagger-ui).
                 * This is because Javascript doesn't support 64-bit longs.
                 */
                .setLongSerializationPolicy(LongSerializationPolicy.STRING);
        if (pretty) {
                builder.setPrettyPrinting();
        }
        return builder.create();
    }

    /**
     * Enables post-processing of objects that implement {@link GsonPostProcessable}.
     */
    private static class GsonPostProcessEnabler implements TypeAdapterFactory {
        @Override
        public <T> TypeAdapter<T> create(final Gson gson, final TypeToken<T> typeToken) {
            final TypeAdapter<T> delegate = gson.getDelegateAdapter(this, typeToken);
            return new TypeAdapter<T>() {
                @Override
                public void write(final JsonWriter out, final T value) throws IOException {
                    delegate.write(out, value);
                }

                @Override
                public T read(final JsonReader in) throws IOException {
                    T obj = delegate.read(in);
                    if (obj instanceof GsonPostProcessable) {
                        ((GsonPostProcessable)obj).postDeserialize();
                    }
                    return obj;
                }
            };
        }
    }

    /**
     * Adapter for protobuf generated classes.
     *
     * Serializes generated messages using protobuf-java-format.
     *
     * Borrows from https://github.com/google/gson/blob/master/proto/src/main/java/com/google/gson/protobuf/ProtoTypeAdapter.java
     * for deserialization via reflection.
     *
     */
    private static class ProtoAdapter implements JsonSerializer<AbstractMessage>, JsonDeserializer<AbstractMessage> {

        private static final ConcurrentMap<String, Map<Class<?>, Method>> mapOfMapOfMethods = new ConcurrentHashMap<>();

        @Override
        public AbstractMessage deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            try {
                final String jsonStr = json.getAsJsonObject().toString();

                @SuppressWarnings("unchecked")
                Class<? extends AbstractMessage> protoClass = (Class<? extends AbstractMessage>)typeOfT;
                // Invoke the ProtoClass.newBuilder() method
                AbstractMessage.Builder<?> protoBuilder =
                        (AbstractMessage.Builder<?>)getCachedMethod(protoClass, "newBuilder").invoke(null);

                JsonFormat.parser().merge(jsonStr, protoBuilder);

                return AbstractMessage.class.cast(protoBuilder.build());
            } catch (Exception e) {
                throw new JsonParseException("Error while parsing proto.", e);
            }
        }

        @Override
        public JsonElement serialize(AbstractMessage src, Type typeOfSrc, JsonSerializationContext context) {
            try {
                return new JsonParser().parse(JsonFormat.printer().omittingInsignificantWhitespace().print(src));
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException("Invalid protobuf provided to the GSON serialization layer.", e);
            }
        }

        private static Method getCachedMethod(Class<?> clazz, String methodName,
                                              Class<?>... methodParamTypes) throws NoSuchMethodException {
            Map<Class<?>, Method> mapOfMethods = mapOfMapOfMethods.get(methodName);
            if (mapOfMethods == null) {
                mapOfMethods = new MapMaker().makeMap();
                Map<Class<?>, Method> previous =
                        mapOfMapOfMethods.putIfAbsent(methodName, mapOfMethods);
                mapOfMethods = previous == null ? mapOfMethods : previous;
            }

            Method method = mapOfMethods.get(clazz);
            if (method == null) {
                method = clazz.getMethod(methodName, methodParamTypes);
                mapOfMethods.putIfAbsent(clazz, method);
                // NB: it doesn't matter which method we return in the event of a race.
            }
            return method;
        }
    }

    /**
     * A deserializer for a {@link BiMap} that assumes the thing you're trying to deserialize
     * is actually a bimap (i.e. no duplicate keys or values).
     */
    @ThreadSafe
    private static class BiMapDeserializer implements JsonDeserializer<BiMap>  {
        @Override
        public BiMap deserialize(final JsonElement json, final Type typeOfT,
                                 final JsonDeserializationContext context)
                throws JsonParseException {
            final Map map = context.deserialize(json, Map.class);
            final BiMap biMap = HashBiMap.create();
            biMap.putAll(map);
            return biMap;
        }
    }

    /**
     * A deserializer for a {@link Table}. This deserializer only works for tables
     * where the row and columns are both strings.
     *
     * TODO (roman, Nov 24 2017): We can extend this to allow any primitive types for rows
     * and columns.
     */
    private static class TableAdapter implements JsonDeserializer<Table>, JsonSerializer<Table> {
        @Override
        public Table deserialize(final JsonElement json, final Type typeOfT,
                                 final JsonDeserializationContext context)
                throws JsonParseException {
            if (!(typeOfT instanceof ParameterizedType)) {
                throw new IllegalArgumentException("Attempt to serialize raw table.");
            }

            final ParameterizedType type = (ParameterizedType)typeOfT;
            final Type[] types = type.getActualTypeArguments();
            if (types.length != 3) {
                throw new IllegalStateException("Table must have exactly three generic types.");
            }

            final Type rowType = types[0];
            final Type columnType = types[1];
            final Type valueType = types[2];

            if (!String.class.equals(rowType) || !String.class.equals(columnType)) {
                throw new IllegalArgumentException("Table must have row and column type " +
                        "String in order to be Json-serializable. Instead we have: " +
                        rowType.getTypeName() + " and " + columnType.getTypeName());
            }
            final Table table = HashBasedTable.create();
            final JsonObject rows = json.getAsJsonObject();
            for (final Entry<String, JsonElement> row : rows.entrySet()) {
                final String rowName = row.getKey();
                final JsonObject columnsForRow = row.getValue().getAsJsonObject();
                for (final Entry<String, JsonElement> column : columnsForRow.entrySet()) {
                    final String columnName = column.getKey();
                    final Object value = context.deserialize(column.getValue(), valueType);
                    table.put(rowName, columnName, value);
                }
            }
            return table;
        }

        @Override
        public JsonElement serialize(final Table src, final Type typeOfSrc, final JsonSerializationContext context) {
            // Unfortunately, the type that comes in when attempting to serialize a Table
            // is not a PrimitiveType and does not have actual type information for the
            // generic arguments, so we have to do some casting.
            final JsonObject object = new JsonObject();
            src.rowKeySet().forEach(rowKey -> {
                final JsonObject rowObj = new JsonObject();
                if (!String.class.isInstance(rowKey)) {
                    throw new IllegalArgumentException("Row type of table must be String " +
                            "in order to be JSON-serializable. Instead it is: " +
                            rowKey.getClass().getSimpleName());
                }

                src.row(rowKey).forEach((colKey, val) -> {
                    if (!String.class.isInstance(colKey)) {
                        throw new IllegalArgumentException("Column type of table must be String " +
                            "in order to be JSON-serializable. Instead it is: " +
                            colKey.getClass().getSimpleName());
                    }
                    rowObj.add(String.class.cast(colKey), context.serialize(val));
                });
                object.add(String.class.cast(rowKey), rowObj);
            });
            return object;
        }
    }
}
