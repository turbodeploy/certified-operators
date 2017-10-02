package com.vmturbo.repository.graph;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import javaslang.collection.List;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ArangoDBCursorTestData<DATA> {
    public abstract List<DATA> data();
    public abstract Class<DATA> klass();

    public List<String> jsonData(final ObjectMapper objectMapper) throws JsonProcessingException {
        List<String> jsonStrings = List.empty();

        for (DATA d : data()) {
            final String s = objectMapper.writeValueAsString(d);
            jsonStrings = jsonStrings.append(s);
        }

        return jsonStrings;
    }
}
