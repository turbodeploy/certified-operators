package com.vmturbo.history.schema;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import org.jooq.impl.EnumConverter;


/**
 * Converting the {@link EnvironmentType} to a {@link Byte} to insert into the database.
 */
public class EnvironmentTypeConverter extends EnumConverter<Byte, EnvironmentType> {
    private static final long serialVersionUID = 123L;

    public EnvironmentTypeConverter() {
        super(Byte.class, EnvironmentType.class);
    }
}
