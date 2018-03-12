package com.vmturbo.history.schema;

import org.jooq.impl.EnumConverter;

public class RelationTypeConverter extends EnumConverter<Byte, RelationType>{
	private static final long serialVersionUID = 1L;

	public RelationTypeConverter() {
		super(Byte.class, RelationType.class);
	}

}
