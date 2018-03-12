package com.vmturbo.history.schema;

import org.jooq.impl.EnumConverter;

import com.vmturbo.api.enums.Period;

public class PeriodConverter extends EnumConverter<String, Period>{
	private static final long serialVersionUID = 1L;

	public PeriodConverter() {
		super(String.class, Period.class);
	}

}
