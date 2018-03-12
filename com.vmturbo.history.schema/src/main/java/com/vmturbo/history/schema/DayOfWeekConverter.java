package com.vmturbo.history.schema;

import org.jooq.impl.EnumConverter;

import com.vmturbo.api.enums.DayOfWeek;

public class DayOfWeekConverter extends EnumConverter<String, DayOfWeek>{
	private static final long serialVersionUID = 1L;

	public DayOfWeekConverter() {
		super(String.class, DayOfWeek.class);
	}

}
