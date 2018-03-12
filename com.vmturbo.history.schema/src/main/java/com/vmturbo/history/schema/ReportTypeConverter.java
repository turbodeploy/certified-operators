package com.vmturbo.history.schema;

import org.jooq.impl.EnumConverter;

import com.vmturbo.api.enums.ReportType;

public class ReportTypeConverter extends EnumConverter<Byte, ReportType>{
	private static final long serialVersionUID = 1L;

	public ReportTypeConverter() {
		super(Byte.class, ReportType.class);
	}
}
