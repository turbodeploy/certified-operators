package com.vmturbo.repository.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_EMPTY)
public class CommodityBoughtRepoDTO extends CommodityBaseRepoDTO {
}