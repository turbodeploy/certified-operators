package com.vmturbo.topology.processor.historical;

import java.util.ArrayList;
import java.util.List;

import com.vmturbo.common.protobuf.topology.HistoricalInfo.HistoricalInfoDTO;
import com.vmturbo.common.protobuf.topology.HistoricalInfo.HistoricalInfoDTO.HistoricalSEInfoDTO;
import com.vmturbo.common.protobuf.topology.HistoricalInfo.HistoricalInfoDTO.HistoricalSEInfoDTO.HistoricalCommInfoDTO;

public class Conversions {

    public static HistoricalCommodityInfo convertFromDto(HistoricalCommInfoDTO source) {
        HistoricalCommodityInfo histCommInfo = new HistoricalCommodityInfo();
        histCommInfo.setCommodityTypeAndKey(source.getCommType());
        histCommInfo.setSourceId(source.getSourceId());
        histCommInfo.setMatched(source.getMatched());
        histCommInfo.setUpdated(source.getExisting());
        return histCommInfo;
    }

    public static HistoricalServiceEntityInfo convertFromDto(HistoricalSEInfoDTO source) {
        HistoricalServiceEntityInfo histSeInfo = new HistoricalServiceEntityInfo();
        histSeInfo.setSeOid(source.getSeOid());
        for (HistoricalCommInfoDTO histCommSoldDto : source.getHistCommSoldList()) {
            histSeInfo.addHistoricalCommoditySold(convertFromDto(histCommSoldDto));
        }
        for (HistoricalCommInfoDTO histCommBoughtDto : source.getHistCommBoughtList()) {
            histSeInfo.addHistoricalCommodityBought(convertFromDto(histCommBoughtDto));
        }
        return histSeInfo;
    }

    public static HistoricalInfo convertFromDto(HistoricalInfoDTO source) {
        HistoricalInfo histInfo = new HistoricalInfo();
        histInfo.clear();
        for (HistoricalSEInfoDTO histSeDto : source.getServiceEntityList()) {
            histInfo.put(histSeDto.getSeOid(), convertFromDto(histSeDto));
        }
        return histInfo;
    }

    public static HistoricalCommInfoDTO convertToDto(HistoricalCommodityInfo source) {
        return HistoricalCommInfoDTO.newBuilder()
                .setCommType(source.getCommodityTypeAndKey())
                .setSourceId(source.getSourceId())
                .setMatched(source.getMatched())
                .setExisting(source.getUpdated())
                .build();
    }

    public static HistoricalSEInfoDTO convertToDto(HistoricalServiceEntityInfo source) {
        List<HistoricalCommInfoDTO> histCommSoldDto = new ArrayList<>();
        for (HistoricalCommodityInfo histCommSold : source.getHistoricalCommoditySold()) {
            histCommSoldDto.add(convertToDto(histCommSold));
        }
        List<HistoricalCommInfoDTO> histCommBoughtDto = new ArrayList<>();
        for (HistoricalCommodityInfo histCommBought : source.getHistoricalCommodityBought()) {
            histCommBoughtDto.add(convertToDto(histCommBought));
        }
        return HistoricalSEInfoDTO.newBuilder().setSeOid(source.getSeOid())
                .addAllHistCommSold(histCommSoldDto)
                .addAllHistCommBought(histCommBoughtDto)
                .build();
    }

    public static HistoricalInfoDTO convertToDto(HistoricalInfo source) {
        List<HistoricalSEInfoDTO> histSeDto = new ArrayList<>();
        for (HistoricalServiceEntityInfo histSe : source.getOidToHistoricalSEInfo().values()) {
            histSeDto.add(convertToDto(histSe));
        }
        return HistoricalInfoDTO.newBuilder()
                .addAllServiceEntity(histSeDto)
                .build();
    }
}
