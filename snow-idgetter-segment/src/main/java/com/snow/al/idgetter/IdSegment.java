package com.snow.al.idgetter;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;

@ToString
public class IdSegment {

    private Long minId;
    @Getter
    @Setter
    private Long maxId;
    @Getter
    @Setter
    private Long step;

    private Long middleId;

    public Long getMiddleId() {
        if (middleId == null) {
            middleId = maxId - BigDecimal.valueOf(step).divide(new BigDecimal("2")).longValue();
        }
        return middleId;
    }

    public Long getMinId() {
        if (minId == null) {
            if (maxId != null && step != null) {
                minId = maxId - step;
            } else {
                throw new IllegalArgumentException("maxId or step is null");
            }
        }
        return minId;
    }
}