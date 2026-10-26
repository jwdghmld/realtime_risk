package com.ecommerce.realtime.model;

import java.io.Serializable;
import java.math.BigDecimal;

public class RiskEvent implements Serializable {
    public String eventId;
    public String eventType;
    public long eventTimeMs;
    public String businessDate;
    public long orderId;
    public Long userId;
    public Long shopId;
    public BigDecimal orderAmount;
    public long createTimeMs;
    public Long orderDetailId;
    public Long skuId;
    public Integer skuNum;
    public BigDecimal originalAmount;
    public BigDecimal finalAmount;
    public Long paymentId;
    public String paymentStatus;
    public BigDecimal paymentAmount;
    public long paymentTimeMs;

    public RiskEvent() {
    }
}
