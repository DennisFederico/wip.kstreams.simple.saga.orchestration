package org.github.dfederico.sagas.domain;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;
import lombok.extern.jackson.Jacksonized;

import java.time.Instant;


@Builder
@Jacksonized
//@Getter
@Data
@Setter(AccessLevel.PROTECTED)
public class Order {
    private Integer id;
    @JsonFormat (shape = JsonFormat.Shape.STRING)
    @Builder.Default
    private Instant createTs = Instant.now();
    private String customerId;
    private String productId;
    private int units;
    private int unitPrice;
    @Builder.Default
    private String status = OrderState.NEW.name();
    private String source;
    private String cause;

    public void rejectOrder(String source, String cause) {
        status = OrderState.REJECTED.name();
        this.source = source;
        this.cause = cause;
    }

    public void approveOrder(String source) {
        status = OrderState.APPROVED.name();
        this.source = source;
    }

    public static enum OrderState {
        NEW, APPROVED, REJECTED, CONFIRMED, COMPENSATE
    }
}