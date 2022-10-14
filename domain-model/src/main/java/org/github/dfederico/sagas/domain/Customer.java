package org.github.dfederico.sagas.domain;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Builder
@Data
@Jacksonized
public class Customer {
    private String customerId;
    private String name;
    private int availableCredit;
    private int reservedCredit;
}
