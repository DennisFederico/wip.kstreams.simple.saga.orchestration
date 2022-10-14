package org.github.dfederico.sagas.domain;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Builder
@Data
@Jacksonized
public class ProductStock {
    private String productId;
    private String productName;
    private int availableUnits;
    private int reservedUnits;
}
