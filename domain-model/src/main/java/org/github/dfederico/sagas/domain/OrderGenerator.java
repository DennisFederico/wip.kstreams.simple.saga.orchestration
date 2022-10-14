package org.github.dfederico.sagas.domain;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.github.dfederico.sagas.domain.CustomerGenerator.CUSTOMER_IDS;
import static org.github.dfederico.sagas.domain.ProductStockGenerator.PRODUCT_IDS;

public class OrderGenerator {

    private static final Random RANDOM = new Random();

    private static final AtomicInteger orderIdSequence = new AtomicInteger(RANDOM.nextInt(1000));

    public static Order generateRandomOrder() {
        return Order.builder()
                .id(orderIdSequence.getAndAdd(RANDOM.nextInt(5)))
                .customerId(CUSTOMER_IDS[RANDOM.nextInt(CUSTOMER_IDS.length)])
                .productId(PRODUCT_IDS[RANDOM.nextInt(PRODUCT_IDS.length)])
                .units(RANDOM.nextInt(10)+1)
                .unitPrice((int) (RANDOM.nextDouble()*10000))
                .build();
    }

    public static Order generateRandomOrder(String customerId, String productId, int quantity, int unitPrice) {
        return Order.builder()
                .id(orderIdSequence.getAndAdd(RANDOM.nextInt(5)))
                .customerId(customerId)
                .productId(productId)
                .status("NEW")
                .units(quantity)
                .unitPrice(unitPrice)
                .build();
    }
}
