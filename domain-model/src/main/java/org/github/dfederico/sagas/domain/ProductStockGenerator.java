package org.github.dfederico.sagas.domain;

import net.datafaker.Faker;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ProductStockGenerator {
    public static final String[] PRODUCT_IDS = {"ABC123", "XYZ001", "P12345", "P00001", "ABC001", "XYZABC"};

    private static final Random RANDOM = new Random();
    private static final Faker FAKER = new Faker();

    public static Map<String, ProductStock> createProductStockRepository() {
        return Arrays.stream(PRODUCT_IDS)
                .map(productId -> ProductStock.builder()
                        .productId(productId)
                        .productName(FAKER.beer().name())
                        .availableUnits(RANDOM.nextInt(90) + 10)
                        .reservedUnits(0)
                        .build()).collect(Collectors.toMap(ProductStock::getProductId, Function.identity()));
    }
}
