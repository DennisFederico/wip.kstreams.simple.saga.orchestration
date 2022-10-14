package org.github.dfederico.sagas.domain;

import net.datafaker.Faker;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CustomerGenerator {
    public static final String[] CUSTOMER_IDS = {"CUST01", "CUST-ABC", "ABC123", "CFLT-XYZ", "MY_CUST"};

    private static final Random RANDOM = new Random();
    private static final Faker FAKER = new Faker();

    public static Map<String, Customer> createCustomerRepository() {
        return Arrays.stream(CUSTOMER_IDS)
                .map(customerId -> Customer.builder()
                        .customerId(customerId)
                        .name(FAKER.company().name())
                        .availableCredit(10_000 + RANDOM.nextInt(10000))
                        .reservedCredit(0)
                        .build()).collect(Collectors.toMap(Customer::getCustomerId, Function.identity()));
    }
}
