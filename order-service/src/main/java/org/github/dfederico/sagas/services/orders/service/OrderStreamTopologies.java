package org.github.dfederico.sagas.services.orders.service;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.github.dfederico.sagas.common.ConfigPropertiesHelper;
import org.github.dfederico.sagas.domain.Order;
import org.javatuples.Pair;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.Stream;

import static org.github.dfederico.sagas.common.ConfigPropertiesHelper.KAFKA_CONFIG_PROPERTIES_PREFIX;

@Slf4j
public class OrderStreamTopologies {

    private static Serde<Order> buildOrderSerde() {
        final KafkaJsonSerializer<Order> jsonSerializer = new KafkaJsonSerializer<>();
        jsonSerializer.configure(Collections.singletonMap(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Order.class.getName()), false);
        final KafkaJsonDeserializer<Order> jsonDeserializer = new KafkaJsonDeserializer<>();
        jsonDeserializer.configure(Collections.singletonMap(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Order.class.getName()), false);
        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }

    private static Order ordersJoiner(Order left, Order right) {
        Order.OrderBuilder orderBuilder = Order.builder()
                .id(left.getId())
                .customerId(left.getCustomerId())
                .productId(left.getProductId())
                .units(left.getUnits())
                .unitPrice(left.getUnitPrice());

        if (left.getStatus().equals(Order.OrderState.APPROVED.name()) &&
                right.getStatus().equals(Order.OrderState.APPROVED.name())) {
            orderBuilder.status(Order.OrderState.CONFIRMED.name());
        } else if (Stream.of(left, right).allMatch(o -> o.getStatus().equals(Order.OrderState.REJECTED.name()))) {
            orderBuilder.status(Order.OrderState.REJECTED.name());
            orderBuilder.cause("All parts Rejected");
        } else if (Stream.of(left, right).anyMatch(o -> o.getStatus().equals(Order.OrderState.REJECTED.name()))) {
            orderBuilder.status(Order.OrderState.COMPENSATE.name());
            Pair<String, String> pair = Stream.of(left, right)
                    .filter(o -> o.getStatus().equals(Order.OrderState.REJECTED.name()))
                    .findFirst()
                    .map(order -> Pair.with(order.getSource(), order.getCause()))
                    .orElse(Pair.with("Orchestrator", "Unknown State"));
            orderBuilder.source(pair.getValue0());
            orderBuilder.cause(pair.getValue1());
        }
        return orderBuilder.build();
    }

    public static Properties prepareKafkaStreamsProperties(Properties applicationProperties) {
        Properties kStreamProperties = new Properties();
        kStreamProperties.putAll(ConfigPropertiesHelper.filterProperties(applicationProperties, KAFKA_CONFIG_PROPERTIES_PREFIX));
        kStreamProperties.putAll(applicationProperties);
        kStreamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kStreamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return kStreamProperties;
    }

    public static StreamsBuilder createOrdersStreamOrchestrator(StreamsBuilder streamBuilder, Properties properties) {
        String paymentsResponseTopic = properties.getProperty("payments.response.topic");
        String inventoryResponseTopic = properties.getProperty("inventory.response.topic");
        String ordersTopic = properties.getProperty("orders.request.topic");

        final Serde<Order> orderSerde = buildOrderSerde();
        //final StreamsBuilder streamBuilder = new StreamsBuilder();
        KStream<Integer, Order> paymentStream = streamBuilder.stream(paymentsResponseTopic, Consumed.with(Serdes.Integer(), orderSerde));
        KStream<Integer, Order> inventoryStream = streamBuilder.stream(inventoryResponseTopic, Consumed.with(Serdes.Integer(), orderSerde));

        paymentStream.join(inventoryStream,
                        OrderStreamTopologies::ordersJoiner,
                        JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(10)),
                        StreamJoined.with(Serdes.Integer(), orderSerde, orderSerde))
                .peek((key, order) -> log.info(">>>>> JOIN RESULT {}:{}", key, order))
                .to(ordersTopic, Produced.with(Serdes.Integer(), orderSerde));
        // return paymentStream;
        return streamBuilder;
    }

    public static StreamsBuilder createOrdersStore(StreamsBuilder streamsBuilder, Properties properties) {

        String ordersTopic = properties.getProperty("orders.request.topic");
        KeyValueBytesStoreSupplier store = Stores.persistentKeyValueStore("orders-store");
        Serde<Order> orderSerde = buildOrderSerde();

        streamsBuilder.stream(ordersTopic, Consumed.with(Serdes.Integer(), orderSerde))
                .toTable(Materialized.<Integer, Order>as(store)
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(orderSerde));
        return streamsBuilder;
    }
}
