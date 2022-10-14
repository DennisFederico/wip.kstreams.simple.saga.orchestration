package org.github.dfederico.sagas.services.inventory.service;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.github.dfederico.sagas.common.ConfigPropertiesHelper;
import org.github.dfederico.sagas.domain.Order;
import org.github.dfederico.sagas.domain.ProductStock;
import org.github.dfederico.sagas.domain.ProductStockGenerator;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.github.dfederico.sagas.common.ConfigPropertiesHelper.KAFKA_CONFIG_PROPERTIES_PREFIX;

@Slf4j
public class InventoryService {

    private static final String SOURCE = "INVENTORY";
    private static final Class<IntegerDeserializer> KAFKA_INTEGER_DESERIALIZER_CLASS = IntegerDeserializer.class;
    private static final Class<IntegerSerializer> KAFKA_INTEGER_SERIALIZER_CLASS = IntegerSerializer.class;
    private static final Class<KafkaJsonDeserializer> CFLT_KAFKA_JSON_DESERIALIZER_CLASS = KafkaJsonDeserializer.class;
    private static final Class<KafkaJsonSerializer> CFLT_KAFKA_JSON_SERIALIZER_CLASS = KafkaJsonSerializer.class;
    private final Map<String, ProductStock> productStockRepository;
    private final Consumer<Integer, Order> kafkaOrdersConsumer;
    private final Producer<Integer, Order> kafkaResponseProducer;
    private final String ordersTopicName;
    private final String responseTopicName;
    private final AtomicBoolean keepPolling = new AtomicBoolean(true);

    public InventoryService(Properties applicationProperties) {
        productStockRepository = ProductStockGenerator.createProductStockRepository();
        ordersTopicName = applicationProperties.getProperty("orders.request.topic");
        responseTopicName = applicationProperties.getProperty("inventory.response.topic");
        Properties kafkaProperties = ConfigPropertiesHelper.filterProperties(applicationProperties, KAFKA_CONFIG_PROPERTIES_PREFIX);
        kafkaOrdersConsumer = createKafkaOrdersConsumer(kafkaProperties);
        kafkaResponseProducer = createKafkaResponseProducer(kafkaProperties);
    }

    private Producer<Integer, Order> createKafkaResponseProducer(Properties connectionProperties) {
        Properties producerProperties = new Properties();
        producerProperties.putAll(connectionProperties);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_INTEGER_SERIALIZER_CLASS);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CFLT_KAFKA_JSON_SERIALIZER_CLASS);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "inventory-orders-tx");
        producerProperties.put(CommonClientConfigs.CLIENT_ID_CONFIG, "InventoryProducer-1");
        return new KafkaProducer<>(producerProperties);
    }

    static Consumer<Integer, Order> createKafkaOrdersConsumer(Properties connectionProperties) {
        Properties consumerProperties = new Properties();
        consumerProperties.putAll(connectionProperties);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KAFKA_INTEGER_DESERIALIZER_CLASS);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CFLT_KAFKA_JSON_DESERIALIZER_CLASS);
        consumerProperties.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Order.class.getName());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        //consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, connectionProperties.getProperty("group.id"));
        //consumerProperties.put(CommonClientConfigs.CLIENT_ID_CONFIG, "Inventory-");
        return new KafkaConsumer<>(consumerProperties);
    }

    public List<ProductStock> getAvailableStock() {
        return new ArrayList<>(productStockRepository.values());
    }

    public void processProductStockReservation(Order order) {
        log.info(">>> Process ProductStock Reservation");
        ProductStock productStock = productStockRepository.get(order.getProductId());

        if (productStock != null) {
            int totalRequest = order.getUnits();
            if (totalRequest > productStock.getAvailableUnits()) {
                order.rejectOrder(SOURCE, "Insufficient Available Product Units");
            } else {
                productStock.setReservedUnits(productStock.getReservedUnits() + totalRequest);
                productStock.setAvailableUnits(productStock.getAvailableUnits() - totalRequest);
                order.approveOrder(SOURCE);
            }
        } else {
            order.rejectOrder(SOURCE, "Unknown Product");
        }

        sendOrderResponse(order);
    }

    public void confirmProductStockReservation(Order order) {
        log.info(">>> Confirm ProductStock Reservation");
        productStockRepository.computeIfPresent(order.getProductId(), (key, productStock) -> {
            productStock.setReservedUnits(productStock.getReservedUnits() - order.getUnits());
            return productStock;
        });
    }

    public void compensateProductStockReservation(Order order) {
        log.info(">>> Compensate ProductStock Reservation [Origin:{} Cause{}]", order.getSource(), order.getCause());
        productStockRepository.computeIfPresent(order.getProductId(), (key, productStock) -> {
            int totalRequest = order.getUnits();
            productStock.setReservedUnits(productStock.getReservedUnits() - totalRequest);
            productStock.setAvailableUnits(productStock.getAvailableUnits() + totalRequest);
            return productStock;
        });
    }

    public void startPollingOrders() {
        kafkaResponseProducer.initTransactions();
        kafkaOrdersConsumer.subscribe(Collections.singletonList(ordersTopicName));

        while (keepPolling.get()) {
            final ConsumerRecords<Integer, Order> orderRecords = kafkaOrdersConsumer.poll(Duration.ofMillis(500));
            orderRecords.forEach(record -> {
                log.info("Consuming record from P:{} O:{} => K: '{}' V:{}",
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.value());

                kafkaResponseProducer.beginTransaction();
                String orderState = record.value().getStatus();
                switch (orderState) {
                    case "NEW":
                        processProductStockReservation(record.value());
                        break;
                    case "CONFIRMED":
                        confirmProductStockReservation(record.value());
                        break;
                    case "COMPENSATE":
                        if (!record.value().getSource().equals(SOURCE))
                            compensateProductStockReservation(record.value());
                        break;
                }

                //Handle Manual OffsetCommit
                Map<TopicPartition, OffsetAndMetadata> offsetData = Collections.singletonMap(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1));
                kafkaResponseProducer.sendOffsetsToTransaction(offsetData, kafkaOrdersConsumer.groupMetadata());
                kafkaResponseProducer.commitTransaction();
            });
        }
    }

    public void stopPolling() {
        keepPolling.set(false);
    }

    private void sendOrderResponse(Order order) {
        ProducerRecord<Integer, Order> record = new ProducerRecord<>(responseTopicName, order.getId(), order);

        kafkaResponseProducer.send(record, (recordMetadata, exception) -> {
            if (exception == null) {
                log.info("Produced record to P:{} O:{} - K:{}, V:{} @timestamp {}",
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        record.key(),
                        record.value(),
                        recordMetadata.timestamp());
            } else {
                kafkaResponseProducer.abortTransaction();
                log.error("An error occurred while producing an event '{}'", exception.getMessage());
                exception.printStackTrace(System.err);
            }
        });
    }
}
