package org.github.dfederico.sagas.services.orders.service;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.github.dfederico.sagas.domain.Order;
import org.github.dfederico.sagas.common.ConfigPropertiesHelper;

import java.util.Properties;

import static org.github.dfederico.sagas.common.ConfigPropertiesHelper.KAFKA_CONFIG_PROPERTIES_PREFIX;

@Slf4j
public class OrderService {

    private static final Class<IntegerSerializer> KAFKA_INTEGER_SERIALIZER = org.apache.kafka.common.serialization.IntegerSerializer.class;
    //private static final String CFLT_JSON_SCHEMA_SERIALIZER = "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer";
    private static final String CFLT_KAFKA_JSON_SERIALIZER = "io.confluent.kafka.serializers.KafkaJsonSerializer";

    //TODO RESOURCE DISPOSAL (CLOSE PRODUCER)
    private final Producer<Integer, Order> kafkaOrdersProducer;
    private final String ordersTopicName;

    public OrderService(Properties applicationProperties) {
        ordersTopicName = applicationProperties.getProperty("orders.request.topic");
        kafkaOrdersProducer = createKafkaOrdersProducer(ConfigPropertiesHelper.filterProperties(applicationProperties, KAFKA_CONFIG_PROPERTIES_PREFIX));
    }

    static Producer<Integer, Order> createKafkaOrdersProducer(Properties connectionProperties) {
        Properties producerProperties = new Properties();
        producerProperties.putAll(connectionProperties);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_INTEGER_SERIALIZER);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CFLT_KAFKA_JSON_SERIALIZER);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put(CommonClientConfigs.CLIENT_ID_CONFIG, "OrdersProducer-1");
        return new KafkaProducer<>(producerProperties);
    }

    public void ProduceOrder(Order order) {
        ProducerRecord<Integer, Order> record = new ProducerRecord<>(ordersTopicName, order.getId(), order);

        kafkaOrdersProducer.send(record, (recordMetadata, exception) -> {
            if (exception == null) {
                log.info("Produced record to P:{} O:{} - K:{}, V:{} @timestamp {}",
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        record.key(),
                        record.value(),
                        recordMetadata.timestamp());
            } else {
                log.error("An error occurred while producing an event '{}'", exception.getMessage());
                exception.printStackTrace(System.err);
            }
        });
    }
}
