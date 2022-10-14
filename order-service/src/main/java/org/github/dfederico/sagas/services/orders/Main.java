package org.github.dfederico.sagas.services.orders;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.github.dfederico.sagas.common.ConfigPropertiesHelper;
import org.github.dfederico.sagas.domain.Order;
import org.github.dfederico.sagas.domain.OrderGenerator;
import org.github.dfederico.sagas.services.orders.service.OrderService;
import org.github.dfederico.sagas.services.orders.service.OrderStreamTopologies;
import spark.Spark;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.github.dfederico.sagas.common.ConfigPropertiesHelper.KAFKA_CONFIG_PROPERTIES_PREFIX;
import static spark.Spark.*;

@Slf4j
public class Main {
    private static final ObjectMapper objectMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .build();

    static {
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Loading Application Properties");
        Properties appProps = ConfigPropertiesHelper.loadApplicationProperties(Paths.get(args[0]));

        System.out.println("Preparing Kafka Topics");
        initKafkaTopics(appProps);

        System.out.println("Starting Order Orchestrator (KStream)");
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        OrderStreamTopologies.createOrdersStreamOrchestrator(streamsBuilder, appProps);
        OrderStreamTopologies.createOrdersStore(streamsBuilder, appProps);
        final Properties kStreamProperties = OrderStreamTopologies.prepareKafkaStreamsProperties(appProps);
        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), kStreamProperties);
        //kafkaStreams.cleanUp();
        kafkaStreams.start();

        final ReadOnlyKeyValueStore<Integer, Order> ordersStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType("orders-store", QueryableStoreTypes.keyValueStore()));
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        System.out.println("Initialize Orders Service");
        initExceptionHandler((e) -> {
            System.err.printf("Exception Starting Server %s$n", e.getMessage());
            e.printStackTrace(System.err);
        });
        OrderService orderService = new OrderService(appProps);

        System.out.println("Starting Orders Service");
        String appPort = appProps.getProperty("spark.port");
        port(Integer.parseInt(appPort));
        path("/api", () -> {
            before("/*", (request, response) -> log.debug("Received API call {}", request.pathInfo()));

            post("/orders", (request, response) -> {
                Order order = OrderGenerator.generateRandomOrder();
                orderService.ProduceOrder(order);
                return order;
            });

            post("/orders/:customer/:product/:quantity", (request, response) -> OrderGenerator.generateRandomOrder(
                    request.params(":customer"), request.params(":product"), Integer.parseInt(request.params(":quantity"))
            ).toString());

            get("/orders/*", (request, response) -> {
                response.type("application/json");
                Integer orderId = Integer.valueOf(request.splat()[0]);
                //int orderId = Integer.parseInt(request.params(":id"));
                Order order = ordersStore.get(orderId);
                return order;
            }, model -> objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(model));

            get("/orders", (request, response) -> {
                response.type("application/json");
                try (KeyValueIterator<Integer, Order> ordersIterator = ordersStore.all()) {
                    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(ordersIterator, Spliterator.ORDERED), false)
                            .map(kv -> kv.value)
                            .collect(Collectors.toList());
                }
            }, model -> {
                if (model instanceof List) {
                    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(model);
                }
                return null;
            });

        });

        Runtime.getRuntime().addShutdownHook(new Thread(Spark::stop));
        System.out.printf("Service started on port '%s'%n", appPort);
    }

    private static void initKafkaTopics(Properties appProps) throws Exception {
        Properties connectionProps = ConfigPropertiesHelper.filterProperties(appProps, KAFKA_CONFIG_PROPERTIES_PREFIX);
        try (Admin kafkaAdmin = Admin.create(connectionProps)) {
            // WHICH TOPICS NEED CREATING?
            List<String> neededTopics = appProps.stringPropertyNames()
                    .stream()
                    .filter(property -> property.endsWith(".topic"))
                    .map(appProps::getProperty)
                    .collect(Collectors.toList());

            List<String> topicsToCreate = new ArrayList<>();

            DescribeTopicsResult describeResults = kafkaAdmin.describeTopics(neededTopics);
            describeResults.topicNameValues().forEach((topicName, future) -> {
                try {
                    future.get(60, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                        topicsToCreate.add(topicName);
                    } else {
                        throw new RuntimeException(e);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            if (!topicsToCreate.isEmpty()) {
                //FETCH ONE NODE_ID
                String brokerId = kafkaAdmin.describeCluster()
                        .nodes()
                        .get(15, TimeUnit.SECONDS).stream()
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("Cannot fetch cluster brokerId"))
                        .idString();

                //WHICH REPLICATION FACTOR?
                ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
                Map<ConfigResource, Config> configs = kafkaAdmin
                        .describeConfigs(Collections.singletonList(brokerResource))
                        .all()
                        .get(15, TimeUnit.SECONDS);
                //configs.get(brokerResource).get("min.insync.replicas");
                short defaultRFValue = Short.parseShort(configs.get(brokerResource).get("default.replication.factor").value());

                Map<String, String> compactProperty = Collections.singletonMap("cleanup.policy", "compact");
                //CONFIGURE TOPICS
                List<NewTopic> newTopics = topicsToCreate.stream()
                        .map(s -> {
                            NewTopic newTopic = new NewTopic(s, 1, defaultRFValue);
                            newTopic.configs(compactProperty);
                            return newTopic;
                        })
                        .collect(Collectors.toList());

                //CREATE TOPICS
                CreateTopicsResult topicsResults = kafkaAdmin.createTopics(newTopics);
                topicsResults.all().get(60, TimeUnit.SECONDS);
            }
        }
    }

}