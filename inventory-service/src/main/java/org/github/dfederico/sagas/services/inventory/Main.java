package org.github.dfederico.sagas.services.inventory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.github.dfederico.sagas.common.ConfigPropertiesHelper;
import org.github.dfederico.sagas.services.inventory.service.InventoryService;
import spark.Spark;

import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import static spark.Spark.*;

@Slf4j
public class Main {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public static void main(String[] args) {
        System.out.println("Loading Application Properties");
        Properties appProps = ConfigPropertiesHelper.loadApplicationProperties(Paths.get(args[0]));

        System.out.println("Initialize Inventory Service");
        initExceptionHandler((e) -> {
            System.err.printf("Exception Starting Server %s$n", e.getMessage());
            e.printStackTrace(System.err);
        });
        InventoryService inventoryService = new InventoryService(appProps);

        System.out.println("Starting Inventory Service");
        String appPort = appProps.getProperty("spark.port");
        port(Integer.parseInt(appPort));
        path("/api", () -> {
            before("/*", "application/json", (request, response) -> log.debug("Received API call {}", request.pathInfo()));
            get("/product-stocks", (request, response) -> {
                response.type("application/json");
                return inventoryService.getAvailableStock();
            }, model -> {
                if (model instanceof List) {
                    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(model);
                }
                return null;
            });
            //Order-ProductStock History?
        });
        Runtime.getRuntime().addShutdownHook(new Thread(Spark::stop));
        System.out.printf("Service started on port '%s'%n", appPort);

        //TODO PUT IN A THREAD AND ADD SHUTDOWN HOOK
        inventoryService.startPollingOrders();
    }
}