# SAGA EXAMPLE

## Architecture

//TODO DIAGRAM HERE

https://microservices.io/patterns/data/saga.html

- **Order Service** handles both the posting of a new order and the orchestration of the transaction.
- **Payment Service** receives orders and check of the customer has enough credit, *approving* the order on this end and making a "reservation" of the funds. 
- **Inventory Service** receives orders and check if there's sufficient product stock to fulfill the order, *approving* the order on this end and making a "reservation" of the product amount requested.

In both Payment and Inventory Services, if the "reservation" cannot be made (not enough funds or product stock), the order is *rejected* on each service side that could not make the reservation.

Order Service Orchestration check responses from the services and sends order **confirmation** if all parties approved the order, **rejects** the order if all parties rejected the reservation or emits a **compensation** if at least one (but not all) parties rejected the reservation, for the others to roll back the reservation.


### Considerations
This example uses a [database per service](https://microservices.io/patterns/data/database-per-service.html), implemented as an in-memory HashMap for Payments and Inventory service, this limits the scalability of the services thus it's only for demo purposes.

Ideally you also would want orders to be "re-keyed" by CustomerId before being consumed by the Payment Service, and "re-keyed" by productId before being consumed by the Inventory Service, this way orders for specific customerId or productId will be consumed by the same service instance, minimizing race conditions that could occur when mutating the payments or inventory repositories.

Consume offset for Orders are handled "manually" in the Payments and Inventory services and committed in a transaction along with the "approve/reject" response for new received orders.

---

## Code
The following parts of the code are worth checking out.

### Services "State Machine"
Inventory and Payment Service consume *Orders* from a kafka topic and act according to the orders state. 
- For NEW orders they will "reserve" the requested resource (funds, product, etc.) by moving the requested amount from available to reserved.
- To COMPENSATE if a reservation is "failed" from any party, the resource amount is moved back from reservation to available. 
- On CONFIRMED orders (when all parties have "approved" the resource reservation), the reserved amount is reduced.

```java
//See PaymentService or InventoryService
public class Service {
    //...
    public void processOrder(Order order) {
        //...
        switch (order.getState()) {
            case "NEW":
                processReservation(order);
                break;
            case "CONFIRMED":
                confirmReservation(order);
                break;
            case "COMPENSATE":
                if (!record.value().getSource().equals(SOURCE))
                    compensateReservation(order);
                break;
        }
        //..
    }
//...

    private void processReservation(Order order) {
        //...
        resource.setReserved(resource.getReserved() + order.requestedAmount);
        resource.setAvailable(resource.getAvailable() - order.requestedAmount);
        approveOrder(Order);
        //...
    }

    private void confirmReservation(Order order) {
        //...
        resource.setReserved(resource.getReserved() - order.requestedAmount);
        //...
    }

    private void compensateReservation(Order order) {
        //...
        resource.setReserved(resource.getReserved() - order.requestedAmount);
        resource.setAvailable(resource.getAvailable() + order.requestedAmount);
        //...
    }
}
```

### Orchestrator Stream
The stream joins the responses (same Order object model) from the services and send follow-through events if the order can be confirmed or services need to compensate/rollback

```java
// See OrderStreamTopologies.java in order-service module
//...
    final Serde<Order> orderSerde = buildOrderSerde();
    KStream<Integer, Order> paymentStream = streamBuilder.stream(paymentsResponseTopic, Consumed.with(Serdes.Integer(), orderSerde));
    KStream<Integer, Order> inventoryStream = streamBuilder.stream(inventoryResponseTopic, Consumed.with(Serdes.Integer(), orderSerde));

    paymentStream.join(inventoryStream,
                OrderStreamTopologies::ordersJoiner,
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(10)),
                StreamJoined.with(Serdes.Integer(), orderSerde, orderSerde))
        .peek((key, order) -> log.info(">>>>> JOIN RESULT {}:{}", key, order))
        .to(ordersTopic, Produced.with(Serdes.Integer(), orderSerde));
        //TODO NOT MATCHING AND (OUT-OF-ORDER)
//...
```

The above simply joins the responses of the service, the next snippet shows the logic behind sending a confirmation or a compensate event

### Stream joiner

```java
public class OrderStreamTopologies {
    //...
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
    //...
}
```



---

## Running the example

All the services and local Kafka cluster can be run with `docker compose` provided in the [sagas-env](/sagas-env) folder.
```shell
cd sagas-env
docker compose up -d
```
Note: First run takes a few more minutes since it needs to compile and build de service images

Use `--build` to rebuild the services if you do some code changes
```shell
docker compose up -d --build
```

### Checking the logs
For demo purposes you can have 3 windows split to show the logs of each service
```shell
docker compose logs -f order-service
docker compose logs -f inventory-service
docker compose logs -f payment-service
```
Or in a single window
```shell
docker compose logs -f inventory-service payment-service order-service
```

### Checking the topics
Events in the topics can be peeked with `kafka-console-consumer` or [kcat](https://github.com/edenhill/kcat), one command for each topic
```shell
kcat -C -b localhost:9092 -t orders-request -o beginning

kcat -C -b localhost:9092 -t orders-inventory -o beginning

kcat -C -b localhost:9092 -t orders-payment-o beginning
```

or all at once wrapped in a consumer group

```shell
kcat -C -b localhost:9092 -o beginning -G kcat-cg -t orders-request orders-payments orders-inventory
```

### Producing Orders
This is done via the *post* endpoint (/api/orders) of the **orders-service** (port 4545)
```shell
## POST: /api/orders
## Creates and posts random Order
curl -X POST http://localhost:4545/api/orders

## POST: /api/orders/:customerId/:productId/:quantity/:unit_price
# Creates an Order with specific content
curl -X POST http://localhost:4545/api/orders/my_customerId/my_productId/10/100
```

### Check Orders Store
The following *get* endpoint methods at (/api/orders) endpoint are available
```shell
## GET: /api/orders
## Returns a list of all the posted orders in their latest state
curl -X GET http://localhost:4545/api/orders

## GET: /api/orders/:orderId
## Returns the order with the specific Id in its latest state
curl -X GET http://localhost:4545/api/orders/12345
```

### Check Inventory and Payments
The other services also offer endpoint to list the products with theirs stock level and list the customers with their credit information

Payments-service endpoint (/api/customers) is available at port 4546
```shell
## GET: /api/customers
## List the customers with their credit levels
curl -X GET http://localhost:4546/api/customers
```

Inventory-service endpoint (/api/products) is available at port 4547
```shell
## GET: /api/product
## List the available products and their stock level
curl -X GET http://localhost:4547/api/products
```

---

## Known limitations
Besides the considerations mentioned at the start of the page...
- Not entirely "event-source", some events are not modelled, and we cannot replay easily all that happens on the payments and inventory services.
- Adding another service to the saga need to re-think the compensation event when two out of three service fail. 
- Handle late arriving messages.
