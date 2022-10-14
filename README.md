# SAGAS EXAMPLE

## Architecture

### Considerations
Ideally you want orders topic to be "re-keyed" by CustomerId before being consumed by the payment service, so all orders for a given customer are ordered and consumed by the same instance minimizing any race conditions when mutating to the customer repository.

Same for the Stock, you would want to re-key by product to minimize race conditions mutating the Stock repository

## Orders Service

## Payment Service

It receives orders and confirm if user has enough credit