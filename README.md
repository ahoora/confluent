A playground for combining Kafka Streams on Confluent and Spring Boot.

# Running
Start a local, simplified, Confluent stack with
```
docker-compose up
```
and then run the app. Interact with the entity post endpoints and query endpoints, e.g.
```
POST http://localhost:8080/entity
Content-Type: application/json

{
  "name": "ok"
}


GET http://localhost:8080/entity
```
