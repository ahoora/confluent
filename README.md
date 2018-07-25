A playground for combining Kafka Streams on Confluent and Spring Boot.

# Running
Start a local, simplified, Confluent stack with
```
docker-compose up
```
and then run the app. Interact with the type generator endpoints and query endpoints, e.g.
```
POST http://localhost:8080/avro/gen?count=3
GET http://localhost:8080/avro/table 
```
