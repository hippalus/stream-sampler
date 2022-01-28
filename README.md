# stream-sampler

Distributed Random Reservoir Sampling Algorithm for Streaming Data

A simple in memory implementation of Reservoir Sampling with replacement and with only one pass through the input iteration whose
size is unpredictable. In the first phase, we generate weights for each element K times, so that each element can get selected
multiple times. This implementation refers to the algorithm described
in [Weighted random sampling with a reservoir.](https://www.researchgate.net/publication/222728123_Weighted_random_sampling_with_a_reservoir)

But when we use on a single instance and single thread or thread parallelism they have some problem in terms of CPU and memory
consumption. So they need to data partitioning and algorithm scalability. When the application is run with stdin, stdin is read
with BufferedReader and each data is published to Kafka topic.

A KafkaStream topology is start, which consumes the topic created with sample size information and runs the mentioned above
algorithm on the streaming data. This application is compatible for scalability.For example, if the character-kafka-subject has 10
partitions, this application can be scaled up to 10 instances. However, it needs some refactoring, such as separating the producer
and the consumer (KStream Topology) into different processes. By using redis priority queue and redis lock for random sample, we
ensured that the application can run stateless when scaled and the algorithm is consistent.

The KStream Application will subscribe to topic and continue to consume continuously, unless it is shut down or there is no
exception. The output of the application (sample)  log periodically , not in the end of KStream because of the end of the KStream
is hard to predict.

TODO : Perhaps Rest endpoint can be developed to retrieve that the sample at any time.

## System Design

![g drawio (1)](https://user-images.githubusercontent.com/17534654/151557171-bf7469a2-0609-42b7-95e1-c1d5976f40bf.png)



## Requirements and Run

* docker, docker-compose
* java  >= 17
* maven >= 3.8.1

```bash 
  cd projectdirectory

  mvn clean install -DskipTests

  docker-compose up -d

```

Make sure the Docker containers are successfully installed and up.

```bash 
  cat input.txt | ./stream-sampler -n SAMPLE_SIZE

  or

  dd if=/dev/urandom count=100 bs=10MB | base64 | ./stream-sampler -n SAMPLE_SIZE

  or

  ./stream-sampler -n SAMPLE_SIZE -g INPUT_SIZE

```

```bash 
usage: cat file.txt | ./stream-sampler.sh -n SAMPLE SIZE
Creates a random representative sample of length SAMPLE SIZE out of the input.
Input is either STDIN, or randomly generated within application.

 -g,--generate <INPUT_SIZE>   Size of random input
    --help
 -n,--size <SAMPLE_SIZE>      Sample size
  
```

## Unit and Integration Tests

Make sure docker containers are not running. Integration tests you may get the error Port Already in Used.

```bash 
docker kill $(docker ps -q)
```

```bash
mvn clean test
```

## Code Covarage
![Screenshot from 2022-01-28 13-22-05](https://user-images.githubusercontent.com/17534654/151549490-24c6560a-8d89-4cfb-ac1c-7bba71091462.png)

![Screenshot from 2022-01-28 13-23-47](https://user-images.githubusercontent.com/17534654/151549522-17522426-2cea-4ae3-88fd-10e02f7c9200.png)

## Used Technologies

Java 17, Spring Boot, KafkaStream, Redis, Docker

