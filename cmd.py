# To create a topic in kafka -- For that first i enter into kafka through docker by docker exec -it kafka bash
# and then move to bin directory ----   cd /opt/kafka/bin
# then run the below command to create topic
# ./kafka-topics.sh --create --topic <topic_name> --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1


# to list all the topics in kafka
# ./kafka-topics.sh --list --bootstrap-server kafka:9092

# to describe the topic
# ./kafka-topics.sh --describe --topic <topic_name> --bootstrap-server kafka:9092