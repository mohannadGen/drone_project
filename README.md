# drone_project
## For using Kafka
Assuming that you are in the Kafka repository:
- First, start the Zookeeper server
~~~bash
$ bin/zookeeper-server-start.sh config/zookeeper.properties
~~~
- Then, in another shell, start a Kafka server
~~~bash
$ bin/kafka-server-start.sh config/server.properties
~~~
You can now create topics, either with shell commands or with a Java program.
