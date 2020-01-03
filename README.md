# Kafka Connect Elasticsearch Connector

Kafka-connect-elasticsearch is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for copying data from kafka to Elasticsearch version 7

# Development

This connector is meant to be easy to edit and build. To build a development version
 you'll need to checkout the project, import it through gradle, then build a shadow jar with:
 
 ```console
 ./gradlew shadowJar
 ```

# Deployment

To deploy the connector and use it, copy the "-all" jar and paste it 
into the kafka connect plugin folder, then restart kafka connect service.

If you just want to use the connector without any further development, a "-all" jar is available
in the root of the project.

# License

This project is licensed under the [GNU Lesser General Public License](COPYING.LESSER).