/*
    Copyright 2019 Cultura (SOCULTUR).

    This file is part of kafka-connect-elasticsearch.

    kafka-connect-elasticsearch is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    kafka-connect-elasticsearch is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with kafka-connect-elasticsearch.  If not, see <https://www.gnu.org/licenses/>.

    contact: team.api.support@cultura.fr
 */

package plugin.kafka.sink.elastic.configs;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;

public class ElasticsearchSinkConnectorConfig extends AbstractConfig {
  private static final String SSL_GROUP = "Security";

  public static final String CONNECTION_URL_CONFIG = "connection.url";
  private static final String CONNECTION_URL_DOC =
      "The comma-separated list of one or more Elasticsearch URLs, such as ``http://eshost1:9200,"
      + "http://eshost2:9200`` or ``https://eshost3:9200``. If https is used, security section has"
      + " to be filled properly to configure ssl";

  public static final String CONNECTION_USERNAME_CONFIG = "connection.username";
  private static final String CONNECTION_USERNAME_DOC =
      "The username used to authenticate with Elasticsearch. "
      + "The default is the null, and authentication will only be performed if "
      + " both the username and password are non-null.";
  public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
  private static final String CONNECTION_PASSWORD_DOC =
      "The password used to authenticate with Elasticsearch. "
      + "The default is the null, and authentication will only be performed if "
      + " both the username and password are non-null.";
  public static final String BATCH_SIZE_CONFIG = "batch.size";
  private static final String BATCH_SIZE_DOC =
      "The number of records to process as a batch when writing to Elasticsearch.";
  public static final String MAX_IN_FLIGHT_REQUESTS_CONFIG = "max.in.flight.requests";
  private static final String MAX_IN_FLIGHT_REQUESTS_DOC =
      "The maximum number of indexing requests that can be in-flight to Elasticsearch before "
      + "blocking further requests.";
  public static final String MAX_BUFFERED_RECORDS_CONFIG = "max.buffered.records";
  private static final String MAX_BUFFERED_RECORDS_DOC =
      "The maximum number of records each task will buffer before blocking acceptance of more "
      + "records. This config can be used to limit the memory usage for each task.";
  public static final String BUFFERED_LINGER_MS_CONFIG = "buffered.records.linger.ms";
  private static final String BUFFERED_LINGER_MS_DOC =
      "The time the task will wait before trying to add record to buffer. The task can't wait more than "
          + "flush timeout";
  public static final String LINGER_MS_CONFIG = "linger.ms";
  private static final String LINGER_MS_DOC =
      "Linger time in milliseconds for batching.\n"
      + "Records that arrive in between request transmissions are batched into a single bulk "
      + "indexing request, based on the ``" + BATCH_SIZE_CONFIG + "`` configuration. Normally "
      + "this only occurs under load when records arrive faster than they can be sent out. "
      + "However it may be desirable to reduce the number of requests even under light load and "
      + "benefit from bulk indexing. This setting helps accomplish that - when a pending batch is"
      + " not full, rather than immediately sending it out the task will wait up to the given "
      + "delay to allow other records to be added so that they can be batched into a single "
      + "request.";
  public static final String FLUSH_TIMEOUT_MS_CONFIG = "flush.timeout.ms";
  private static final String FLUSH_TIMEOUT_MS_DOC =
      "The timeout in milliseconds to use for periodic flushing, and when waiting for buffer "
      + "space to be made available by completed requests as records are added. If this timeout "
      + "is exceeded the task will fail.";
  public static final String MAX_RETRIES_CONFIG = "max.retries";
  private static final String MAX_RETRIES_DOC =
      "The maximum number of retries that are allowed for failed indexing requests. If the retry "
      + "attempts are exhausted the task will fail.";
  public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
  private static final String RETRY_BACKOFF_MS_DOC =
      "How long to wait in milliseconds before attempting the first retry of a failed indexing "
      + "request. Upon a failure, this connector may wait up to twice as long as the previous "
      + "wait, up to the maximum number of retries. "
      + "This avoids retrying in a tight loop under failure scenarios.";

  public static final String INDEX_USE_KEY_AS_ID = "index.use.key.as.id";
  private static final String INDEX_USE_KEY_AS_ID_DOC =
      "Use the key of the record as the id when indexing into Elasticsearch.\nIf false, the key will be"
      + "timestamp-random(1.000.000).\nIf true and key is null, generated key will be used to avoid exception";

  public static final String INDEX_NAME = "index.name";
  private static final String INDEX_NAME_DOC =
      "The name of the destination index. If the index does not exists, it will be created at the "
      + "start of the connect";
  public static final String NUMBER_OF_SHARD = "index.shard";
  private static final String NUMBER_OF_SHARD_DOC =
      "The number of shards of the index";
  public static final String NUMBER_OF_REPLICA = "index.replica";
  private static final String NUMBER_OF_REPLICA_DOC =
      "The replica of the index";

  public static final String READ_TIMEOUT_MS_CONFIG = "read.timeout.ms";
  private static final String READ_TIMEOUT_MS_CONFIG_DOC = "How long to wait in "
      + "milliseconds for the Elasticsearch server to send a response. The task fails "
      + "if any read operation times out, and will need to be restarted to resume "
      + "further operations.";

  public static final String ROUTING_ENABLE = "elastic.rooting.enable";
  private static final String ROUTING_ENABLE_DOC = "Is a specific routing required during the indexation? "
      + "Required for example if a parent / child relationship is on the index.";
  public static final String ROUTING_FIELD_NAME = "elastic.rooting.field.name";
  private static final String ROUTING_FIELD_NAME_DOC = "Name of the field who store the rooting value. "
      + "The routing field type must be string."
      + "This configuration will be ignored if routing is disable. The field will not be indexed into Elasticsearch.";

  private static final String ELASTICSEARCH_SECURITY_PROTOCOL_CONFIG = "elastic.security.protocol";
  private static final String ELASTICSEARCH_SECURITY_PROTOCOL_DOC =
      "The security protocol to use when connecting to Elasticsearch. "
          + "Values can be `PLAINTEXT` or `SSL`. If `PLAINTEXT` is passed, "
          + "all ssl security configs will be ignored.";

  public static final ConfigDef CONFIG = baseConfigDef();

  public ElasticsearchSinkConnectorConfig(Map<String, String> props) {
    super(CONFIG, props);
  }

  protected static ConfigDef baseConfigDef() {
    final ConfigDef configDef = new ConfigDef();
    addConnectorConfigs(configDef);
    addSecurityConfigs(configDef);
    return configDef;
  }

  private static void addSecurityConfigs(ConfigDef configDef) {
    int order = 0;
    configDef.define(
        ELASTICSEARCH_SECURITY_PROTOCOL_CONFIG,
        Type.STRING,
        SecurityProtocol.PLAINTEXT.name(),
        Importance.MEDIUM,
        ELASTICSEARCH_SECURITY_PROTOCOL_DOC,
        SSL_GROUP,
        ++order,
        Width.SHORT,
        "Security protocol"
    ).define(
        SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
        ConfigDef.Type.STRING,
        SslConfigs.DEFAULT_SSL_KEYSTORE_TYPE,
        ConfigDef.Importance.MEDIUM,
        SslConfigs.SSL_KEYSTORE_TYPE_DOC,
        SSL_GROUP,
        ++order,
        Width.SHORT,
        "SSL keystore type"
    ).define(
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
        ConfigDef.Type.STRING,
        null,
        ConfigDef.Importance.HIGH,
        SslConfigs.SSL_KEYSTORE_LOCATION_DOC,
        SSL_GROUP,
        ++order,
        Width.LONG,
        "SSL keystore location"
    ).define(
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
        ConfigDef.Type.PASSWORD,
        null,
        ConfigDef.Importance.HIGH,
        SslConfigs.SSL_KEYSTORE_PASSWORD_DOC,
        SSL_GROUP,
        ++order,
        Width.SHORT,
        "SSL keystore password"
    );
  }

  private static void addConnectorConfigs(ConfigDef configDef) {
    final String group = "Connector";
    int order = 0;
    configDef.define(
        CONNECTION_URL_CONFIG,
        Type.LIST,
        Importance.HIGH,
        CONNECTION_URL_DOC,
        group,
        ++order,
        Width.LONG,
        "Connection URLs"
    ).define(
        INDEX_NAME,
        Type.STRING,
        Importance.HIGH,
        INDEX_NAME_DOC,
        group,
        ++order,
        Width.LONG,
        "Index name"
    ).define(
        INDEX_USE_KEY_AS_ID,
        Type.BOOLEAN,
        true,
        Importance.HIGH,
        INDEX_USE_KEY_AS_ID_DOC,
        group,
        ++order,
        Width.SHORT,
        "Index id"
    ).define(
        CONNECTION_USERNAME_CONFIG,
        Type.STRING,
        null,
        Importance.MEDIUM,
        CONNECTION_USERNAME_DOC,
        group,
        ++order,
        Width.SHORT,
        "Connection Username"
    ).define(
        CONNECTION_PASSWORD_CONFIG,
        Type.PASSWORD,
        null,
        Importance.MEDIUM,
        CONNECTION_PASSWORD_DOC,
        group,
        ++order,
        Width.SHORT,
        "Connection Password"
    ).define(
        BATCH_SIZE_CONFIG,
        Type.INT,
        2000,
        Importance.MEDIUM,
        BATCH_SIZE_DOC,
        group,
        ++order,
        Width.SHORT,
        "Batch Size"
    ).define(
        MAX_IN_FLIGHT_REQUESTS_CONFIG,
        Type.INT,
        5,
        Importance.MEDIUM,
        MAX_IN_FLIGHT_REQUESTS_DOC,
        group,
        5,
        Width.SHORT,
        "Max In-flight Requests"
    ).define(
        ROUTING_ENABLE,
        Type.BOOLEAN,
        false,
        Importance.LOW,
        ROUTING_ENABLE_DOC,
        group,
        ++order,
        Width.MEDIUM,
        "Routing enable"
    ).define(
        ROUTING_FIELD_NAME,
        Type.STRING,
        "",
        Importance.LOW,
        ROUTING_FIELD_NAME_DOC,
        group,
        ++order,
        Width.MEDIUM,
        "Routing enable"
    ).define(
        NUMBER_OF_SHARD,
        Type.INT,
        10,
        Importance.LOW,
        NUMBER_OF_SHARD_DOC,
        group,
        ++order,
        Width.MEDIUM,
        "Number of shards"
    ).define(
        NUMBER_OF_REPLICA,
        Type.INT,
        2,
        Importance.LOW,
        NUMBER_OF_REPLICA_DOC,
        group,
        ++order,
        Width.MEDIUM,
        "Number of replica"
    ).define(
        MAX_BUFFERED_RECORDS_CONFIG,
        Type.INT,
        20_000,
        Importance.LOW,
        MAX_BUFFERED_RECORDS_DOC,
        group,
        ++order,
        Width.SHORT,
        "Max Buffered Records"
    ).define(
        BUFFERED_LINGER_MS_CONFIG,
        Type.LONG,
        1000,
        Importance.LOW,
        BUFFERED_LINGER_MS_DOC,
        group,
        ++order,
        Width.SHORT,
        "Buffer linger ms"
    ).define(
        LINGER_MS_CONFIG,
        Type.LONG,
        500L,
        Importance.LOW,
        LINGER_MS_DOC,
        group,
        ++order,
        Width.SHORT,
        "Linger (ms)"
    ).define(
        FLUSH_TIMEOUT_MS_CONFIG,
        Type.LONG,
        60000L,
        Importance.LOW,
        FLUSH_TIMEOUT_MS_DOC,
        group,
        ++order,
        Width.SHORT,
        "Flush Timeout (ms)"
    ).define(
        MAX_RETRIES_CONFIG,
        Type.INT,
        2,
        Importance.LOW,
        MAX_RETRIES_DOC,
        group,
        ++order,
        Width.SHORT,
        "Max Retries"
    ).define(
        RETRY_BACKOFF_MS_CONFIG,
        Type.LONG,
        100L,
        Importance.LOW,
        RETRY_BACKOFF_MS_DOC,
        group,
        ++order,
        Width.SHORT,
        "Retry Backoff (ms)"
      ).define(
        READ_TIMEOUT_MS_CONFIG, 
        Type.INT,
        10000,
        Importance.LOW, 
        READ_TIMEOUT_MS_CONFIG_DOC,
        group,
        ++order, 
        Width.SHORT, 
        "Read Timeout (ms)"
    );
  }

  public boolean secured() {
    SecurityProtocol securityProtocol = securityProtocol();
    return SecurityProtocol.SSL.equals(securityProtocol);
  }

  private SecurityProtocol securityProtocol() {
    return SecurityProtocol.valueOf(getString(ELASTICSEARCH_SECURITY_PROTOCOL_CONFIG));
  }

}
