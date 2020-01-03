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
    along with Foobar.  If not, see <https://www.gnu.org/licenses/>.

    contact: team.api.support@cultura.fr
 */

package plugin.kafka.sink.elastic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig;

@Slf4j
public class ElasticSinkConnect extends SinkConnector {

  private Map<String, String> configProperties;

  @Override
  public void start(Map<String, String> props) {
    log.info("---------------------------> start connector");
    try {
      configProperties = props;
      new ElasticsearchSinkConnectorConfig(props);
    } catch (ConfigException e) {
      throw new ConnectException(
          "Couldn't start Elasticsearch Sink Connector due to configuration error", e
      );
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return ElasticSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("---------------------------> configure task with number : {}", maxTasks);
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    Map<String, String> taskProps = new HashMap<>(configProperties);
    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() {
    log.info("---------------------------> stopping elastic sink connect");
  }

  @Override
  public ConfigDef config() {
    return ElasticsearchSinkConnectorConfig.CONFIG;
  }

  @Override
  public String version() {
    return null;
  }
}
