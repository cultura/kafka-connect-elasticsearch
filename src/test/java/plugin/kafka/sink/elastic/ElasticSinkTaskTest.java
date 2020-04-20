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

package plugin.kafka.sink.elastic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static plugin.kafka.sink.elastic.ElasticSinkTask.ROUTING_REGEX_BEGENNING;
import static plugin.kafka.sink.elastic.ElasticSinkTask.ROUTING_REGEX_END;

import java.util.regex.Pattern;
import org.apache.kafka.common.utils.SystemTime;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.jupiter.api.Test;

public class ElasticSinkTaskTest {

  private ElasticSinkTask elasticSinkTask = new ElasticSinkTask();

  @Test
  public void should_return_key(){
    elasticSinkTask.setRecordKeyId(true);
    assertEquals("key", elasticSinkTask.generateDocId("key"));
  }

  @Test
  public void should_not_return_key_when_false(){
    elasticSinkTask.setRecordKeyId(false);
    elasticSinkTask.setTime(new SystemTime());
    assertNotEquals("key", elasticSinkTask.generateDocId("key"));
  }

  @Test
  public void should_not_return_key_when_key_null(){
    elasticSinkTask.setRecordKeyId(false);
    elasticSinkTask.setTime(new SystemTime());
    assertNotNull(elasticSinkTask.generateDocId(null));
  }

  @Test
  public void should_add_routing(){
    String routingRegex = ROUTING_REGEX_BEGENNING + "ean" + ROUTING_REGEX_END;
    elasticSinkTask.setRoutingFieldRegex(routingRegex);
    elasticSinkTask.setRoutingFieldPattern(Pattern.compile(routingRegex));

    String payload ="{\"editeur\":\"\",\"nomenclature\":\"PCE\",\"ean\":\"366482400048\",\"diffuseur\":\"\"}";
    IndexRequest indexRequest = new IndexRequest();
    String payloadClean = elasticSinkTask.addRouting(payload, indexRequest);
    assertEquals("{\"editeur\":\"\",\"nomenclature\":\"PCE\",\"diffuseur\":\"\"}", payloadClean);
    assertEquals("366482400048", indexRequest.routing());

  }

  @Test
  public void should_not_add_routing(){
    String routingRegex = ROUTING_REGEX_BEGENNING + "codePro" + ROUTING_REGEX_END;
    elasticSinkTask.setRoutingFieldRegex(routingRegex);
    elasticSinkTask.setRoutingFieldPattern(Pattern.compile(routingRegex));

    String payload ="{\"editeur\":\"\",\"nomenclature\":\"PCE\",\"ean\":\"366482400048\",\"diffuseur\":\"\"}";
    IndexRequest indexRequest = new IndexRequest();
    String payloadClean = elasticSinkTask.addRouting(payload, indexRequest);
    assertEquals(payload, payloadClean);
    assertNull(indexRequest.routing());

  }

}
