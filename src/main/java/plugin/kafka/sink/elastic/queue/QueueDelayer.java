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

package plugin.kafka.sink.elastic.queue;

import java.util.Map;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import plugin.kafka.sink.elastic.ElasticSinkTask;
import plugin.kafka.sink.elastic.configs.ElasticsearchSinkConnectorConfig;
import plugin.kafka.sink.elastic.configs.SharedContext;

/**
 * Handle the delay when buffer is full or during {@link ElasticSinkTask#flush(Map)}
 */
@Slf4j
@Builder
public class QueueDelayer {

  private Time time;
  private SharedContext sharedContext;

  /**
   * Wait {@link ElasticsearchSinkConnectorConfig#LINGER_MS_CONFIG} until buffer isn't full or
   * {@link ElasticsearchSinkConnectorConfig#FLUSH_TIMEOUT_MS_CONFIG} is reach.
   */
  public void waitBeforeAddRequest() {
    final long startTime = time.milliseconds();
    for (long elapsedMs = time.milliseconds() - startTime;
        !sharedContext.isStopRequested() && elapsedMs < sharedContext.getFlushTimeoutMS() && sharedContext
            .bufferedRecords() >= sharedContext.getMaxBufferedRecords();
        elapsedMs = time.milliseconds() - startTime) {
      try {
        log.debug(
            "elastic ---------------------------> waiting {} before added new data into full buffer {}",
            elapsedMs, sharedContext.bufferedRecords());
        Thread.sleep(sharedContext.getBufferLingerMs());
      } catch (InterruptedException e) {
        throw new ConnectException(e);
      }
    }
  }

  /**
   * Wait {@link ElasticsearchSinkConnectorConfig#LINGER_MS_CONFIG} until buffer is empty or
   * {@link ElasticsearchSinkConnectorConfig#FLUSH_TIMEOUT_MS_CONFIG} is reach.
   * @throws InterruptedException
   */
  public void waitDuringFlush() throws InterruptedException {
    final long startTime = time.milliseconds();
    long elapsedMs = startTime - time.milliseconds();
    while (!sharedContext.isStopRequested() && elapsedMs < sharedContext.getFlushTimeoutMS() && sharedContext
        .bufferedRecords() > 0) {
      Thread.sleep(sharedContext.getLingerMs());
      elapsedMs += sharedContext.getLingerMs();
      log.debug("---------------------------> elapseTime : {}, remaining records in buffer : {}",
          elapsedMs, sharedContext.bufferedRecords());
    }
  }

}
