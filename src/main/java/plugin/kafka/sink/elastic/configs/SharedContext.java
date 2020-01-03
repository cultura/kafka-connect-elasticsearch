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

package plugin.kafka.sink.elastic.configs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.action.index.IndexRequest;
import plugin.kafka.sink.elastic.client.ElasticClient;
import plugin.kafka.sink.elastic.health.TaskHealth;

@Builder
@Getter
public class SharedContext {

  @Setter
  private ElasticClient client;
  private TaskHealth taskHealth;

  private long lingerMs;
  private int maxInFlightRequests;
  private int batchSize;
  private int maxRetries;
  private long retryBackoffMs;
  private long bufferLingerMs;
  private long readTimeoutMS;
  private long flushTimeoutMS;
  private int maxBufferedRecords;
  @Setter
  private volatile boolean stopRequested;
  @Setter
  private volatile boolean flushRequested;

  private AtomicInteger inFlightRecords;
  private ConcurrentLinkedQueue<IndexRequest> unsentRecords;

  public void init(){
    this.inFlightRecords = new AtomicInteger();
    this.unsentRecords = new ConcurrentLinkedQueue<>();
  }

  public synchronized int bufferedRecords() {
    return unsentRecords.size() + inFlightRecords.get();
  }

  public void addRecord(IndexRequest request){
    this.unsentRecords.add(request);
  }

  private IndexRequest removeRecord(){
    return this.unsentRecords.poll();
  }

  private void incrementInFlightRecords(int batchSize){
    this.inFlightRecords.addAndGet(batchSize);
  }

  public void decrementInFlightRecords(int batchSize){
    this.inFlightRecords.addAndGet(- batchSize);
  }

  public synchronized List<IndexRequest> createBulk() {
    int bulkSize = Math.min(
        getBatchSize(), getUnsentRecords().size());
    List<IndexRequest> requests = new ArrayList<>(bulkSize);
    for (int i = 0; i < bulkSize; i++) {
      requests.add(removeRecord());
    }
    incrementInFlightRecords(bulkSize);
    return requests;
  }

}
