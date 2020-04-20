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

package plugin.kafka.sink.elastic.models;

import static plugin.kafka.sink.elastic.testutil.TestUtil.createRecord;
import static plugin.kafka.sink.elastic.testutil.TestUtil.createSchema;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkRecordsHandler {

  private static final String TOPIC = "unused";
  private static final int PARTITION = 0;

  private Schema keySchema;
  private Schema valueSchema;
  private List<SinkRecord> records = new ArrayList<>();

  public SinkRecordsHandler() {
    this.keySchema = Schema.STRING_SCHEMA;
    this.valueSchema = createSchema();
  }

  public SinkRecordsHandler(Schema keySchema) {
    this.keySchema = keySchema;
    this.valueSchema = createSchema();
  }

  public SinkRecordsHandler(Schema keySchema, Schema valueSchema) {
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
  }

  public List<SinkRecord> getRecords(){
    return this.records;
  }

  public void clearRecords(){
    this.records.clear();
  }

  public void addRecord(){
    addRecord("key", "john", "test elastic 7");
  }

  public void addRecord(Object key, Object value){
    records.add(new SinkRecord(TOPIC,
        PARTITION,
        keySchema,
        key,
        valueSchema,
        value,
        0));
  }

  public void addRecord(String key, String user, String message){
    records.add(new SinkRecord(TOPIC,
        PARTITION,
        keySchema,
        key,
        valueSchema,
        createRecord(valueSchema, user, message),
        0));
  }

  public void generateRandomMessages(int number){
    for (int i = 0; i < number; i++) {
      addRecord(RandomStringUtils.random(3), RandomStringUtils.random(10),
          RandomStringUtils.random(20));
    }
  }

}
