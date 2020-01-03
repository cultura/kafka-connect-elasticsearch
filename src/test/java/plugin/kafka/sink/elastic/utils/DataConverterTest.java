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

package plugin.kafka.sink.elastic.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;
import plugin.kafka.sink.elastic.models.SinkRecordsHandler;
import plugin.kafka.sink.elastic.testutil.TestUtil;

public class DataConverterTest {

  @Test
  public void should_return_null_for_null_key(){
    SinkRecordsHandler sinkRecordsHandler = new SinkRecordsHandler(null);
    sinkRecordsHandler.addRecord(null, null);
    assertNull(DataConverter.convertKey(sinkRecordsHandler.getRecords().get(0)));
  }

  @Test
  public void should_return_string_key_for_allowed_type(){
    SinkRecordsHandler sinkRecordsHandler = new SinkRecordsHandler(Schema.INT8_SCHEMA);
    sinkRecordsHandler.addRecord(0, null);
    assertEquals("0", DataConverter.convertKey(sinkRecordsHandler.getRecords().get(0)));

    sinkRecordsHandler = new SinkRecordsHandler(Schema.INT16_SCHEMA);
    sinkRecordsHandler.addRecord(0, null);
    assertEquals("0", DataConverter.convertKey(sinkRecordsHandler.getRecords().get(0)));

    sinkRecordsHandler = new SinkRecordsHandler(Schema.INT32_SCHEMA);
    sinkRecordsHandler.addRecord(0, null);
    assertEquals("0", DataConverter.convertKey(sinkRecordsHandler.getRecords().get(0)));

    sinkRecordsHandler = new SinkRecordsHandler(Schema.INT64_SCHEMA);
    sinkRecordsHandler.addRecord(0, null);
    assertEquals("0", DataConverter.convertKey(sinkRecordsHandler.getRecords().get(0)));

    sinkRecordsHandler = new SinkRecordsHandler(Schema.STRING_SCHEMA);
    sinkRecordsHandler.addRecord("test", null);
    assertEquals("test", DataConverter.convertKey(sinkRecordsHandler.getRecords().get(0)));
  }

  @Test
  public void should_fail_key_conversion_for_custom_type(){
    Schema schema = TestUtil.createSchema();
    SinkRecordsHandler sinkRecordsHandler = new SinkRecordsHandler(schema);
    sinkRecordsHandler.addRecord(TestUtil.createRecord(schema), null);
    assertThrows(DataException.class, () -> DataConverter.convertKey(sinkRecordsHandler.getRecords().get(0)));
  }

  @Test
  public void should_convert_simple_keys_without_schema(){
    SinkRecordsHandler handler = new SinkRecordsHandler(null);
    handler.addRecord("test", null);
    assertEquals("test", DataConverter.convertKey(handler.getRecords().get(0)));
  }

  @Test
  public void should_not_convert_complex_keys_without_schema(){
    SinkRecordsHandler handler = new SinkRecordsHandler(null);
    handler.addRecord(Collections.singleton("test"), null);
    assertThrows(DataException.class, () -> DataConverter.convertKey(handler.getRecords().get(0)));
  }

  @Test
  public void should_return_null_for_null_value(){
    SinkRecordsHandler handler = new SinkRecordsHandler();
    handler.addRecord(null, null);
    assertNull(DataConverter.getPayload(handler.getRecords().get(0)));
  }

}
