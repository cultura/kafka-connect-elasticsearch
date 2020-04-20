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

package plugin.kafka.sink.elastic.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.KEYWORD_TYPE;
import static plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants.TEXT_TYPE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.gson.JsonObject;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;
import plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants;

public class MappingTest{

  private static final String INDEX = "kafka-connect";
  private static final String TYPE = "kafka-connect-type";

  @Test
  @SuppressWarnings("unchecked")
  public void testStringMapping() throws Exception {

    Schema schema = SchemaBuilder.struct().name("textRecord")
            .field("string", Schema.STRING_SCHEMA)
            .build();
    ObjectNode mapping = (ObjectNode) Mapping.inferMapping(schema);
    ObjectNode properties = mapping.with("properties");
    ObjectNode string = properties.with("string");
    TextNode stringType = (TextNode) string.get("type");
    ObjectNode fields = string.with("fields");
    ObjectNode keyword = fields.with("keyword");
    TextNode keywordType = (TextNode) keyword.get("type");
    NumericNode ignoreAbove = (NumericNode) keyword.get("ignore_above");

    assertEquals(TEXT_TYPE, stringType.asText());
    assertEquals(KEYWORD_TYPE, keywordType.asText());
    assertEquals(256, ignoreAbove.asInt());
  }

  @Test
  public void testInferMapping() throws Exception {

    Schema stringSchema = SchemaBuilder
        .struct()
        .name("record")
        .field("foo", SchemaBuilder.string().defaultValue("0").build())
        .build();
    JsonNode stringMapping = Mapping.inferMapping(stringSchema);

    assertNull(stringMapping.get("properties").get("foo").get("null_value"));

    Schema intSchema =SchemaBuilder
        .struct()
        .name("record")
        .field("foo", SchemaBuilder.int32().defaultValue(0).build())
        .build();

    JsonNode intMapping = Mapping.inferMapping(intSchema);
    assertNotNull(intMapping.get("properties").get("foo").get("null_value"));
    assertEquals(0, intMapping.get("properties").get("foo").get("null_value").asInt());
  }

  protected Schema createSchema() {
    Schema structSchema = createInnerSchema();
    return SchemaBuilder.struct().name("record")
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("bytes", Schema.BYTES_SCHEMA)
        .field("int8", Schema.INT8_SCHEMA)
        .field("int16", Schema.INT16_SCHEMA)
        .field("int32", Schema.INT32_SCHEMA)
        .field("int64", Schema.INT64_SCHEMA)
        .field("float32", Schema.FLOAT32_SCHEMA)
        .field("float64", Schema.FLOAT64_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
        .field("struct", structSchema)
        .field("decimal", Decimal.schema(2))
        .field("date", Date.SCHEMA)
        .field("time", Time.SCHEMA)
        .field("timestamp", Timestamp.SCHEMA)
        .build();
  }

  private Schema createInnerSchema() {
    return SchemaBuilder.struct().name("inner")
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("bytes", Schema.BYTES_SCHEMA)
        .field("int8", Schema.INT8_SCHEMA)
        .field("int16", Schema.INT16_SCHEMA)
        .field("int32", Schema.INT32_SCHEMA)
        .field("int64", Schema.INT64_SCHEMA)
        .field("float32", Schema.FLOAT32_SCHEMA)
        .field("float64", Schema.FLOAT64_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
        .field("decimal", Decimal.schema(2))
        .field("date", Date.SCHEMA)
        .field("time", Time.SCHEMA)
        .field("timestamp", Timestamp.SCHEMA)
        .build();
  }

  @SuppressWarnings("unchecked")
  private void verifyMapping(Schema schema, JsonObject mapping) throws Exception {
    String schemaName = schema.name();
    Object type = mapping.get("type");
    if (schemaName != null) {
      switch (schemaName) {
        case Date.LOGICAL_NAME:
        case Time.LOGICAL_NAME:
        case Timestamp.LOGICAL_NAME:
          assertEquals("\"" + ElasticsearchSinkConnectorConstants.DATE_TYPE + "\"", type.toString());
          return;
        case Decimal.LOGICAL_NAME:
          assertEquals("\"" + ElasticsearchSinkConnectorConstants.DOUBLE_TYPE + "\"", type.toString());
          return;
      }
    }

    Schema.Type schemaType = schema.type();
    switch (schemaType) {
      case ARRAY:
        verifyMapping(schema.valueSchema(), mapping);
        break;
      case MAP:
        Schema newSchema = DataConverter.preProcessSchema(schema);
        JsonObject mapProperties = mapping.get("properties").getAsJsonObject();
        verifyMapping(newSchema.keySchema(), mapProperties.get(ElasticsearchSinkConnectorConstants.MAP_KEY).getAsJsonObject());
        verifyMapping(newSchema.valueSchema(), mapProperties.get(ElasticsearchSinkConnectorConstants.MAP_VALUE).getAsJsonObject());
        break;
      case STRUCT:
        JsonObject properties = mapping.get("properties").getAsJsonObject();
        for (Field field: schema.fields()) {
          verifyMapping(field.schema(), properties.get(field.name()).getAsJsonObject());
        }
        break;
      default:
        assertEquals("\"" + Mapping.getElasticsearchType(schemaType) + "\"", type.toString());
    }
  }
}
