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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import plugin.kafka.sink.elastic.ElasticsearchSinkConnectorConstants;

public class DataConvertionTest {
  
  private String key;
  private String topic;
  private int partition;
  private long offset;
  private String index;
  private String type;
  private Schema schema;

  @BeforeEach
  public void setUp() {
    key = "key";
    topic = "topic";
    partition = 0;
    offset = 0;
    index = "index";
    type = "type";
    schema = SchemaBuilder
        .struct()
        .name("struct")
        .field("string", Schema.STRING_SCHEMA)
        .build();
  }

  @Test
  public void primitives() {
    assertIdenticalAfterPreProcess(Schema.INT8_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.INT16_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.INT32_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.INT64_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.FLOAT32_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.FLOAT64_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.BOOLEAN_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.STRING_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.BYTES_SCHEMA);

    assertIdenticalAfterPreProcess(Schema.OPTIONAL_INT16_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.OPTIONAL_INT32_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.OPTIONAL_INT64_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.OPTIONAL_FLOAT32_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.OPTIONAL_FLOAT64_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.OPTIONAL_STRING_SCHEMA);
    assertIdenticalAfterPreProcess(Schema.OPTIONAL_BYTES_SCHEMA);

    assertIdenticalAfterPreProcess(SchemaBuilder.int8().defaultValue((byte) 42).build());
    assertIdenticalAfterPreProcess(SchemaBuilder.int16().defaultValue((short) 42).build());
    assertIdenticalAfterPreProcess(SchemaBuilder.int32().defaultValue(42).build());
    assertIdenticalAfterPreProcess(SchemaBuilder.int64().defaultValue(42L).build());
    assertIdenticalAfterPreProcess(SchemaBuilder.float32().defaultValue(42.0f).build());
    assertIdenticalAfterPreProcess(SchemaBuilder.float64().defaultValue(42.0d).build());
    assertIdenticalAfterPreProcess(SchemaBuilder.bool().defaultValue(true).build());
    assertIdenticalAfterPreProcess(SchemaBuilder.string().defaultValue("foo").build());
    assertIdenticalAfterPreProcess(SchemaBuilder.bytes().defaultValue(new byte[0]).build());
  }

  private void assertIdenticalAfterPreProcess(Schema schema) {
    assertEquals(schema, DataConverter.preProcessSchema(schema));
  }

  @Test
  public void decimal() {
    Schema origSchema = Decimal.schema(2);
    Schema preProcessedSchema = DataConverter.preProcessSchema(origSchema);
    assertEquals(Schema.FLOAT64_SCHEMA, preProcessedSchema);

    assertEquals(0.02, DataConverter.preProcessValue(new BigDecimal("0.02"), origSchema, preProcessedSchema));

    // optional
    assertEquals(
        Schema.OPTIONAL_FLOAT64_SCHEMA,
        DataConverter.preProcessSchema(Decimal.builder(2).optional().build())
    );

    // defval
    assertEquals(
        SchemaBuilder.float64().defaultValue(0.00).build(),
        DataConverter.preProcessSchema(Decimal.builder(2).defaultValue(new BigDecimal("0.00")).build())
    );
  }

  @Test
  public void array() {
    Schema origSchema = SchemaBuilder.array(Decimal.schema(2)).schema();
    Schema preProcessedSchema = DataConverter.preProcessSchema(origSchema);
    assertEquals(SchemaBuilder.array(Schema.FLOAT64_SCHEMA).build(), preProcessedSchema);

    assertEquals(
        Arrays.asList(0.02, 0.42),
        DataConverter.preProcessValue(Arrays.asList(new BigDecimal("0.02"), new BigDecimal("0.42")), origSchema, preProcessedSchema)
    );

    // optional
    assertEquals(
        SchemaBuilder.array(preProcessedSchema.valueSchema()).optional().build(),
        DataConverter.preProcessSchema(SchemaBuilder.array(Decimal.schema(2)).optional().build())
    );

    // defval
    assertEquals(
        SchemaBuilder.array(preProcessedSchema.valueSchema()).defaultValue(Collections.emptyList()).build(),
        DataConverter.preProcessSchema(SchemaBuilder.array(Decimal.schema(2)).defaultValue(Collections.emptyList()).build())
    );
  }

  @Test
  public void map() {
    Schema origSchema = SchemaBuilder.map(Schema.INT32_SCHEMA, Decimal.schema(2)).build();
    Schema preProcessedSchema = DataConverter.preProcessSchema(origSchema);
    assertEquals(
        SchemaBuilder.array(
            SchemaBuilder.struct().name(Schema.INT32_SCHEMA.type().name() + "-" + Decimal.LOGICAL_NAME)
                .field(ElasticsearchSinkConnectorConstants.MAP_KEY, Schema.INT32_SCHEMA)
                .field(ElasticsearchSinkConnectorConstants.MAP_VALUE, Schema.FLOAT64_SCHEMA)
                .build()
        ).build(),
        preProcessedSchema
    );

    Map<Object, Object> origValue = new HashMap<>();
    origValue.put(1, new BigDecimal("0.02"));
    origValue.put(2, new BigDecimal("0.42"));
    assertEquals(
        new HashSet<>(Arrays.asList(
            new Struct(preProcessedSchema.valueSchema())
                .put(ElasticsearchSinkConnectorConstants.MAP_KEY, 1)
                .put(ElasticsearchSinkConnectorConstants.MAP_VALUE, 0.02),
            new Struct(preProcessedSchema.valueSchema())
                .put(ElasticsearchSinkConnectorConstants.MAP_KEY, 2)
                .put(ElasticsearchSinkConnectorConstants.MAP_VALUE, 0.42)
        )),
        new HashSet<>((List<?>) DataConverter.preProcessValue(origValue, origSchema, preProcessedSchema))
    );

    // optional
    assertEquals(
        SchemaBuilder.array(preProcessedSchema.valueSchema()).optional().build(),
        DataConverter.preProcessSchema(SchemaBuilder.map(Schema.INT32_SCHEMA, Decimal.schema(2)).optional().build())
    );

    // defval
    assertEquals(
        SchemaBuilder.array(preProcessedSchema.valueSchema()).defaultValue(Collections.emptyList()).build(),
        DataConverter.preProcessSchema(SchemaBuilder.map(Schema.INT32_SCHEMA, Decimal.schema(2)).defaultValue(Collections.emptyMap()).build())
    );
  }

  @Test
  public void stringKeyedMapCompactFormat() {
    Schema origSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();

    Map<Object, Object> origValue = new HashMap<>();
    origValue.put("field1", 1);
    origValue.put("field2", 2);

    // Use the newer compact format for map entries with string keys
    Schema preProcessedSchema = DataConverter.preProcessSchema(origSchema);
    assertEquals(
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(),
        preProcessedSchema
    );
    HashMap<?, ?> newValue = (HashMap<?, ?>) DataConverter.preProcessValue(origValue, origSchema, preProcessedSchema);
    assertEquals(origValue, newValue);
  }

  @Test
  public void struct() {
    Schema origSchema = SchemaBuilder.struct().name("struct").field("decimal", Decimal.schema(2)).build();
    Schema preProcessedSchema = DataConverter.preProcessSchema(origSchema);
    assertEquals(
        SchemaBuilder.struct().name("struct").field("decimal", Schema.FLOAT64_SCHEMA).build(),
        preProcessedSchema
    );

    assertEquals(
        new Struct(preProcessedSchema).put("decimal", 0.02),
        DataConverter.preProcessValue(new Struct(origSchema).put("decimal", new BigDecimal("0.02")), origSchema, preProcessedSchema)
    );

    // optional
    assertEquals(
        SchemaBuilder.struct().name("struct").field("decimal", Schema.FLOAT64_SCHEMA).optional().build(),
        DataConverter.preProcessSchema(SchemaBuilder.struct().name("struct").field("decimal", Decimal.schema(2)).optional().build())
    );
  }

  @Test
  public void optionalFieldsWithoutDefaults() {
    // One primitive type should be enough
    testOptionalFieldWithoutDefault(SchemaBuilder.bool());
    // Logical types
    testOptionalFieldWithoutDefault(Decimal.builder(2));
    testOptionalFieldWithoutDefault(Time.builder());
    testOptionalFieldWithoutDefault(Timestamp.builder());
    // Complex types
    testOptionalFieldWithoutDefault(SchemaBuilder.array(Schema.BOOLEAN_SCHEMA));
    testOptionalFieldWithoutDefault(SchemaBuilder.struct().field("innerField", Schema.BOOLEAN_SCHEMA));
    testOptionalFieldWithoutDefault(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA));
    // Have to test maps with useCompactMapEntries set to true and set to false
    testOptionalFieldWithoutDefault(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA));
  }

  private void testOptionalFieldWithoutDefault(
    SchemaBuilder optionalFieldSchema
  ) {
    Schema origSchema = SchemaBuilder.struct().name("struct").field(
        "optionalField", optionalFieldSchema.optional().build()
    ).build();
    Schema preProcessedSchema = DataConverter.preProcessSchema(origSchema);

    Object preProcessedValue = DataConverter.preProcessValue(
        new Struct(origSchema).put("optionalField", null), origSchema, preProcessedSchema
    );

    assertEquals(new Struct(preProcessedSchema).put("optionalField", null), preProcessedValue);
  }

}
