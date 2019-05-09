/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.metrics.pipeline;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaAvroBeamSQLToKafka {
    private final static Logger LOG = LoggerFactory.getLogger(KafkaAvroBeamSQLToKafka.class);

    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();

    static final String OUTPUT_PATH = "gs://data-mydata/output/kafka_metadata/";  // Default output path
    static final String BOOTSTRAP_SERVERS = "kafka-cluster-10-nodes-w-0:9092";  // Default bootstrap kafka servers
    static final String IN_TOPIC = "avrotopic";  // Default kafka topic name
    static final String OUT_TOPIC = "outtopic";  // Default kafka topic name
    static final int WINDOW_SIZE = 10; // Default window duration in minutes
    static final Duration WINDOW_LATENESS_DURATION = Duration.standardSeconds(0);

    /**
     * Specific pipeline options.
     */
    public interface KafkaPipelineOptions extends PipelineOptions, StreamingOptions {
        @Description("Kafka bootstrap servers")
        @Default.String(BOOTSTRAP_SERVERS)
        String getBootstrap();

        void setBootstrap(String value);

        @Description("Output Path")
        @Default.String(OUTPUT_PATH)
        String getOutput();

        void setOutput(String value);

        @Description("Kafka topic name")
        @Default.String(IN_TOPIC)
        String getTopic();

        void setTopic(String value);

        @Description("outputPrefix")
        String getoutputPrefix();

        void setoutputPrefix(String value);

        @Description("Consumer group")
        @Default.String("kafka-bq-client")
        String getConsumerGroup();

        void setConsumerGroup(String value);

        @Description("What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server ")
        @Default.String("latest")
        String getConsumerOffsetReset();

        void setConsumerOffsetReset(String value);

        @Description("Window duration in seconds")
        @Default.Integer(5)
        int getWindowDuration();

        void setWindowDuration(int value);
    }

    public static final String schemaStr = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"event_type\", \"type\":\"string\" },"
            + "  { \"name\":\"action_file_name\", \"type\":\"string\" },"
            + "  { \"name\":\"action_file_md5\", \"type\":\"string\" },"
            + "  { \"name\":\"action_register_key_name\", \"type\":\"string\" },"
            + "  { \"name\":\"action_register_data\", \"type\":\"string\" }"
            + "]}";

    static Schema schemaWrite;

    static String beamSQL = "select * from PCOLLECTION where event_type = '3' AND (action_file_name LIKE 'bad.exe' OR action_file_md5 LIKE '99999999')";

    public final static void main(String args[]) throws Exception {
        LOG.info("main({}) <<", Arrays.asList(args));
        KafkaAvroBeamSQLToKafka.KafkaPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(KafkaAvroBeamSQLToKafka.KafkaPipelineOptions.class);
        Schema.Parser parser = new Schema.Parser();
        schemaWrite = parser.parse(schemaStr);

        options.setStreaming(true);

        Schema schema = (new Schema.Parser()).parse(schemaStr);
        AvroCoder<GenericRecord> avroCoder = AvroCoder.of(schema);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<Row> data = pipeline.apply(new KafkaAvroBeamSQLToKafka.ReadKafka())
                .apply(ParDo.of(new KafkaAvroBeamSQLToKafka.KafkaRecordToRowFn(avroCoder)))
                .apply("Windowing", Window.into(FixedWindows.of(Duration.standardSeconds(5))));
        org.apache.beam.sdk.schemas.Schema pcollectionSchema = org.apache.beam.sdk.schemas.Schema.builder().addStringField("event_type").
                addStringField("action_file_name").addStringField("action_file_md5").
                addStringField("action_register_key_name").addStringField("action_register_data").build();

        data = data.setRowSchema(pcollectionSchema);

        PCollection<Row> queriedData = data.apply("Beam_SQL", SqlTransform.query(beamSQL));
        queriedData.apply("", ParDo.of(new RowToGenericRecordToKV()))
                .apply(new KafkaAvroBeamSQLToKafka.KafkaWrite());


        pipeline.run().waitUntilFinish();
    }

    public static class ReadKafka extends PTransform<PBegin, PCollection<KafkaRecord<String, byte[]>>> {

        @Override
        public PCollection<KafkaRecord<String, byte[]>> expand(PBegin pBegin) {
            KafkaAvroBeamSQLToKafka.KafkaPipelineOptions options = (KafkaAvroBeamSQLToKafka.KafkaPipelineOptions) pBegin.getPipeline().getOptions();

            KafkaIO.Read<String, byte[]> kafkaRead = KafkaIO.<String, byte[]>read()
                    .withBootstrapServers(BOOTSTRAP_SERVERS)
                    .withTopic(IN_TOPIC)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of())
                    .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"));

            // only consider value of kafkaRecord no matter which format selected
            PCollection<KafkaRecord<String, byte[]>> kafkaRecords = pBegin.apply("Read from Kafka", kafkaRead);

            return kafkaRecords;
        }
    }

    public static class KafkaRecordToRowFn extends DoFn<KafkaRecord<String, byte[]>, Row> {

        private AvroCoder<GenericRecord> avroCoder = null;
        public static org.apache.beam.sdk.schemas.Schema rowSchema = org.apache.beam.sdk.schemas.Schema.builder().addStringField("event_type").
                addStringField("action_file_name").addStringField("action_file_md5").
                addStringField("action_register_key_name").addStringField("action_register_data").build();


        public KafkaRecordToRowFn(AvroCoder avroCoder) {
            Schema schema = (new Schema.Parser()).parse(schemaStr);
            AvroCoder<GenericRecord> avroCoder2 = AvroCoder.of(schema);
            this.avroCoder = avroCoder2;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            GenericRecord record = avroCoder.decode(new ByteArrayInputStream(c.element().getKV().getValue()));
            Row row = Row.withSchema(rowSchema).addValues(record.get("event_type").toString(), record.get("action_file_name").toString(),
                    record.get("action_file_md5").toString(), record.get("action_register_key_name").toString(),
                    record.get("action_register_data").toString()).build();
            c.output(row);
        }
    }


    public static class RowToGenericRecordToKV extends DoFn<Row, KV<byte[], byte[]>> {
        public static org.apache.beam.sdk.schemas.Schema schema = org.apache.beam.sdk.schemas.Schema.builder().addStringField("event_type").
                addStringField("action_file_name").addStringField("action_file_md5").
                addStringField("action_register_key_name").addStringField("action_register_data").build();
        private AvroCoder<GenericRecord> avroCoder = null;

        public RowToGenericRecordToKV() {
            Schema schema = (new Schema.Parser()).parse(schemaStr);
            AvroCoder<GenericRecord> avroCoder2 = AvroCoder.of(schema);
            this.avroCoder = avroCoder2;

        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            Row row = c.element();
            Schema schema = (new Schema.Parser()).parse(schemaStr);
            GenericRecord record = new GenericData.Record(schema);
            record.put("event_type", row.getString("event_type"));
            record.put("action_file_name", row.getString("action_file_name"));
            record.put("action_file_md5", row.getString("action_file_md5"));
            record.put("action_register_key_name", row.getString("action_register_key_name"));
            record.put("action_register_data", row.getString("action_register_data"));
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            avroCoder.encode(record, baos);

            c.output(KV.of(String.valueOf(System.currentTimeMillis()).getBytes(), baos.toByteArray()));
        }
    }


    public static class KafkaWrite extends PTransform<PCollection<KV<byte[], byte[]>>, PDone> {

        @Override
        public PDone expand(PCollection<KV<byte[], byte[]>> input) {
            KafkaAvroBeamSQLToKafka.KafkaPipelineOptions options = (KafkaAvroBeamSQLToKafka.KafkaPipelineOptions) input.getPipeline().getOptions();

            KafkaIO.Write<byte[], byte[]> kafkaWrite = KafkaIO.<byte[], byte[]>write()
                    .withBootstrapServers(BOOTSTRAP_SERVERS)
                    .withTopic(OUT_TOPIC)
                    .withKeySerializer(ByteArraySerializer.class)
                    .withValueSerializer(ByteArraySerializer.class);

            return input.apply("Write Kafka", kafkaWrite);
        }
    }


}

