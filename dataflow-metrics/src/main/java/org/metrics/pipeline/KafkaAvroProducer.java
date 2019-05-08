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



import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaAvroProducer {
    public static final String classSchema = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"event_type\", \"type\":\"string\" },"
            + "  { \"name\":\"action_file_name\", \"type\":\"string\" },"
            + "  { \"name\":\"action_file_md5\", \"type\":\"string\" },"
            + "  { \"name\":\"action_register_key_name\", \"type\":\"string\" },"
            + "  { \"name\":\"action_register_data\", \"type\":\"string\" }"
            + "]}";


    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); //run from worker node
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(classSchema);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);
        for (long i = 0; i < 1000000000; i++) { //1Billion
            GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("event_type", "3");
            avroRecord.put("action_file_name", "bad.exe");
            avroRecord.put("action_file_md5", "99999999");
            avroRecord.put("action_register_key_name", "BadKey");
            avroRecord.put("action_register_data", "fakeKeyValue");

            byte[] bytes = recordInjection.apply(avroRecord);

//            System.out.println("Sending : " + i);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("avrotopic", bytes);
            producer.send(record);
        }

        producer.close();
    }


}

