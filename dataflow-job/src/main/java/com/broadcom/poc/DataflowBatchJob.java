package com.broadcom.poc;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

//Dataflow job to process CSV data in GCS with the Lookup data
//Store the processed data to GCS in Parquet format
public class DataflowBatchJob {
    public interface DataflowBatchJobOptions extends PipelineOptions {


        @Description("Path of the file to read from")
        @Default.String("gs://mybucket-poc/data/*.txt")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to read from")
        @Default.String("gs://mybucket-poc/lookup/lookup.txt")
        String getInputLookupFile();

        void setInputLookupFile(String value);

        /** Set this required option to specify where to write the output. */
        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();

        void setOutput(String value);
    }

        public static class Dump extends DoFn<String, String> {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println();
                System.out.println(c.element());
                System.out.println();

            }
        }

    //Convert each row to Key and Value
    //First Comma separated token is the Key and the Values are concatenation of the remaining tokens
    static class ExtractKeyValueFn extends DoFn<String, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            try {
                String row = c.element();
                String[] tokens = row.split(",");
                String key = tokens[0];
                String value = tokens[1] + "," + tokens[2] + "," + tokens[3] + "," + tokens[4];
                c.output(KV.of(key, value));
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public static class AddAColumn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println();
            System.out.println(c.element());
            System.out.println();

        }
    }

    //Join the data from the mainData and lookupData.
    // Each mainData element is added with the look up data by concatenation
    //Each mainData row is exploded with multiple rows of lookupData & mainData row
    static PCollection<String> joinData(
            PCollection<KV<String, String>> mainData, PCollection<KV<String, String>> lookupData) throws Exception {

        final TupleTag<String> mainDataTag = new TupleTag<>();
        final TupleTag<String> lookupDataTag = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> kvpCollection =
                KeyedPCollectionTuple.of(mainDataTag, mainData)
                        .and(lookupDataTag, lookupData)
                        .apply(CoGroupByKey.create());

        // Process the CoGbkResult elements generated by the CoGroupByKey transform.
        // country code 'key' -> string of <event info>, <country name>
        PCollection<KV<String, String>> finalResultCollection =
                kvpCollection.apply(
                        "Process",
                        ParDo.of(
                                new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        KV<String, CoGbkResult> e = c.element();
                                        String key = e.getKey();
                                        try {
                                            Iterable<String> mainValue = e.getValue().getAll(mainDataTag);
                                            for (String lookupValue : c.element().getValue().getAll(lookupDataTag)) {
                                                // Generate a string that combines information from both collection values
                                                c.output(KV.of(
                                                        key, mainValue.toString() + "," + lookupValue));
                                            }
                                        }
                                        catch (Exception ex){
                                            ex.printStackTrace();

                                        }
                                    }
                                }));

        PCollection<String> formattedResults =
                finalResultCollection.apply(
                        "Format",
                        ParDo.of(
                                new DoFn<KV<String, String>, String>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        String outputstring =
                                                c.element().getKey() + ", " + c.element().getValue();
                                        c.output(outputstring);
                                    }
                                }));
        return formattedResults;
    }


    private static final Schema SCHEMA = new Schema.Parser().parse("{\n"
            + " \"namespace\": \"ioitavro\",\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"BroadcomAvroLine\",\n"
            + " \"fields\": [\n"
            + "     {\"name\": \"row\", \"type\": \"string\"}\n"
            + " ]\n"
            + "}");

    private static class DeterministicallyConstructAvroRecordsFn extends DoFn<String, GenericRecord> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(
                    new GenericRecordBuilder(SCHEMA).set("row", c.element()).build()
            );
        }
    }

    //Read the Source CSV files from GCS and Lookup data CSV from GCS
    //Joined Source data and Lookup Data
    //Write to GCS in Parquet format
    public static void main(String[] args) {
        DataflowBatchJob.DataflowBatchJobOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowBatchJob.DataflowBatchJobOptions.class);

        Pipeline p = Pipeline.create(options);

        //Read from Google Cloud Storage(GCS) to mainData
        PCollection<String> mainData = p.apply("Read Big Stream from " + options.getInputFile(), TextIO.read()
                .from(options.getInputFile()))
                // filter the header row
                .apply("Remove header row",
                        Filter.by((String row) -> !((row.startsWith("key")))));
        //Read the lookup data to lookupData
        PCollection<String> lookupData = p.apply("Read Lookup Stream from " + options.getInputFile(), TextIO.read()
                .from(options.getInputLookupFile()))
                // filter the header row
                .apply("Remove header row",
                        Filter.by((String row) -> !((row.startsWith("key")))));

        PCollection<KV<String, String>> mainKVData =
                mainData.apply("Extract Key, Value from Big Stream", ParDo.of(new ExtractKeyValueFn()));
        PCollection<KV<String, String>> lookupKVData =
                lookupData.apply("Extract Key, Value from Lookup Stream", ParDo.of(new ExtractKeyValueFn()));


        PCollection<String> formattedResults = null;
        try {
            formattedResults = joinData(mainKVData, lookupKVData);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //Write to GCS in Parquet format
        formattedResults.apply("Produce Avro records", ParDo.of(new DeterministicallyConstructAvroRecordsFn()))
                .setCoder(AvroCoder.of(SCHEMA))
                .setCoder(AvroCoder.of(GenericRecord.class, SCHEMA)) //PCollection<GenericRecord>
                .apply(FileIO.<GenericRecord>write().via(ParquetIO.sink(SCHEMA)).to(options.getOutput()).withSuffix(".parquet"));

//        formattedResults.apply(TextIO.write().to(options.getOutput() + "csv/"));


        p.run().waitUntilFinish();

    }
}
