
package com.doug.streaming;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("serial")
public class PubSubStreamingPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubStreamingPipeline.class);


    public static void main(String[] args) {

        String BUCKET_NAME = "doug-rehnstrom-dataflow-temp";
        String PROJECT_ID = "doug-rehnstrom";
        String TOPIC = "messages";

        DataflowPipelineOptions options = PipelineOptionsFactory.create()
                .as(DataflowPipelineOptions.class);
        options.setStreaming(true);
        options.setRunner(DataflowPipelineRunner.class);
        options.setProject(PROJECT_ID);
        options.setStagingLocation(String.format("gs://%s/pubsub-streaming-pipeline-staging", BUCKET_NAME));
        Pipeline p = Pipeline.create(options);


        p.apply(PubsubIO.Read.named("ReadFromPubsub")
                .topic(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC)))
                .apply(ParDo.of(new MakeUppperCase()))
                .apply(ParDo.of(new FormatForBigQuery()))
                .apply(BigQueryIO.Write
                        .named("Write")
                        .to(String.format("%s:streaming_dataset.messages_table", PROJECT_ID))
                        .withSchema(FormatForBigQuery.getBigQuerySchema())
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        p.run();
    }
}