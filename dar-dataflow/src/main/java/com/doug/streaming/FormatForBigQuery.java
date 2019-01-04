package com.doug.streaming;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.transforms.DoFn;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by doug on 4/20/16.
 */
public class FormatForBigQuery extends
        DoFn<String, TableRow> {
    @Override
    public void processElement(ProcessContext processContext) throws Exception {
        String input = processContext.element();
        TableRow row = new TableRow()
                .set("Message", input);
        processContext.output(row);
    }

    public static TableSchema getBigQuerySchema(){
        // This example writes to BigQuery, so need to first make a Table Schema for BigQuery
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("Message").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }

}

