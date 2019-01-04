package com.doug.streaming;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

/**
 * Created by doug on 4/20/16.
 */
public class MakeUppperCase extends DoFn<String, String> {

    @Override
    public void processElement(ProcessContext c) {
        c.output(c.element().toUpperCase());
    }
}
