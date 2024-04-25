package com.madamaya.l3stream.workflows.nyc.ops;

import com.madamaya.l3stream.glCommons.StringGL;
import com.madamaya.l3stream.workflows.nyc.objects.NYCInputTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;

import java.text.SimpleDateFormat;

public class DataParserNYCGL implements MapFunction<StringGL, NYCInputTupleGL> {
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @Override
    public NYCInputTupleGL map(StringGL input) throws Exception {
        String inputStr = input.getString();
        String line = inputStr.substring(1, inputStr.length() - 1).trim();
        NYCInputTupleGL out = new NYCInputTupleGL(line, input.getDominantOpTime(), input.getKafkaAppandTime(), input.getStimulus(), sdf);
        GenealogMapHelper.INSTANCE.annotateResult(input, out);

        return out;
    }
}
