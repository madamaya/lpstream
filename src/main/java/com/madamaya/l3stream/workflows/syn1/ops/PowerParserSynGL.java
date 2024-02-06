package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.glCommons.StringGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.regex.Pattern;

public class PowerParserSynGL implements MapFunction<StringGL, SynPowerTupleGL> {
    private static final Pattern delimiter = Pattern.compile(",");

    @Override
    public SynPowerTupleGL map(StringGL input) throws Exception {
        String inputStr = input.getString();
        String[] elements = delimiter.split(inputStr);
        int type = Integer.parseInt(elements[0]);
        if (type == 0) {
            return new SynPowerTupleGL(type);
        } else {
            SynPowerTupleGL tuple = new SynPowerTupleGL(
                    type,
                    Integer.parseInt(elements[1]),
                    Double.parseDouble(elements[2]),
                    elements[3],
                    Long.parseLong(elements[4]),
                    input.getDominantOpTime(),
                    input.getKafkaAppandTime(),
                    input.getStimulus()
            );
            GenealogMapHelper.INSTANCE.annotateResult(input, tuple);
            return tuple;
        }
    }
}
