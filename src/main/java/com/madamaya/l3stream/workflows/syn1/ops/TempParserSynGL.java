package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.glCommons.StringGL;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.regex.Pattern;

public class TempParserSynGL implements MapFunction<StringGL, SynTempTupleGL> {
    private static final Pattern delimiter = Pattern.compile(",");

    @Override
    public SynTempTupleGL map(StringGL input) throws Exception {
        String inputStr = input.getString();
        String[] elements = delimiter.split(inputStr);
        int type = Integer.parseInt(elements[0]);
        if (type == 0) {
            SynTempTupleGL tuple = new SynTempTupleGL(
                    type,
                    Integer.parseInt(elements[1]),
                    Integer.parseInt(elements[2]),
                    Double.parseDouble(elements[3]),
                    elements[4],
                    Long.parseLong(elements[5]),
                    input.getStimulus()
            );
            GenealogMapHelper.INSTANCE.annotateResult(input, tuple);
            return tuple;
        } else {
            return new SynTempTupleGL(type);
        }
    }
}
