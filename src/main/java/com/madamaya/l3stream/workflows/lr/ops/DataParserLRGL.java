package com.madamaya.l3stream.workflows.lr.ops;

import com.madamaya.l3stream.glCommons.ObjectNodeGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import io.palyvos.provenance.usecases.linearroad.noprovenance.LinearRoadInputTuple;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadInputTupleGL;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.regex.Pattern;

public class DataParserLRGL implements MapFunction<ObjectNodeGL, LinearRoadInputTupleGL> {
    private static final Pattern delimiter = Pattern.compile(",");

    @Override
    public LinearRoadInputTupleGL map(ObjectNodeGL jNodeGL) throws Exception {
        ObjectNode jNode = jNodeGL.getObjectNode();

        String line = jNode.get("value").textValue();
        String[] elements = delimiter.split(line.trim());
        LinearRoadInputTupleGL out = new LinearRoadInputTupleGL(
                Integer.valueOf(elements[0]),
                Long.valueOf(elements[1]),
                Integer.valueOf(elements[2]),
                Integer.valueOf(elements[3]),
                Integer.valueOf(elements[4]),
                Integer.valueOf(elements[5]),
                Integer.valueOf(elements[6]),
                Integer.valueOf(elements[7]),
                Integer.valueOf(elements[8]),
                jNodeGL.getStimulus()
        );
        out.setKey(String.valueOf(out.getVid()));

        GenealogMapHelper.INSTANCE.annotateResult(jNodeGL, out);
        return out;
    }
}
