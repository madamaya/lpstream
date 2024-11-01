package com.madamaya.l3stream.workflows.lr.ops;

import com.madamaya.l3stream.conf.L3Config;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputStringGL;
import io.palyvos.provenance.usecases.linearroad.provenance.LinearRoadInputTupleGL;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;

public class DataParserLRGL extends RichMapFunction<KafkaInputStringGL, LinearRoadInputTupleGL> {
    private static final Pattern delimiter = Pattern.compile(",");
    long start;
    long count;
    ExperimentSettings settings;

    public DataParserLRGL(ExperimentSettings settings) {
        this.settings = settings;
    }

    @Override
    public LinearRoadInputTupleGL map(KafkaInputStringGL input) throws Exception {
        String inputStr = input.getStr();
        String line = inputStr.substring(1, inputStr.length() - 1).trim();
        String[] elements = delimiter.split(line);
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
                elements[15],
                input.getDominantOpTime(),
                input.getKafkaAppandTime(),
                input.getStimulus()
        );
        out.setKey(String.valueOf(out.getVid()));
        GenealogMapHelper.INSTANCE.annotateResult(input, out);
        count++;
        return out;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        start = System.nanoTime();
        count = 0L;
    }

    @Override
    public void close() throws Exception {
        long end = System.nanoTime();

        String dataPath = L3Config.L3_HOME + "/data/output/throughput/" + settings.getQueryName();
        if (Files.notExists(Paths.get(dataPath))) {
            Files.createDirectories(Paths.get(dataPath));
        }

        PrintWriter pw = new PrintWriter(dataPath + "/" + settings.getStartTime() + "_" + getRuntimeContext().getIndexOfThisSubtask() + "_" + settings.getDataSize() + ".log");
        pw.println(start + "," + end + "," + (end - start) + "," + count);
        pw.flush();
        pw.close();
        super.close();
    }
}
