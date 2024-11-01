package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.workflows.syn1.objects.SynTempTuple;
import io.palyvos.provenance.l3stream.conf.L3conf;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;

public class TempParserSyn extends RichMapFunction<KafkaInputString, SynTempTuple> {
    private static final Pattern delimiter = Pattern.compile(",");
    long start;
    long count;
    ExperimentSettings settings;

    public TempParserSyn(ExperimentSettings settings) {
        this.settings = settings;
    }

    @Override
    public SynTempTuple map(KafkaInputString input) throws Exception {
        String inputStr = input.getStr();
        String[] elements = delimiter.split(inputStr);
        int type = Integer.parseInt(elements[0]);
        count++;
        if (type == 0) {
            SynTempTuple tuple = new SynTempTuple(
                    type,
                    Integer.parseInt(elements[1]),
                    Integer.parseInt(elements[2]),
                    Double.parseDouble(elements[3]),
                    elements[4],
                    Long.parseLong(elements[5]),
                    input.getDominantOpTime(),
                    input.getKafkaAppandTime(),
                    input.getStimulus()
            );
            return tuple;
        } else {
            return new SynTempTuple(type);
        }
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

        String dataPath = L3conf.L3_HOME + "/data/output/throughput/" + settings.getQueryName();
        if (Files.notExists(Paths.get(dataPath))) {
            Files.createDirectories(Paths.get(dataPath));
        }

        PrintWriter pw = new PrintWriter(dataPath + "/" + settings.getStartTime() + "_" + 0 + "_" + getRuntimeContext().getIndexOfThisSubtask() + "_" + settings.getDataSize() + ".log");
        pw.println(start + "," + end + "," + (end - start) + "," + count);
        pw.flush();
        pw.close();
        super.close();
    }
}
