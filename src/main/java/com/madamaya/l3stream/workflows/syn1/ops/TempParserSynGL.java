package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.syn1.objects.SynTempTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputStringGL;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;

public class TempParserSynGL extends RichMapFunction<KafkaInputStringGL, SynTempTupleGL> {
    private static final Pattern delimiter = Pattern.compile(",");
    long start;
    long count;
    ExperimentSettings settings;

    public TempParserSynGL(ExperimentSettings settings) {
        this.settings = settings;
    }

    @Override
    public SynTempTupleGL map(KafkaInputStringGL input) throws Exception {
        String inputStr = input.getStr();
        String[] elements = delimiter.split(inputStr);
        int type = Integer.parseInt(elements[0]);
        count++;
        if (type == 0) {
            SynTempTupleGL tuple = new SynTempTupleGL(
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
            GenealogMapHelper.INSTANCE.annotateResult(input, tuple);
            return tuple;
        } else {
            SynTempTupleGL tuple = new SynTempTupleGL(type);
            GenealogMapHelper.INSTANCE.annotateResult(input, tuple);
            return tuple;
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

        String dataPath = L3Config.L3_HOME + "/data/output/throughput/" + settings.getQueryName();
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
