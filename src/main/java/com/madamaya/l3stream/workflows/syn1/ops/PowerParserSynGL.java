package com.madamaya.l3stream.workflows.syn1.ops;

import com.madamaya.l3stream.conf.L3Config;
import com.madamaya.l3stream.workflows.syn1.objects.SynPowerTupleGL;
import io.palyvos.provenance.genealog.GenealogMapHelper;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputStringGL;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;

public class PowerParserSynGL extends RichMapFunction<KafkaInputStringGL, SynPowerTupleGL> {
    private static final Pattern delimiter = Pattern.compile(",");
    long start;
    long count;
    ExperimentSettings settings;

    public PowerParserSynGL(ExperimentSettings settings) {
        this.settings = settings;
    }

    @Override
    public SynPowerTupleGL map(KafkaInputStringGL input) throws Exception {
        String inputStr = input.getStr();
        String[] elements = delimiter.split(inputStr);
        int type = Integer.parseInt(elements[0]);
        count++;
        if (type == 0) {
            SynPowerTupleGL tuple = new SynPowerTupleGL(type);
            GenealogMapHelper.INSTANCE.annotateResult(input, tuple);
            return tuple;
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

        PrintWriter pw = new PrintWriter(dataPath + "/" + settings.getStartTime() + "_" + 1 + "_" + getRuntimeContext().getIndexOfThisSubtask() + "_" + settings.getDataSize() + ".log");
        pw.println(start + "," + end + "," + (end - start) + "," + count);
        pw.flush();
        pw.close();
        super.close();
    }
}
