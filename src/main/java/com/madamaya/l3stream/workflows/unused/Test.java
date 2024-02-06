package com.madamaya.l3stream.workflows.unused;

import io.palyvos.provenance.util.ExperimentSettings;

public class Test {
    public static void main(String[] args) throws Exception {
        ExperimentSettings settings = ExperimentSettings.newInstance(args);
        System.out.println(settings.aggregateStrategySupplier().get());
    }
}
