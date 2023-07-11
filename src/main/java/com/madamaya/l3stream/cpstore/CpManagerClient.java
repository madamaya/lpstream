package com.madamaya.l3stream.cpstore;

import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

public class CpManagerClient extends RichMapFunction<ObjectNode, ObjectNode> implements CheckpointListener {
    ExperimentSettings settings;

    public CpManagerClient(ExperimentSettings settings) {
        this.settings = settings;
    }

    @Override
    public ObjectNode map(ObjectNode jsonNodes) throws Exception {
        return null;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        sendMessage(settings, getRuntimeContext().getJobId().toHexString());
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
        sendMessage(settings, Long.toString(l));
    }

    private void sendMessage(ExperimentSettings settings, String message) throws Exception {
        try(Socket socket = new Socket()) {
            // 接続開始
            socket.connect(new InetSocketAddress(settings.getCpMServerIP(), settings.getCpMServerPort()));

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            bw.write(message);
            bw.close();
        } catch (Exception e) {
            System.err.println(e);
            throw new Exception("CpManager: sendMessage");
        }
    }
}
