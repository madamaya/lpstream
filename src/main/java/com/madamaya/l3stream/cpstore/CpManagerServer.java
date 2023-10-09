package com.madamaya.l3stream.cpstore;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.JobID;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CpManagerServer {
    public static void main(String[] args) throws Exception {

        CpManagementConfig config = CpManagementConfig.newInstance(args);
        createDir(config);

        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.bind(new InetSocketAddress(config.getIp(), config.getPort()));

            System.out.println("Waiting - JobID");
            config.setJobID(getJobID(serverSocket));
            System.out.println(config.getJobID());

            while (true) {
                System.out.println("Waiting - CPID");
                retainCheckpoint(config, getCpID(serverSocket));
            }

        } catch (Exception e) {
            System.err.println(e);
        }
    }

    private static JobID getJobID(ServerSocket serverSocket) throws IOException {
        Socket socket = serverSocket.accept();
        BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String jobIDstr = br.readLine().replace("\n", "");
        br.close();
        socket.close();

        return JobID.fromHexString(jobIDstr);
    }

    private static long getCpID(ServerSocket serverSocket) throws IOException {
        Socket socket = serverSocket.accept();
        BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String checkpointIDstr = br.readLine().replace("\n", "");
        br.close();
        socket.close();

        // Validation
        String rstr = "^[0-9]+$";
        Pattern p = Pattern.compile(rstr);
        Matcher m = p.matcher(checkpointIDstr);
        if (m.matches()) {
            return Long.parseLong(checkpointIDstr);
        } else {
            return -2;
        }
    }

    private static void createDir(CpManagementConfig config) throws Exception {
        try {
            Files.createDirectory(Path.of(config.getCheckpointDir() + "/_checkpoints"));
        } catch (FileAlreadyExistsException e) {
            System.out.println("'_checkpoints' dir is already existed.");
        } catch (Exception e) {
            System.err.println(e);
            // throw new Exception("createDir");
        }
    }

    private static void retainCheckpoint(CpManagementConfig config, long checkpointId) throws Exception {

        System.out.println("checkpointID = " + checkpointId);
        if (checkpointId == -2)
            return;

        String jobPath = config.getCheckpointDir() + "/" + config.getJobID();
        String pJobPath = config.getCheckpointDir() + "/_checkpoints/" + config.getJobID();

        if (checkpointId == 1) {
            try {
                Files.createDirectory(Path.of(pJobPath));
            } catch (Exception e) {
                System.err.println(e);
                throw new Exception("retainCheckpoint1");
            }
        }

        String jobCpIdPath = jobPath + "/chk-" + checkpointId;
        String pJobCpIdPath = pJobPath + "/chk-" + checkpointId;
        try {
            Files.createDirectory(Path.of(pJobCpIdPath));
        } catch (Exception e) {
            System.err.println(e);
            throw new Exception("retainCheckpoint2");
        }

        // ディレクトリ下に chk-i のハードリンク作成
        createLink(jobCpIdPath, pJobCpIdPath);
    }

    private static void createLink(String target, String newlink) throws Exception {
        File[] cpFiles = new File(target).listFiles();
        if (cpFiles.length > 0) {
            for (int i = 0; i < cpFiles.length; i++) {
                try {
                    Files.createLink(Path.of(newlink + "/" + cpFiles[i].getName()), Path.of(target + "/" + cpFiles[i].getName()));
                } catch (Exception e) {
                    System.err.println(e);
                    throw new Exception("createLink");
                }
            }
        }
    }
}
