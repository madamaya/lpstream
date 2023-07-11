package com.madamaya.l3stream.samples.lr.alflink;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;

public class Params implements Serializable {
    private int type;
    private String commentLen;
    private int reasonSize;
    private long sleepTime;
    private long windowSize;

    public Params() {
    }

    public Params(int type, String commentLen, int reasonSize, long sleepTime, long windowSize) {
        this.type = type;
        this.commentLen = commentLen;
        this.reasonSize = reasonSize;
        this.sleepTime = sleepTime;
        this.windowSize = windowSize;
    }

    private static String readFile(String filePath) {
        String line = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(filePath)));
            line = br.readLine();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return line;
    }

    public static Params newInstance(String filePath) {
        String line = readFile(filePath);
        String[] params = line.split(",");
        if (params.length != 5) {
            String msg = "ERROR: " + " " + filePath;
            for (int i = 0; i < params.length; i++) {
                msg += (params[i] + ", ");
            }
            throw new UnsupportedOperationException(msg);
        }
        int type = Integer.parseInt(params[0]);
        if (type != 0 && type != 1 && type != 2 && type != 5) {
            throw new IllegalStateException("params[0] = " + type);
        }
        String commentLen = params[1];
        if (!commentLen.equals("-short") && !commentLen.equals("-long")) {
            throw new IllegalStateException("params[1] = " + commentLen);
        }
        int reasonSize = Integer.parseInt(params[2]);
        if (reasonSize != 1 && reasonSize != 10 && reasonSize != 75 && reasonSize != 100 && reasonSize != 1000 && reasonSize != 10000 && reasonSize != 99999) {
            throw new IllegalStateException("params[2] = " + reasonSize);
        }

        // type, reasonSize, sleepTime, windowSize
        return new Params(type, commentLen, reasonSize, Long.parseLong(params[3]), Long.parseLong(params[4]));
    }

    public int getType() {
        return type;
    }

    public String getCommentLen() {
        return commentLen;
    }

    public int getReasonSize() {
        return reasonSize;
    }

    public long getSleepTime() {
        return sleepTime;
    }

    public long getWindowSize() {
        return windowSize;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setCommentLen(String commentLen) {
        this.commentLen = commentLen;
    }

    public void setReasonSize(int reasonSize) {
        this.reasonSize = reasonSize;
    }

    public void setSleepTime(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    public void setWindowSize(long windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public String toString() {
        return "Params{" +
                "type=" + type +
                ", commentLen='" + commentLen + '\'' +
                ", reasonSize=" + reasonSize +
                ", sleepTime=" + sleepTime +
                ", windowSize=" + windowSize +
                '}';
    }
}
