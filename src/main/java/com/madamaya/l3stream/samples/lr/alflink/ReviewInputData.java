package com.madamaya.l3stream.samples.lr.alflink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class ReviewInputData {
    private long stimulus;

    private String productId;
    private double score;
    private String reviewText;
    private long reviewTime = -1L;

    public ReviewInputData(String productId, double score, String reviewText) {
        this.productId = productId;
        this.score = score;
        this.reviewText = reviewText;
    }

    public ReviewInputData(String productId, double score, String reviewText, long reviewTime) {
        this.productId = productId;
        this.score = score;
        this.reviewText = reviewText;
        this.reviewTime = reviewTime;
    }

    public ReviewInputData(ObjectNode inputJson) {
        throw new UnsupportedOperationException("RID");
    }

    public long getStimulus() {
        return stimulus;
    }

    public String getProductId() {
        return productId;
    }

    public double getScore() {
        return score;
    }

    public String getReviewText() {
        return reviewText;
    }

    public long getReviewTime() {
        return reviewTime;
    }

    public void setStimulus(long stimulus) {
        this.stimulus = stimulus;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public void setReviewText(String reviewText) {
        this.reviewText = reviewText;
    }

    public void setReviewTime(long reviewTime) {
        this.reviewTime = reviewTime;
    }

    public boolean validate() {
        if (productId == null || reviewText == null)
            return false;
        if (productId.length() <= 0 || reviewText.length() <= 0)
            return false;
        if (score < -0.001 || score >= 5.001)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ReviewInputData{" +
                "productId='" + productId + '\'' +
                ", score=" + score + '\'' +
                ", reviewText='" + reviewText.replace("\n", " ") + '\'' +
                ", reviewTime=" + reviewTime +
                '}';
    }

    public static boolean predictProduct(ReviewInputData value, PredictType pt, int subtaskId) {
        // AI
        if (pt.getType() == 0) {
            String reviewData = value.getReviewText();
            List<Double> serverResult = sendComment(reviewData, subtaskId);
            boolean serverPred = (serverResult != null && serverResult.get(1) < 0.1);
            return serverPred;
        } else if (pt.getType() == 1) {
            return value.getScore() >= 2.01;
        } else {
            throw new IllegalStateException("predictProduct: pt.getType()=" + pt.getType());
        }
    }

    protected static List<Double> sendComment(String comment, int subtaskId) {
        long port = 5000 + (subtaskId % 2);
        String ipAddress;
        /*if (subtaskId >= 0 && subtaskId < 4) {
            ipAddress = "172.16.0.131";
        } else*/
        if (subtaskId >= 0 && subtaskId < 2) {
            ipAddress = "172.16.0.94";
        } else if (subtaskId >= 2 && subtaskId < 4) {
            ipAddress = "172.16.0.69";
        } else {
            throw new IllegalStateException();
        }
        String url = "http://" + ipAddress + ":" + port + "/model/predict";
        /*
        long port = 5000 + subtaskId;
        String url = "http://172.16.0.69:" + port + "/model/predict";
         */
        List<Double> ret = null;
        try {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("accept", "application/json");
            con.setRequestProperty("Content-Type", "application/json");

            con.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());

            wr.writeBytes("{ \"text\": [ \"" + comment.replace("\n", " ").replace("\\", "\\\\").replace("\"", "\\\"") + "\" ]}");
            wr.flush();
            wr.close();

            int responseCode = con.getResponseCode();

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            con.disconnect();

            ret = convertJson(response.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ret;
    }

    protected static List<Double> convertJson(String response) {
        JsonNode node = null;
        try {
            node = new ObjectMapper().readTree(response);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (node.get("status").textValue().equals("ok")) {
            String pred = node.get("predictions").toString().replace("[", "").replace("]", "");
            JsonNode predNode = null;
            try {
                predNode = new ObjectMapper().readTree(pred);
            } catch (IOException e) {
                e.printStackTrace();
            }
            double positive = predNode.get("positive").doubleValue();
            double negative = predNode.get("negative").doubleValue();
            List<Double> ret = new ArrayList<>(2);
            ret.add(positive);
            ret.add(negative);
            return ret;
        } else {
            List<Double> ret = new ArrayList<>(2);
            ret.add(0.0);
            ret.add(0.0);
            return ret;
        }
    }

    public static String shortenComment(String comment) {
        if (comment == null || comment == "")
            return "";
        String retComment = "";
        String[] patitionedComment = comment.split("\\.");
        for (int i = 0; i < patitionedComment.length; i++) {
            if (retComment.length() < 15 && patitionedComment[i].length() > 0) {
                retComment = retComment + patitionedComment[i];
            }
        }
        return retComment;
    }
}
