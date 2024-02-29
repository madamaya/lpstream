package com.madamaya.l3stream.workflows.utils.objects;

import java.util.Comparator;
import java.util.PriorityQueue;

public class MedianCalc {
    private PriorityQueue<Long> leftHeap;
    private PriorityQueue<Long> rightHeap;
    private long dataNum;

    public MedianCalc() {
        this.leftHeap = new PriorityQueue<>(Comparator.reverseOrder());
        this.rightHeap = new PriorityQueue<>(Comparator.naturalOrder());
        this.dataNum = 0;
    }

    public void append(long val) {
        /* Corner case (dataNum == 0) */
        if (dataNum == 0) {
            this.leftHeap.add(val);
            dataNum++;
        /* Corner case (dataNum == 1) */
        } else if (dataNum == 1) {
            if (leftHeap.peek() <= val) {
                this.rightHeap.add(val);
            } else {
                long tmp = this.leftHeap.poll();
                this.leftHeap.add(val);
                this.rightHeap.add(tmp);
            }
            dataNum++;
        /* General case (dataNum >= 2) */
        } else if (dataNum >= 2) {
            /* Decide the position into which val should be inserted. */
            int caseFlag;
            long left = leftHeap.peek();
            long right = rightHeap.peek();
            if (left <= val && val <= right) {
                caseFlag = 0;
            } else if (val < left) {
                caseFlag = -1;
            } else { // right < val
                caseFlag = 1;
            }

            /* Insert */
            if (leftHeap.size() == rightHeap.size()) {
                if (caseFlag == 0) {
                    leftHeap.add(val);
                } else if (caseFlag == -1) {
                    leftHeap.add(val);
                } else {
                    rightHeap.add(val);
                }
            } else if (leftHeap.size() + 1 == rightHeap.size()) {
                if (caseFlag == 0) {
                    leftHeap.add(val);
                } else if (caseFlag == -1) {
                    leftHeap.add(val);
                } else {
                    long tmp = rightHeap.poll();
                    leftHeap.add(tmp);
                    rightHeap.add(val);
                }
            } else if (leftHeap.size() == rightHeap.size() + 1) {
                if (caseFlag == 0) {
                    rightHeap.add(val);
                } else if (caseFlag == -1) {
                    long tmp = leftHeap.poll();
                    leftHeap.add(val);
                    rightHeap.add(tmp);
                } else {
                    rightHeap.add(val);
                }
            } else {
                throw new IllegalStateException();
            }
            dataNum++;
        } else {
            throw new IllegalStateException();
        }
    }

    public double getMedian() {
        /* Error cases */
        if (dataNum == 0) {
            throw new IllegalStateException("dataNum == 0");
        }
        if (Math.abs(leftHeap.size() - rightHeap.size()) >= 2) {
            throw new IllegalStateException("abs(leftHeap.size - rightHeap.size) >= 2");
        }

        /* Return result */
        if (dataNum % 2 == 0) {
            return ((double) leftHeap.peek() + rightHeap.peek()) / 2;
        } else {
            if (leftHeap.size() > rightHeap.size()) {
                return leftHeap.peek();
            } else {
                return rightHeap.peek();
            }
        }
    }

    public long getDataNum() {
        return this.dataNum;
    }
}
