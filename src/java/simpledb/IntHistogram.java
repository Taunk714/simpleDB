package simpledb;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int max;
    private int min;
    private int[] histogramList;
    private double bucketSize;
    private int tupleNum = 0;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.max = max;
        this.min = min;
        this.histogramList = new int[buckets];
        bucketSize = (double) (max - min + 1) / buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = (int) ((v - this.min)/this.bucketSize);
        histogramList[index]++;
        tupleNum++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        double res = 0;
        int index = (int) ((v - this.min)/this.bucketSize);
        if (op.equals(Predicate.Op.EQUALS)){
            if (v < this.min || v > this.max){
                return 0.0;
            }else{
                return (histogramList[index] / Math.ceil(bucketSize))/ tupleNum;
            }
        }else if (op.equals(Predicate.Op.GREATER_THAN)){
            if (v < this.min){
                return 1.0;
            }else if (v > this.max){
                return 0.0;
            }
            res += histogramList[index] * (1-(v - (v - min) / bucketSize * bucketSize - min)/Math.ceil(bucketSize));
            for (int i = index + 1; i < histogramList.length; i++){
                res += histogramList[i];
            }
            return res/tupleNum;
        }else if (op.equals(Predicate.Op.LESS_THAN)){
            if (v < this.min){
                return 0.0;
            }else if (v > this.max){
                return 1.0;
            }
            res += histogramList[index] * (v - (v - min) / bucketSize * bucketSize - min)/Math.ceil(bucketSize);
            for (int i = index - 1; i > -1; i--){
                res += histogramList[i];
            }
            return res/tupleNum;
        }else if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)){
            if (v <= this.min){
                return 1.0;
            }else if (v > this.max){
                return 0.0;
            }
            res += histogramList[index] * (1-(v - (v - min) / bucketSize * bucketSize - min)/Math.ceil(bucketSize));
            for (int i = index + 1; i < histogramList.length; i++){
                res += histogramList[i];
            }
            res += histogramList[index] / Math.ceil(bucketSize);
            return res/tupleNum;
        }else if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)){
            if (v>= this.max){
                return 1.0;
            }else if (v < this.min){
                return 0.0;
            }
            res += histogramList[index] * (v - (v - min) / bucketSize * bucketSize - min)/Math.ceil(bucketSize);
            for (int i = index - 1; i > -1; i--){
                res += histogramList[i];
            }
            res += histogramList[index] / Math.ceil(bucketSize);
            return res/tupleNum;
        }else if (op.equals(Predicate.Op.NOT_EQUALS)){
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }

        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return histogramList.length + "buckets, maximum is "+ max + ", minimum is " + min;
    }
}
