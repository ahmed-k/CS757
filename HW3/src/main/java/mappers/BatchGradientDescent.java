package mappers;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by alabdullahwi on 4/21/2015.
 */

public class BatchGradientDescent {
    static int d;
    static int m;
    static int n;
    static double[][] V;
    static double[][] U;

    public static class BGDMapper extends Mapper<IntWritable, Text, Text, Text> {

        static int row;
        static int col;
        static double val;
        static Text valOut = new Text();
        static Text keyOut = new Text();

        public void setup(Mapper.Context context) throws IOException, InterruptedException {
            d = Integer.valueOf(context.getConfiguration().get("d"));
            m = Integer.valueOf(context.getConfiguration().get("m"));
            n = Integer.valueOf(context.getConfiguration().get("n"));
            V = new double[d][n];
            U = new double[m][d];
        }

        public void map(IntWritable _key, Text _value, Mapper.Context context) throws IOException, InterruptedException {

            String[] value  = _value.toString().split("\t");
            String matrixID = value[3];
            row      = Integer.valueOf(value[0]);
            col      = Integer.valueOf(value[1]);
            val      = Double.valueOf(value[2]);

            //populate U and V models
            if ("V".equals(matrixID)) {
                V[row][col] = val;
            }
            else if ("U".equals(matrixID)) {
                U[row][col] = val;
            }

            //original matrix row, need to populate to all reducers of U cells in the same row and all V cell reducers in the same column of this cell
            else {
                valOut.set("O\t"+row+"\t"+col+"\t"+val);
                for (int i = 0; i<m; i++) {
                    keyOut.set("V\t"+row+"\t"+i);
                    context.write(keyOut,valOut);
                }
                for (int j =0; j<n; j++) {
                    keyOut.set("U\t"+j+"\t"+col);
                    context.write(keyOut,valOut);
                }
            }
        }//map
    }

    public static class BGDReducer extends Reducer<Text, Text, Text, Text> {

        static Text keyOut;
        static Text valOut;

        public void reduce (Text _key, Iterable<Text> _vals, Context context) throws IOException, InterruptedException {
            String[] key = _key.toString().split("\t");
            String matrixId = key[0];
            int cellRow = Integer.valueOf(key[1]);
            int cellCol = Integer.valueOf(key[2]);
            if ("U".equals(matrixId)) {
               double[] originalMatrixVector = new double[m];
                for (Text _val: _vals) {
                   String[] vals = _val.toString().split("\t");
                    int vCol = Integer.valueOf(vals[2]);
                    double vVal = Double.valueOf(vals[3]);
                    originalMatrixVector[vCol] = vVal;
                }
            }
            else if ("V".equals(matrixId)) {
                double [] originalMatrixVector = new double[n];
                for (Text _val: _vals) {
                    String[] vals = _val.toString().split("\t");
                    int vRow = Integer.valueOf(vals[1]);
                    double vVal = Double.valueOf(vals[3]);
                    originalMatrixVector[vRow] = vVal;
                }
                double calculationResult = calculate(matrixId,cellRow,cellCol, originalMatrixVector);
                assert calculationResult > -1;
                keyOut.set(cellRow+"\t"+cellCol+"\t"+calculationResult);
                valOut.set(matrixId);
                context.write(keyOut, valOut);
            }
        }  //reduce

       //assume MatrixId is only U for now
        public double calculate(String matrixId, int cellRow, int cellCol, double[] originalMatrixVector) {
            double denominator = 0;
            double numerator   = 0;

            if ("U".equals(matrixId)) {
                //first calculate denominator
                for (double _vsj : V[cellCol]){
                    //there exists a rating
                    if (originalMatrixVector[cellCol] != 0) {
                        denominator += _vsj*_vsj;
                    }
                }
                //now calculate the summation at the numerator
                double kSum = 0;
                for (int j=0 ; j < n ; j++ ) {
                    if (originalMatrixVector[j] != 0) {
                        for (int k = 0; k < d; k++) {
                            if (k != cellRow) {
                                double matrixVal = U[cellRow][k];
                                double otherMatrixVal = V[k][j];
                                double cellProduct = matrixVal * otherMatrixVal;
                                kSum += cellProduct;
                            }
                        }
                        double Mrj = originalMatrixVector[j];
                        numerator += Mrj - kSum;
                    }
                }
                return numerator/denominator;
            }
            else if ("V".equals(matrixId))
             {
                //first calculate denominator
                 for (int i = 0 ; i < m ; i++) {
                     if (originalMatrixVector[i] != 0 ) {
                         denominator += U[i][cellRow] * U[i][cellRow];
                     }
                 }
                //now calculate the summation at the numerator
                double kSum = 0;
                for (int i=0 ; i < m ; i++ ) {
                    if (originalMatrixVector[i] != 0) {
                        for (int k = 0; k < d; k++) {
                            if (k != cellRow) {
                                kSum += V[i][k] * V[k][cellCol];
                            }
                        }
                        numerator += originalMatrixVector[cellRow] - kSum;
                    }
                }
                return numerator/denominator;
            }
            return -1;
        }
    }//reduce wrapper
}


