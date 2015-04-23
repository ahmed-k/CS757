package mappers;
import org.apache.hadoop.io.DoubleWritable;
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

            //V matrix is needed everywhere we want to predict a U cell
            if ("V".equals(matrixID)) {
                V[row][col] = val;
            }
            else if ("U".equals(matrixID)) {
                U[row][col] = val;
            }
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

    public static class BGDReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        static double[] matrixVector;
        static double[] originalMatrixVector;
        static double[][] otherMatrix;

        public void reduce (Text _key, Iterable<Text> _vals, Context context) throws IOException, InterruptedException {
            String[] key = _key.toString().split("\t");
            String matrixId = key[0];
            int cellRow = Integer.valueOf(key[1]);
            int cellCol = Integer.valueOf(key[2]);
            if ("U".equals(matrixId)) {
                originalMatrixVector = new double[m];
               matrixVector = U[cellRow];
               otherMatrix = V;
                for (Text _val: _vals) {
                   String[] vals = _val.toString().split("\t");
                    int vCol = Integer.valueOf(vals[2]);
                    double vVal = Double.valueOf(vals[3]);
                    originalMatrixVector[vCol] = vVal;
                }
            }
            else if ("U".equals(matrixId)) {
                matrixVector = V[cellCol];
                originalMatrixVector = new double[n];
                otherMatrix = U;
                for (Text _val: _vals) {
                    String[] vals = _val.toString().split("\t");
                    int vRow = Integer.valueOf(vals[1]);
                    double vVal = Double.valueOf(vals[3]);
                    originalMatrixVector[vRow] = vVal;
                }
            }
        }  //reduce

       //assume MatrixId is only U for now
        public DoubleWritable calculate(String matrixId, int cellRow, int cellCol) {

            //first calculate Vsj
            double vsj = 0;
            for (double _vsj : otherMatrix[cellCol]){
                //there exists a rating
                if (originalMatrixVector[cellCol] != 0) {
                    vsj += _vsj*_vsj;
                }
            }

            return null;
        }
    }//reduce wrapper
}


