package mappers;

import customkeys.MatrixWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by alabdullahwi on 4/21/2015.
 */

public class BatchGradientDescent {

    public static class BGDMapper extends Mapper<Text, Text, Text, MatrixWritable> {

        static int d;
        static int m;
        static int n;
        static double[][] V;
        static double[][] U;
        static double[][] O;
        static int row;
        static int col;
        static double val;
        static Text valOut = new Text();
        static Text keyOut = new Text();

        public void setup(Mapper.Context context) throws IOException, InterruptedException {
            d = Integer.valueOf(context.getConfiguration().get("d"));
            m = Integer.valueOf(context.getConfiguration().get("m"));
            n = Integer.valueOf(context.getConfiguration().get("n"));
            U = new double[m][d];
            V = new double[d][n];
            O = new double[m][n];
        }

        public void map(Text _key, Text _value, Mapper.Context context) throws IOException, InterruptedException {

            String[] value  = _value.toString().split("\t");

            String matrixID = value[2];
            row      = Integer.valueOf(_key.toString());
            col      = Integer.valueOf(value[0]);
            val      = Double.valueOf(value[1]);

            //populate U and V models
            if ("V".equals(matrixID)) {
                V[row-1][col-1] = val;
            }
            else if ("U".equals(matrixID)) {
                U[row-1][col-1] = val;
            }
            else {
                O[row-1][col-1] = val;
            }
        }//map


        public void cleanup(Context context) throws IOException, InterruptedException {

            //propagate U and V cells to all concerned parties
            MatrixWritable _U = new MatrixWritable("U", U);
            MatrixWritable _V = new MatrixWritable("V", V);
            MatrixWritable _O = new MatrixWritable("O", O);

            //send U keys
            for (int r =0 ; r< m ; r++) {
                for ( int s = 0 ; s < d ; s++){
                    keyOut.set("U"+"\t"+r+"\t"+s);
                    context.write(keyOut, _V);
                    context.write(keyOut, _U.row(r));
                    context.write(keyOut, _O.row(r));
                }
            }
            //send V keys
            for (int r = 0 ; r < d; r++) {
                for ( int s = 0 ; s < n ; s++ ) {
                    keyOut.set("V"+"\t"+r+"\t"+s);
                    context.write(keyOut, _U);
                    context.write(keyOut, _V.col(r));
                    context.write(keyOut, _O.col(r));
                }
            }
        }
    }

    public static class BGDReducer extends Reducer<Text, MatrixWritable, Text, Text> {

        static Text keyOut;
        static Text valOut;

        public void reduce (Text _key, Iterable<MatrixWritable> _vals, Context context) throws IOException, InterruptedException {
            String[] key = _key.toString().split("\t");
            String matrixId = key[0];
            int cellRow = Integer.valueOf(key[1]);
            int cellCol = Integer.valueOf(key[2]);
/*            if ("U".equals(matrixId)) {
                for (MatrixWritable _val: _vals) {
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
                }*/
                double calculationResult = calculate(matrixId,cellRow,cellCol, _vals);
                assert calculationResult > -1;
                keyOut.set(cellRow+"\t"+cellCol+"\t"+calculationResult);
                valOut.set(matrixId);
                context.write(keyOut, valOut);
        }  //reduce

       //assume MatrixId is only U for now
        public double calculate(String matrixId, int cellRow, int cellCol, Iterable<MatrixWritable> matrices) {

            MatrixWritable O = null;
            MatrixWritable V = null;
            MatrixWritable U = null;

            for (MatrixWritable matrix : matrices) {
                String _matrixId = matrix.getId();
                if ("U".equals(_matrixId)) {
                    U = matrix;
                }
                else if  ("V".equals(_matrixId)) {
                    V = matrix;
                }
                else if ("O".equals(_matrixId)) {
                    O = matrix;
                }
            }

            double denominator = 0;
            double numerator   = 0;

            if ("U".equals(matrixId)) {
                double[][] v = V.getMatrix();
                double[] uRow = U.getMatrix()[0];
                double[] oRow = O.getMatrix()[0];

                //first calculate denominator
                for (int i = 0 ; i < v[cellRow].length ; i++) {
                    if (oRow[cellCol] != 0 ) {
                        denominator += v[cellRow][i] * v[cellRow][i] ;
                    }
                }

                //now calculate the summation at the numerator
                double kSum = 0;
                for (int j=0 ; j < oRow.length ; j++ ) {
                    if (oRow[j] != 0) {
                        for (int k = 0; k < uRow.length; k++) {
                            if (k != cellRow) {
                                double matrixVal = uRow[k];
                                double otherMatrixVal = v[k][j];
                                double cellProduct = matrixVal * otherMatrixVal;
                                kSum += cellProduct;
                            }
                        }
                        double Mrj = oRow[j];
                        numerator += Mrj - kSum;
                    }
                }
                return numerator/denominator;
            }
            else if ("V".equals(matrixId))
             {
                 double[][] u =  U.getMatrix();
                 double[] vCol = V.getMatrix()[0];
                 double[] oCol = O.getMatrix()[0];
                //first calculate denominator
                 for (int i = 0 ; i < oCol.length ; i++) {
                     if (oCol[i] != 0 ) {
                         denominator += u[i][cellRow] * u[i][cellRow];
                     }
                 }
                //now calculate the summation at the numerator
                double kSum = 0;
                for (int i=0 ; i < oCol.length ; i++ ) {
                    if (oCol[i] != 0) {
                        for (int k = 0; k < vCol.length; k++) {
                            if (k != cellRow) {
                                kSum += u[i][k] * u[k][cellCol];
                            }
                        }
                        numerator += oCol[cellRow] - kSum;
                    }
                }
                return numerator/denominator;
            }
            return -1;
        }
    }//reduce wrapper
}


