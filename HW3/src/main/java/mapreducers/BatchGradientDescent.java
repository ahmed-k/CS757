package mapreducers;

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
        }

        public void map(Text _key, Text _value, Mapper.Context context) throws IOException, InterruptedException {

            String[] value  = _value.toString().split("\t");

            String matrixID = value[2];
            row      = Integer.valueOf(_key.toString());
            col      = Integer.valueOf(value[0]);
            val      = Double.valueOf(value[1]);

            //populate U and V models
            if ("V".equals(matrixID)) {
                if (V == null) {
                    V = new double [d][n];
                }
                V[row-1][col-1] = val;
            }
            else if ("U".equals(matrixID)) {
                if (U == null) {
                    U = new double[m][d];
                }
                U[row-1][col-1] = val;
            }
            else {
                if (O == null) {
                    O = new double[m][n];
                }
                O[row-1][col-1] = val;
            }
        }//map


        public void cleanup(Context context) throws IOException, InterruptedException {

            //propagate U and V cells to all concerned parties
            MatrixWritable _U = (U == null) ? null :  new MatrixWritable("U", U);
            MatrixWritable _V = (V == null) ? null :  new MatrixWritable("V", V);
            MatrixWritable _O = (O == null) ? null :  new MatrixWritable("O", O);

            //send U keys
            for (int r =0 ; r< m ; r++) {
                for ( int s = 0 ; s < d ; s++){
                    keyOut.set("U"+"\t"+r+"\t"+s);
                    if (V != null) {
                        context.write(keyOut, _V);
                    }
                    if (U != null) {
                        context.write(keyOut, _U.row(r));
                    }
                    if ( O!= null) {
                        context.write(keyOut, _O.row(r));
                    }
                }
            }
            //send V keys
            for (int r = 0 ; r < d; r++) {
                for ( int s = 0 ; s < n ; s++ ) {
                    keyOut.set("V"+"\t"+r+"\t"+s);
                    if (U != null) {
                        context.write(keyOut, _U);
                    }
                    if (V != null) {
                        context.write(keyOut, _V.col(r));
                    }
                    if ( O != null) {
                        context.write(keyOut, _O.col(r));
                    }
                }
            }
        }
    }

    public static class BGDReducer extends Reducer<Text, MatrixWritable, Text, Text> {

        static Text keyOut = new Text();
        static Text valOut = new Text();

        public void reduce (Text _key, Iterable<MatrixWritable> _vals, Context context) throws IOException, InterruptedException {
            String[] key = _key.toString().split("\t");
            String matrixId = key[0];
            int cellRow = Integer.valueOf(key[1]);
            int cellCol = Integer.valueOf(key[2]);
                double calculationResult = calculate(matrixId,cellRow,cellCol, _vals);
                assert calculationResult > -1;
                keyOut.set((cellRow+1)+"\t"+(cellCol+1)+"\t"+calculationResult);
                valOut.set(matrixId);
                context.write(keyOut, valOut);
        }  //reduce

        public double calculate(String matrixId, int cellRow, int cellCol, Iterable<MatrixWritable> matrices) {

            MatrixWritable O = null;
            MatrixWritable V = null;
            MatrixWritable U = null;

            for (MatrixWritable matrix : matrices) {
                String _matrixId = matrix.getId();
                if ("U".equals(_matrixId)) {
                    U = new MatrixWritable("U", matrix.getMatrix());
                }
                else if  ("V".equals(_matrixId)) {
                    V = new MatrixWritable("V", matrix.getMatrix());
                }
                else if ("O".equals(_matrixId)) {
                    O = new MatrixWritable("O", matrix.getMatrix());
                }
            }

            double denominator = 0;
            double numerator   = 0;

            if ("U".equals(matrixId)) {
                double[][] v = V.getMatrix();
                double[] uRow = U.getMatrix()[0];
                double[] oRow = O.getMatrix()[0];

                //first calculate denominator
                    for (int i = 0; i < v[cellCol].length; i++) {
                        try {
                            if (oRow[i] != 0) {
                                denominator += v[cellCol][i] * v[cellCol][i];
                            }
                        }
                        catch(ArrayIndexOutOfBoundsException arx) {
                            throw new ArrayIndexOutOfBoundsException("MatrixID: " + matrixId +
                                    " cellRow: " + cellRow + " cellCol : " + cellCol +
                                    " URow: " + uRow.length + " i: " + i +
                                    " U: " + U.toString() +
                                    " V: " + V.toString() +
                                    " O: " + O.toString()
                            );
                        }
                }
                //now calculate the summation at the numerator
                for (int j=0 ; j < oRow.length ; j++ ) {
                    if (oRow[j] != 0) {
                    double kSum = 0;
                        for (int k = 0; k < uRow.length; k++) {
                            if (k != cellCol) {
                                try {
                                    kSum += uRow[k] * v[k][j];
                                }
                                catch(ArrayIndexOutOfBoundsException arx) {
                                    throw new ArrayIndexOutOfBoundsException("MatrixID: "+ matrixId +
                                            " cellRow: " + cellRow + " cellCol : " + cellCol +
                                            " URow: " + uRow.length + " k: " + k + " j: " + j  +
                                            " U: "+ U.toString() +
                                            " V: "+ V.toString() +
                                            " O: "+ O.toString()
                                    );
                                }
                            }
                        }
                        numerator += v[cellCol][j] * (oRow[j] - kSum);
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
                for (int i=0 ; i < oCol.length ; i++ ) {
                    if (oCol[i] != 0) {
                        double kSum = 0;
                        for (int k = 0; k < vCol.length; k++) {
                            if (k != cellRow) {
                                kSum += u[i][k] * vCol[k];
                            }
                        }
                        numerator += u[i][cellRow] * (oCol[i] - kSum);
                    }
                }
                return numerator/denominator;
            }
            return -1;
        }
    }//reduce wrapper
}


