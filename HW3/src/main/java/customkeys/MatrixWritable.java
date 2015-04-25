package customkeys;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatrixWritable implements WritableComparable<MatrixWritable> {

    private String id;
    private double[][] matrix;

    public MatrixWritable() {
        id = null;
        matrix = null;
    }

    public MatrixWritable(String id, int dim1, int dim2) {
        this.id = id;
        matrix = new double[dim1][dim2];
    }

    public MatrixWritable(String id, double[][] matrix) {
        this.id = id ;
        this.matrix = matrix;
    }

    public MatrixWritable row(int rowNum) {
        int rowLength = matrix[rowNum].length;
        double[][] retv = new double[1][rowLength];
        for (int i = 0 ; i < rowLength; i++){
            retv[0][i] = matrix[rowNum][i];
        }
        return new MatrixWritable(id, retv);
    }

    public MatrixWritable col(int colNum) {
        int colLength = matrix.length;
        double[][] retv = new double[1][colLength];
        for (int i = 0 ; i < colLength; i++){
            retv[0][i] = matrix[i][colNum];
        }
        return new MatrixWritable(id, retv);
    }


    public String getId() {
        return id;
    }
    public double[][] getMatrix() {
        return matrix;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeInt(matrix.length);
        for (int i = 0 ; i < matrix.length ; i++) {
            dataOutput.writeInt(matrix[i].length);
        }
        for (int i = 0 ; i < matrix.length ; i++ ){
            for (int j = 0 ; j < matrix[i].length ; j ++ ) {
                dataOutput.writeDouble(matrix[i][j]);
            }
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readUTF();
        matrix = new double[dataInput.readInt()][];
        for (int i = 0 ; i < matrix.length ; i++ ) {
            matrix[i] = new double[dataInput.readInt()];
        }
        for (int i = 0 ; i < matrix.length ; i++ ) {
            for (int j = 0 ; j< matrix[i].length ; j++) {
                matrix[i][j] = dataInput.readDouble();
            }
        }
    }

    @Override
    public String toString() {
        String retv = "mx"+ id + "\t ";
        for (int  k = 0; k < matrix[0].length ; k++) {
            retv += (k+"\t");
        }
        retv +="\n";
        for (int i = 0 ; i < matrix.length ; i++ ) {

            retv += (i +"\t\t");
            for (int j = 0 ; j < matrix[i].length ; j++ ) {
                retv += matrix[i][j] + "\t";
            }
            retv+="\n";
        }
        retv+="\n";
        return retv;
    }

    @Override
    public boolean equals(Object o) {
        boolean retv = true;
        if (this == o) {
            return true;
        }
        if ( o == null || this.getClass() != o.getClass()) {
            return false;
        }
        MatrixWritable other = (MatrixWritable) o;
        //compare fields
        for (int i = 0 ; i < matrix.length ; i++ ) {
            for (int j = 0; j < matrix[i].length; j++) {
                retv &= (matrix[i][j] == other.cell(i,j));
            }
        }
        return retv;
    }


    public double cell(int dim1, int dim2) {
        return matrix[dim1][dim2];
    }

    @Override
    public int hashCode() {
        int _matrixHash = matrix.hashCode();
        int _idHash = this.id.hashCode();
        return 163 * (_matrixHash ) + _idHash;
    }

    public int compareTo(MatrixWritable o) {
        return 0;
    }
}