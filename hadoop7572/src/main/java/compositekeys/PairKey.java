package compositekeys;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairKey implements WritableComparable<PairKey> {

    private Integer lowID;
    private Integer highID;



    public PairKey() {

        this.lowID  = null;
        this.highID = null;

    }

    public PairKey(Integer one, Integer two) {
        //should be impossible
        if (one.equals(two)) {
            throw new IllegalArgumentException("Cannot have a pair key with identical IDs");
        }
        if (one < two) {
            lowID = one;
            highID = two;
        }
        else {
            lowID = two;
            highID = one;
        }
    }

    public Integer getLowID() {
        return lowID;
    }

    public Integer getHighID() {
        return highID;
    }



    public void setLowID(Integer _lowID) {
        lowID = _lowID;
    }

    public void setHighID(Integer _highID) {
        highID = _highID;
    }


    public int compareTo(PairKey other) {
        int _lowCompare = lowID.compareTo(other.getLowID());
        if (_lowCompare != 0) {
            return _lowCompare;
        }

        int _highCompare = highID.compareTo(other.getHighID());
        return _highCompare;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(lowID.intValue());
        dataOutput.writeInt(highID.intValue());
    }

    public void readFields(DataInput dataInput) throws IOException {
        lowID = new Integer(dataInput.readInt());
        highID = new Integer(dataInput.readInt());
    }

    @Override
    public String toString() {
        return "<" + lowID + ", " + highID + ">";
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if ( o == null || this.getClass() != o.getClass()) {
            return false;
        }

        PairKey other = (PairKey) o;

        //compare fields
        if (this.lowID != null ?    this.lowID.equals(other.getLowID()) == false  : other.getLowID() != null) return false;
        if (this.highID != null ?   this.highID.equals(other.getHighID()) == false : other.getHighID() != null) return false;

        return true;
    }


    @Override
    public int hashCode() {
        int _lowHash = this.lowID.hashCode();
        int _highHash = this.highID.hashCode();
        return 163 * (_lowHash ) + _highHash;
    }
}