package example;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair> {
    private int first;
    private int second;
    public IntPair(){

    }

    public IntPair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(IntPair o) {
        if (first != o.first)
            return first < o.first ? -1 : 1;
        else if (second != o.second)
            return second > o.second ? -1 : 1;
        else
            return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(first);
        dataOutput.writeInt(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readInt();
        second = dataInput.readInt();
    }

    @Override
    public String toString() {
        return String.valueOf(first) + '\t' + second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }
}
