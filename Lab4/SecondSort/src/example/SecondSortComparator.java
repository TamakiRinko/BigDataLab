package example;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondSortComparator extends WritableComparator {
    protected SecondSortComparator() {
        super(IntPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 分组
        IntPair o1 = (IntPair)a;
        IntPair o2 = (IntPair)b;
        return o1.getFirst() - o2.getFirst();
    }

}
