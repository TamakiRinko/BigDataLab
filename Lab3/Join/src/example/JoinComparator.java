package example;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinComparator extends WritableComparator {
    protected JoinComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 分组
        OrderBean o1 = (OrderBean)a;
        OrderBean o2 = (OrderBean)b;
        return o1.getpId().compareTo(o2.getpId());
    }
}
