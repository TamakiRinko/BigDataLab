package example;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> valueIterator = values.iterator();
        valueIterator.next();               // 跳过第一个
        String price = key.getpPrice();
        String name = key.getpName();
        while (valueIterator.hasNext()){
            valueIterator.next();
            // key会变化！虽然因为compareTo发送来了，但其实每次key还是会变化
            key.setpName(name);
            key.setpPrice(price);
            context.write(key, NullWritable.get());
        }
    }
}
