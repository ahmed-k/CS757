package partitioners;

import compositekeys.PairKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Ahmed Alabdullah on 3/17/15.
 */
public class PairPartitioner extends Partitioner<PairKey, IntWritable> {


    @Override
    public int getPartition(PairKey key, IntWritable value, int numOfPartitions) {
        return key.getLowID().hashCode() % numOfPartitions;
    }


}
