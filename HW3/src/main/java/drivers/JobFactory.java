package drivers;

import customkeys.MatrixVectorWritable;
import customkeys.MatrixWritable;
import mapreducers.BatchGradientDescent;
import mapreducers.BooleanCollapser;
import mapreducers.IterationController;
import mapreducers.MatrixMultiplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by alabdullahwi on 4/25/2015.
 */
public class JobFactory {

    public Job configureMultiplyJob(Configuration conf, String[] args) throws IOException {
        Job job = new Job(conf, "matrix multiplication");
        job.setJarByClass(AssignmentDriver.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(MatrixMultiplier.MultiplierMapper.class);
        job.setReducerClass(MatrixMultiplier.MultiplierReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MatrixVectorWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job;
    }

    public Job configureBGDJob(Configuration conf , String[] args ) throws IOException {
        Job job = new Job(conf, "batch gradient descent");
        job.setJarByClass(AssignmentDriver.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(BatchGradientDescent.BGDMapper.class);
        job.setReducerClass(BatchGradientDescent.BGDReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MatrixWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job;
    }
    public Job createIterationJob() throws IOException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "iteration determination");
        job.setJarByClass(AssignmentDriver.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(IterationController.IterationMapper.class);
        job.setReducerClass(IterationController.IterationReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MatrixWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("output/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("output/iteration"));
        return job;
    }


    public Job createCollapseJob() throws IOException {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "collapse booleans into single answer");
        job.setJarByClass(AssignmentDriver.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(BooleanCollapser.CollapserMapper.class);
        job.setReducerClass(BooleanCollapser.CollapserReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BooleanWritable.class);
        job.setOutputKeyClass(BooleanWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path("output/iteration/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("output/collapse"));
        return job;


    }

}
