package drivers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by alabdullahwi on 4/21/2015.
 */
public class AssignmentDriver {

    public static void main(String[] args) throws Exception {
        //get args
        JobFactory jf = new JobFactory();
        Configuration conf = jf.createConfiguration(args);
        Job job;
        if (args.length == 6) {
            System.out.println("Running final matrix multplication job...");
            job = jf.configureMultiplyJob(conf, args);
            System.exit(job.waitForCompletion(true)? 0 :1);
        }
        else {
            job = jf.configureBGDJob(conf, args);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            if (job.waitForCompletion(true)) {
                System.out.println("Running conversion to boolean...");
                Job job2 = jf.createIterationJob();
                if(job2.waitForCompletion(true)) {
                    System.out.println("Collapsing booleans into single boolean...");
                    Job job3 = jf.createCollapseJob();
                    System.exit(job3.waitForCompletion(true)? 0: 1);
                }
            }
        }
    }




}
