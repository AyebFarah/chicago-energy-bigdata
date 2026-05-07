package chicago.energy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job2_EnergyStarCount {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println(
                    "Usage: Job2_EnergyStarCount <input> <output>"
            );
            System.exit(1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,
                "Buildings with ENERGY STAR Score per Year");

        job.setJarByClass(Job2_EnergyStarCount.class);

        job.setMapperClass(EnergyStarMapper.class);
        job.setReducerClass(EnergyStarReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}