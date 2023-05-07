import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class JoinDriver {
    public final static void main(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Multiple join");

        job.setJarByClass(JoinDriver.class);
        job.setReducerClass(JoinReducer.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, JoinMapperInspections.class);

        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, JoinMapperFines.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}