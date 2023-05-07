import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PaymentDriver {
    public final static void main(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Payment join");

        job.setJarByClass(PaymentDriver.class);
        job.setReducerClass(PaymentReducer.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, JoinMapperFines.class);

        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, JoinMapperPayments.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}