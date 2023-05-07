import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PaymentReducer extends Reducer<Text, Text, NullWritable, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String fee = "";
        double payment = 0;
        for(Text value : values) {
            if (value.toString().startsWith("fee")) {
                fee = value.toString().replace("fee:", "");
            } else {
                payment += Double.parseDouble(value.toString().replace("payment:", ""));
            }
        }
        if (!fee.isEmpty()) {
            String merge = key + "," + fee + "," + payment;
            context.write(NullWritable.get(), new Text(merge));
        }
    }
}