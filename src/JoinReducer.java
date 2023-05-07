import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class JoinReducer extends Reducer<Text, HashMap<String,String>, NullWritable, Text> {
    public void reduce(Text key, Iterable<HashMap<String,String>> values, Context context)
            throws IOException, InterruptedException {
        HashMap<String, String> inspections = new HashMap<>();
        HashMap<String, String> fines = new HashMap<>();
        for(HashMap<String, String> value : values) {
            if(value.containsKey("Business Name")) {
                inspections = value;
            } else {
                fines = value;
            }
        }
        String merge = key + "," + inspections.toString() + "," + fines.toString();
        context.write(NullWritable.get(), new Text(merge));
    }
}