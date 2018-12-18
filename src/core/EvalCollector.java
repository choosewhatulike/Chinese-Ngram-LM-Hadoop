import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EvalCollector {
    public static class MyMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final static IntWritable one = new IntWritable(1);
        private final static Text out = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString();
            String[] vals = val.split("\t");
            if(vals.length > 2) {
                out.set(vals[1] + "\t" + vals[2]);
                context.write(one, out);
            }
        }
    }

    public static class MyReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalProb = 0.0;
            long nums = 0;
            for(Text value : values) {
                String[] vals = value.toString().split("\t");
                totalProb += Double.parseDouble(vals[0]);
                nums += Long.parseLong(vals[1]);
            }
            context.write(new Text("Perplexity"), new DoubleWritable(totalProb / nums));
        }
    }
}
