import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class Eval {
    public static int hash(String str, int n) {
        Character c = str.charAt(str.length() - 1);
        return (c.hashCode() ^ (c.hashCode() << 8) * 103) % n;
    }

    public static final String vocabPathName = "vocabPathName";
    public static final String binNumName = "binNum";

    public static class EvalMapper extends Mapper<Object, Text, IntWritable, Text> {
        private int binNum;
        private int N;
        private final static IntWritable resKey = new IntWritable();
        private final static Text resVal = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            binNum = context.getConfiguration().getInt(binNumName, 10);
            N = context.getConfiguration().getInt(NGramCount.CountMapper.Num, 3);
        }

        private void emitWord(String text, Context context) throws IOException, InterruptedException {
            resKey.set(hash(text, binNum));
            resVal.set(text);
            context.write(resKey, resVal);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString();
            val = Utils.filter(val);
            String text = val.replaceAll(" +", "");
            final int length = text.length();
            for(int i = 1; i <= length && i <= N; ++i) {
                emitWord(text.substring(0, i), context);
            }
            for(int i = 1; i + N <= length; ++i) {
                emitWord(text.substring(i, i + N), context);
            }
        }
    }

    public static class EvalReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {
        private Utils.Vocab vocab = null;
        private int N;

        private double totalProb;
        private long nums;
        private int binNum;

        public Utils.Vocab readVocab(Context context, int hashNum) throws IOException {
            Configuration conf = context.getConfiguration();
            String path = conf.get(vocabPathName);
            Path dirPath = new Path(path);
            FileSystem fs = dirPath.getFileSystem(conf);
            Utils.Vocab vocab = new Utils.Vocab();
            for(FileStatus k : fs.globStatus(new Path(path + "*-00000"))) {
                Path inpath = k.getPath();
                FSDataInputStream stream = fs.open(inpath);
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] vals = line.split("\t");
                    if(vals.length == 3) {
                        if(vals[0].length() == 1) vocab.nWords++;
                        if(hash(vals[0], binNum) == hashNum) {
                            vocab.add(vals[0], null, Double.parseDouble(vals[2]));
                        }
                    }
                    context.progress();
                }
                reader.close();
                stream.close();
            }
            return vocab;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            totalProb = 0.0;
            nums = 0;
            vocab = null;
            N = context.getConfiguration().getInt(NGramCount.CountMapper.Num, 3);
            binNum =  context.getConfiguration().getInt(binNumName, 10);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            vocab = readVocab(context, key.get());

            for(Text value : values) {
                String text = value.toString();
                double logit = vocab.calcProb(text);
                totalProb += logit;
                nums++;
                context.progress();
            }
            vocab = null;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("probResult\t" + totalProb + "\t" + nums), new DoubleWritable(totalProb / nums));
        }
    }
}
