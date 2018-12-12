import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String inPath = "/user/rocks1/15307130288/input/test/testdata.xml";
//        /user/rocks1/15307130288/input/data/news_sohusite_xml.dat
        String tmpPath = "/user/rocks1/15307130288/output/tmp/01";
        String outPath = "/user/rocks1/15307130288/output/test/";
        if (args.length >= 2) {
            inPath = args[1];
        }
        Path in = new Path(inPath);
        Path tmp = new Path(tmpPath);
        Path out = new Path(outPath);

        boolean res = false;

        out.getFileSystem(conf).delete(out, true);
        tmp.getFileSystem(conf).delete(tmp, true);

        conf.set("charsetName", "UTF-8");

        Job job = Job.getInstance(conf, "count test job");
        job.setJarByClass(Main.class);
        job.setMapperClass(TriGramCount.TriGramMapper.class);
        job.setReducerClass(TriGramCount.TriGramReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setInputFormatClass(XMLInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        XMLInputFormat.addInputPath(job, in);
        TextOutputFormat.setOutputPath(job, tmp);

        res = job.waitForCompletion(true);
        if(!res) System.exit(1);

        conf = new Configuration();
        Job job2 = Job.getInstance(conf, "freq test job");
        job2.setJarByClass(Main.class);
        job2.setMapperClass(GramFreq.FreqMapper.class);
        job2.setReducerClass(GramFreq.FreqReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(MapWritable.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        KeyValueTextInputFormat.addInputPath(job2, tmp);
        TextOutputFormat.setOutputPath(job2, out);

        res = job2.waitForCompletion(true);

//        tmp.getFileSystem(conf).delete(tmp, true);

        System.exit(res ? 0 : 1);
    }
}
