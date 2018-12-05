import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestXMLReader {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String inPath = "/user/rocks1/15307130288/input/test/testdata.xml";
        String outPath = "/user/rocks1/15307130288/output/test/";
        if (args.length >= 1) {
            inPath = args[1];
        }
        Path in = new Path(inPath);
        Path out = new Path(outPath);
        out.getFileSystem(conf).delete(out, true);

        Job job = Job.getInstance(conf, "fileintputformat test job");
        job.setJarByClass(TestXMLReader.class);
        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(XMLInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        XMLInputFormat.addInputPath(job, in);
        TextOutputFormat.setOutputPath(job, out);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
