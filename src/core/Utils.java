import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Utils {
    private static final String illegal = "[^\\u4e00-\\u9fa5\\pP+~$`^=|<>～｀＄＾＋＝｜＜＞￥×]+";
    private static final String punct = "[\\pP+~$`^=|<>～｀＄＾＋＝｜＜＞￥×]+";

    static public String filter(String text) throws UnsupportedEncodingException {
        String res;
        res = text.replaceAll(illegal, "");
        res = res.replaceAll(punct, " ");
        return res;
    }

    static public String[] splitPunct(String str) {
        return str.split(" ");
    }

    public static class Vocab{
        public static class Values {
            Long freq = null;
            Double prob = null;
        }

        public Map<String, Values> bin = new HashMap<String, Values>();
        public long nWords = 0;
        private final static double backoffScale = -Math.log(0.4);

        public double calcProb(String str) {
            final int length = str.length();
            if(!bin.containsKey(str)) {
                // need smooth to avoid zero
                // use back-off smooth
                if (length <= 1) return Math.log(nWords);
                String base = str.substring(1, length);
                return calcProb(base) + backoffScale;
            }
            Values val = bin.get(str);
            if(val.prob != null) return val.prob;
            else {
                if (length <= 1) {
                    long count = val.freq;
                    val.prob = Math.log(nWords) - Math.log(count);
                } else {
                    String base = str.substring(0, length - 1);
                    long baseCount = bin.get(base).freq;
                    long count = val.freq;
                    val.prob = Math.log(baseCount) - Math.log(count);
                }
            }
            return val.prob;
        }

        public void add(String follow, Long count) {
            if(bin.containsKey(follow)) {
                Values val = bin.get(follow);
                val.freq += count;
                val.prob = null;
                bin.put(follow, val);
            } else {
                Values val = new Values();
                val.freq = count;
                bin.put(follow, val);
            }
        }

        public void add(String key, Long count, Double prob) {
            Values val = new Values();
            val.freq = count;
            val.prob = prob;
            bin.put(key, val);
        }
    }
}
