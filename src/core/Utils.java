import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class Utils {
    static public String filter(String text) throws UnsupportedEncodingException {
        String res = text.replaceAll(punct, " ");
        return res.replaceAll(" +", " ");
    }

    private static final String punct = "[^(\\u4e00-\\u9fa5)]+";
//    private static final String punct = "[\\pP+~$`^=|<>～｀＄＾＋＝｜＜＞￥×]";

    static public String[] splitPunct(String str) {
        return str.split(" ");
    }
}
