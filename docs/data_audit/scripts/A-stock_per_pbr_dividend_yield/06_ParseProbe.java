import com.github.tototoshi.csv.*;
import scala.Option;
import scala.collection.immutable.List;
public class AperpbrParseProbe {
  static void probe(String label, String line) {
    Option<List<String>> r = CSVParser.parse(line, '\\', ',', '"');
    if (r.isEmpty()) { System.out.println(label + " -> None"); return; }
    List<String> l = r.get();
    System.out.print(label + " -> size=" + l.size() + " [");
    for (int i = 0; i < l.size(); i++) System.out.print("(" + i + ")='" + l.apply(i) + "' ");
    System.out.println("]");
  }
  public static void main(String[] a) {
    probe("era1(2005-2017/04) TWSE", "\"1101\",\"台泥\",\"16.92\",\"5.91\",\"1.07\",\r\n");
    probe("era2(2017/05-2024/06) TWSE", "\"1101\",\"台泥\",\"4.12\",\"105\",\"20.47\",\"1.22\",\"105/4\",\r\n");
    probe("era3(2024/07-) TWSE", "\"1101\",\"台泥\",\"33.55\",\"2.98\",\"112\",\"30.23\",\"1.09\",\"113/1\",\r\n");
    probe("tpexA", "\"1333\",\"恩得利\",\"27.83\",\"0.00000000\",\"108\",\"8.80\",\"2.02\"\r\n");
    probe("tpexB", "\"1240\",\"茂生農經        \",\"13.64\",\"3.40000000\",\"113\",\"5.74\",\"1.69\",\"113Q4\"\r\n");
  }
}
