// A-margin_transactions #5 — 直接跑「真正的 Scala CSV 解析器」,不做任何推測。
//
// 為什麼要這支:TradingReader 用的是 util/QuantlibCSVReader(它在 tototoshi scala-csv
// 之上多了兩條規則:含 `""` 但無 `,""` 的整列跳過、整列先 replace("=","")),
// 空欄位、`="0050"` 前綴、代號補空白這幾種邊界只能實跑才知道結果。本檔用 Java
// 直接呼叫同一顆 CSVParser + 同兩條規則,印出每一格與「現行 reader 的列過濾」收不收。
//
// 編譯與執行(從 repo 根;jar 取自 sbt 已下載的 classpath):
//   JAR=$(find target/bg-jobs -name "scala-csv_2.13-1.3.6.jar" | head -1)
//   SLIB=$(find target/bg-jobs -name "scala-library-2.13.15.jar" | head -1)
//   OUT=$(mktemp -d)
//   javac -cp "$JAR:$SLIB" -d "$OUT" docs/data_audit/scripts/A-margin_transactions/ScalaParserProbe.java
//   java -cp "$JAR:$SLIB:$OUT" ScalaParserProbe
//
// 實測結果(2026-07-22):
//   tpex/2007/2007_1_10.csv 5346 -> 20 欄, idx18=[] idx19=[451]
//       → readMarginTransactions 讀 values(18) 得空字串 → offsetting 寫 0(DB 現值 451 是舊版程式留下的)
//   tpex/2011/2011_10_11.csv -> 443 列符合 20 欄,但 reader 只收 2 列(代號是 "1336  ")
//   tpex/2014/2014_10_30.csv -> 收 1/572 ;2014_10_31.csv -> 收 571/572(空白版型結束於 10-30)
//   twse/2015/2015_1_5.csv   -> 收 881/883(`="0050"` 被 replace("=") 救回,正常)

import com.github.tototoshi.csv.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;

public class ScalaParserProbe {

    private static scala.collection.immutable.List<String> parse(String raw, DefaultCSVFormat fmt) {
        // QuantlibCSVReader.readNext 的兩條規則
        if (raw.contains("\"\"") && !raw.contains(",\"\"")) return null;
        scala.Option<scala.collection.immutable.List<String>> r =
            CSVParser.parse(raw.replace("=", ""), fmt.escapeChar(), fmt.delimiter(), fmt.quoteChar());
        return r.isEmpty() ? null : r.get();
    }

    /** 印出某檔某代號被解析成什麼(逐格)。 */
    static void showRow(String path, String code, int want) throws Exception {
        DefaultCSVFormat fmt = com.github.tototoshi.csv.package$.MODULE$.defaultCSVFormat();
        for (String raw : Files.readAllLines(Paths.get(path), Charset.forName("Big5-HKSCS"))) {
            scala.collection.immutable.List<String> l = parse(raw, fmt);
            if (l == null || l.isEmpty() || l.size() != want) continue;
            if (!l.head().trim().startsWith(code)) continue;
            StringBuilder sb = new StringBuilder();
            scala.collection.Iterator<String> it = l.iterator();
            int i = 0;
            while (it.hasNext()) sb.append(i++).append(":[").append(it.next()).append("] ");
            System.out.println("ROW  " + path + " size=" + l.size() + " => " + sb);
            return;
        }
        System.out.println("ROW  " + path + " code " + code + " not found");
    }

    /** 現行 reader 的列過濾(row.size==want && row.head.matches("[0-9][0-9A-Z]*"))會收下幾列。 */
    static void showFilter(String path, int want) throws Exception {
        DefaultCSVFormat fmt = com.github.tototoshi.csv.package$.MODULE$.defaultCSVFormat();
        int sizeOk = 0, accepted = 0;
        String firstRejected = null;
        for (String raw : Files.readAllLines(Paths.get(path), Charset.forName("Big5-HKSCS"))) {
            scala.collection.immutable.List<String> l = parse(raw, fmt);
            if (l == null || l.isEmpty() || l.size() != want) continue;
            sizeOk++;
            if (l.head().matches("[0-9][0-9A-Z]*")) accepted++;
            else if (firstRejected == null) firstRejected = "[" + l.head() + "]";
        }
        System.out.println("FILT " + path + "  size" + want + "_rows=" + sizeOk
                           + "  reader_accepts=" + accepted + "  firstRejectedHead=" + firstRejected);
    }

    public static void main(String[] a) throws Exception {
        showRow("data/margin_transactions/tpex/2007/2007_1_10.csv", "5346", 20);   // era A
        showRow("data/margin_transactions/tpex/2007/2007_6_5.csv", "5346", 20);    // era B
        showRow("data/margin_transactions/tpex/2026/2026_7_15.csv", "5439", 20);   // era C
        showRow("data/margin_transactions/twse/2026/2026_7_15.csv", "0050", 17);   // ="0050"
        System.out.println();
        showFilter("data/margin_transactions/tpex/2010/2010_10_1.csv", 20);
        showFilter("data/margin_transactions/tpex/2011/2011_10_11.csv", 20);
        showFilter("data/margin_transactions/tpex/2013/2013_3_15.csv", 20);
        showFilter("data/margin_transactions/tpex/2014/2014_10_30.csv", 20);
        showFilter("data/margin_transactions/tpex/2014/2014_10_31.csv", 20);
        showFilter("data/margin_transactions/tpex/2026/2026_7_15.csv", 20);
        showFilter("data/margin_transactions/twse/2015/2015_1_5.csv", 17);
    }
}
