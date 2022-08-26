package com.github.cysong.dbassert.report;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * build html table
 * Uses:
 * HTMLTableBuilder htmlBuilder = new HTMLTableBuilder(null, true, 2, 3);
 * htmlBuilder.addTableHeader("1H", "2H", "3H");
 * htmlBuilder.addRowValues("1", "2", "3");
 * htmlBuilder.addRowValues("4", "5", "6");
 * htmlBuilder.addRowValues("9", "8", "7");
 * String table = htmlBuilder.build();
 * System.out.println(table.toString());
 *
 * @author cysong
 * @date 2022/8/26 10:39
 **/
public class HtmlTableBuilder {
    private static final Logger log = LoggerFactory.getLogger(HtmlTableBuilder.class);
    public static String HTML_START = "<html>";
    public static String HTML_END = "</html>";
    public static String TABLE_START_BORDER = "<table border=\"1\">";
    public static String TABLE_START = "<table>";
    public static String TABLE_END = "</table>";
    public static String HEADER_START = "<th>";
    public static String HEADER_END = "</th>";
    public static String ROW_START = "<tr>";
    public static String ROW_END = "</tr>";
    public static String COLUMN_START = "<td>";
    public static String COLUMN_END = "</td>";
    public static String HEAD = "<head>\n" +
            "\t<style type=\"text/css\">\n" +
            "\ttable, th, td {\n" +
            "\t    padding: 3px;\n" +
            "\t    border: 1px solid black;\n" +
            "\t}\n" +
            "\ttd {\n" +
            "\t    display: table-cell;\n" +
            "\t    text-align: center;\n" +
            "\t}\n" +
            "\tth {\n" +
            "\t    display: table-cell;\n" +
            "\t    vertical-align: inherit;\n" +
            "\t    font-weight: bold;\n" +
            "\t    text-align: -internal-center;\n" +
            "\t}\n" +
            "\ttable {\n" +
            "\t    margin-top: 12px;\n" +
            "\t    border-collapse: collapse;\n" +
            "\t    border-spacing: 0px;\n" +
            "\t}\n" +
            "\t</style>\n" +
            "</head>\n";
    private final StringBuilder table = new StringBuilder();
    private int columns;

    /**
     * @param header
     */
    public HtmlTableBuilder(String header) {
        table.append(HTML_START).append("\n");
        table.append(HEAD);
        table.append("<body>").append("\n");
        if (header != null) {
            table.append("<div><b>");
            table.append(header);
            table.append("</b></div>").append("\n");
        }
        table.append(TABLE_START).append("\n");
        table.append(TABLE_END).append("\n");
        table.append("</body>").append("\n");
        table.append(HTML_END);
    }

    /**
     * @param values
     */
    public void addTableHeader(String... values) {
        if (columns == 0) {
            columns = values.length;
        } else {
            if (values.length != columns) {
                throw new IllegalArgumentException("Error column length,expected:" + columns);
            }
        }

        int lastIndex = table.lastIndexOf(TABLE_END);
        if (lastIndex > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append(ROW_START);
            for (String value : values) {
                sb.append(HEADER_START);
                sb.append(value);
                sb.append(HEADER_END);
            }
            sb.append(ROW_END).append("\n");
            table.insert(lastIndex, sb.toString());
        }
    }

    /**
     * @param values
     */
    public void addRowValues(String bgColor, String... values) {
        if (columns == 0) {
            columns = values.length;
        } else {
            if (values.length != columns) {
                throw new IllegalArgumentException("Error column length,expected:" + columns);
            }
        }

        int lastIndex = table.lastIndexOf(TABLE_END);
        if (lastIndex > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("<tr bgcolor=\"%s\">", bgColor));
            for (String value : values) {
                sb.append(COLUMN_START);
                sb.append(value);
                sb.append(COLUMN_END);
            }
            sb.append(ROW_END).append("\n");
            table.insert(lastIndex, sb.toString());
        }
    }

    /**
     * @return
     */
    public String build() {
        return table.toString();
    }

}