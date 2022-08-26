package com.github.cysong.dbassert;

import com.github.cysong.dbassert.report.HtmlTableBuilder;
import org.testng.annotations.Test;

/**
 * HtmlTableBuilder testcase
 *
 * @author cysong
 * @date 2022/8/26 11:12
 **/
public class HtmlTableBuilderTest {

    @Test
    public void testTable() {
        HtmlTableBuilder htmlBuilder = new HtmlTableBuilder("test table");
        htmlBuilder.addTableHeader("1H", "2H", "3H");
        htmlBuilder.addRowValues("red", "1", "2", "3");
        htmlBuilder.addRowValues("yellow", "4", "5", "6");
        htmlBuilder.addRowValues("#5FFF33", "9", "8", "7");
        String table = htmlBuilder.build();
        System.out.println(table);
    }

}
