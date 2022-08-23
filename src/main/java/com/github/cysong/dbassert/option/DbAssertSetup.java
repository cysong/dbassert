package com.github.cysong.dbassert.option;

/**
 * global options setup method
 *
 * @author cysong
 * @date 2022/08/22 15:50
 **/
public class DbAssertSetup {

    public static DbAssertOptions setup() {
        return DbAssertOptions.getGlobal();
    }
}
