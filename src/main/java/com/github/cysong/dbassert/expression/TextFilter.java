package com.github.cysong.dbassert.expression;

import com.github.cysong.dbassert.utitls.Utils;

/**
 * @author cysong
 * @date 2022/8/23 10:45
 **/
public class TextFilter extends AbstractFilter {
    private String expression;

    public static TextFilter create(String expression) {
        return new TextFilter(expression);
    }

    protected TextFilter(String expression) {
        assert Utils.isNotBlank(expression);
        this.expression = expression;
    }

    public String getExpression() {
        return expression;
    }
}
