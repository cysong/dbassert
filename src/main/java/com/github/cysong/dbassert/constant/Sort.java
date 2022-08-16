package com.github.cysong.dbassert.constant;

import com.github.cysong.dbassert.expression.Order;
import com.github.cysong.dbassert.utitls.Utils;

public class Sort {
    private String orderBy;
    private Order order;

    public static Sort create(String orderBy) {
        assert Utils.isNotBlank(orderBy);
        return Sort.create(orderBy, Order.ASC);
    }

    public static Sort create(String orderBy, Order order) {
        return new Sort(orderBy, order == null ? Order.ASC : order);
    }

    public Sort(String orderBy, Order order) {
        this.orderBy = orderBy;
        this.order = order;
    }

    public String getOrderBy() {
        return orderBy;
    }

    public Order getOrder() {
        return order;
    }
}
