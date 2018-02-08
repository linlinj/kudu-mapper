package com.linlinj.db.kudu;

import com.linlinj.db.kudu.exception.KuduOperationException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by admin on 2018/1/25.
 */
public class KuduQuery {

    private static final Object NOT_SET = new Object();
    private String key;
    private List<KuduQuery> criteriaChain;
    private Object isValue = NOT_SET;

    public KuduQuery() {this.criteriaChain = new ArrayList<KuduQuery>();}

    public KuduQuery(String key) {
        this.criteriaChain = new ArrayList<KuduQuery>();
        this.criteriaChain.add(this);
        this.key = key;
    }

    protected KuduQuery(List<KuduQuery> criteriaChain, String key) {
        this.criteriaChain = criteriaChain;
        this.criteriaChain.add(this);
        this.key = key;
    }

    public KuduQuery and(String key) {
        return new KuduQuery(this.criteriaChain, key);
    }

    public KuduQuery is(Object o) {

        if (!isValue.equals(NOT_SET)) {
            throw new KuduOperationException(
                    "Multiple 'is' values declared. You need to use 'and' with multiple criteria");
        }
        /*
        if (lastOperatorWasNot()) {
            throw new InvalidMongoDbApiUsageException("Invalid query: 'not' can't be used with 'is' - use 'ne' instead.");
        }*/

        this.isValue = o;
        return this;
    }

    public KuduQuery ne(Object o) {
        //criteria.put("$ne", o);
        return new KuduQuery(this.criteriaChain, key);
    }

    public KuduQuery lt(Object o) {
        //criteria.put("$ne", o);
        return new KuduQuery(this.criteriaChain, key);
    }

    public KuduQuery lte(Object o) {
        //criteria.put("$ne", o);
        return new KuduQuery(this.criteriaChain, key);
    }

    public KuduQuery gt(Object o) {
        return new KuduQuery(this.criteriaChain, key);
    }

    public KuduQuery gte(Object o) {
        return new KuduQuery(this.criteriaChain, key);
    }
}
