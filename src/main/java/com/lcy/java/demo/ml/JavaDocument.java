package com.lcy.java.demo.ml;

import java.io.Serializable;

/**
 * Unlabeled instance type, Spark SQL can infer schema from Java Beans.
 * 未标记的实例类型，Spark SQL可以从Java Bean推断模式。
 */
@SuppressWarnings("serial")
public class JavaDocument implements Serializable {

    private long id;
    private String text;

    public JavaDocument(long id, String text) {
        this.id = id;
        this.text = text;
    }

    public long getId() {
        return this.id;
    }

    public String getText() {
        return this.text;
    }
}
