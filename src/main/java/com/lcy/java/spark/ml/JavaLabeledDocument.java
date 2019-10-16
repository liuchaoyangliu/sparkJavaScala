package com.lcy.java.spark.ml;

import java.io.Serializable;

/**
 * Labeled instance type, Spark SQL can infer schema from Java Beans.
 * 标签实例类型，Spark SQL可以从Java Bean推断出模式。
 */
@SuppressWarnings("serial")
public class JavaLabeledDocument extends JavaDocument implements Serializable {
    
    private double label;
    
    public JavaLabeledDocument(long id, String text, double label) {
        super(id, text);
        this.label = label;
    }
    
    public double getLabel() {
        return this.label;
    }
}
