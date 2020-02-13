package com.lcy.java.spark.rdd.secondarySort;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * 比较器：
 * 1. 实现 Ordered<T> 和 Serializable 接口
 * 2. 重写 equals 和 hashCode 方法
 * 3. 重写 $less ，$greater ，$less$eq ，$greater$eq 方法
 * 4. 重写 compare ， compareTo 方法
 * 5. 构造函数
 */

public class SecondSort implements Ordered<SecondSort>, Serializable {

    Integer first;
    Integer second;

    public SecondSort() {
    }

    public SecondSort(Integer first, Integer second) {
        this.first = first;
        this.second = second;
    }

    /**
     * 小于
     * @param that
     * @return
     */
    public boolean $less(SecondSort that) {
        if (this.first < that.getFirst()) {
            return true;
        } else if (this.first == that.getFirst() && this.second < that.getSecond()) {
            return true;
        }
        return false;
    }

    /**
     * 大于
     * @param that
     * @return
     */
    public boolean $greater(SecondSort that) {
        if (this.first > that.getFirst()) {
                return true;
            } else if (this.first == that.getFirst() && this.second > that.getSecond()) {
                return true;
        }
        return false;
    }

    /**
     * 小于等于
     * @param that
     * @return
     */
    public boolean $less$eq(SecondSort that) {
        if (this.$less(that)) {
            return true;
        } else if (this.first == that.getFirst() && this.second == that.getSecond()) {
            return true;
        }
        return false;
    }

    /**
     * 大于等于
     * @param that
     * @return
     */
    public boolean $greater$eq(SecondSort that) {
        if (this.$greater(that)) {
            return true;
        } else if (this.first == that.getFirst() && this.second == that.getSecond()) {
            return true;
        }
        return false;
    }

    /**
     * 比较
     * @param that
     * @return
     */
    public int compare(SecondSort that) {
        if (this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        } else {
            return this.second - that.getSecond();
        }
    }

    /**
     * 比较
     * @param that
     * @return
     */
    public int compareTo(SecondSort that) {
        if (this.first - that.getFirst() != 0) {
            return this.first - that.getFirst();
        } else {
            return this.second - that.getSecond();
        }
    }

    public Integer getFirst() {
        return first;
    }

    public void setFirst(Integer first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecondSort that = (SecondSort) o;

        if (first != that.first) return false;
        return second == that.second;
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
    }

}
