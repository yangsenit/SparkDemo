package com.ys.Hive_Demo;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by Administrator on 2016/8/7.
 */
/*user define function 需要实现udf的evaluate的函数，evaluate这个函数是可以被重载的

Hive UDF开发
Hive进行UDF开发十分简单，此处所说UDF为Temporary的function，所以需要hive版本在0.4.0以上才可以。
Hive的UDF开发只需要重构UDF类的evaluate函数即可。例：
package com.hrj.hive.udf;
import org.apache.hadoop.hive.ql.exec.UDF;
public class helloUDF extends UDF {
    public String evaluate(String str) {
        try {
            return "HelloWorld " + str;
        } catch (Exception e) {
            return null;
        }
    }
}
将该java文件编译成helloudf.jar
hive> add jar helloudf.jar;
hive> create temporary function helloworld as 'com.hrj.hive.udf.helloUDF';  //helloworld  映射到 自己定的helloUDF需要写完整的包名，直到函数

hive> select helloworld(t.col1) from t limit 10;
hive> drop temporary function helloworld;
注：
1.helloworld为临时的函数，所以每次进入hive都需要add jar以及create temporary操作
2.UDF只能实现一进一出的操作，如果需要实现多进一出，则需要实现UDAF
 */
public class UDF_Add extends UDF {
    public Integer evaluate(Integer a,Integer b){
        if(a!=null||b!=null){
            return a+b;
        }else{
            return null;
        }
    }
}
