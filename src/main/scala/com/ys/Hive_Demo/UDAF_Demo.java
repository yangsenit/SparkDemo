package com.ys.Hive_Demo;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

/**
 * Created by Administrator on 2016/8/7.
 */
public class UDAF_Demo extends UDAF {
   public static class Evaluator extends GenericUDAFEvaluator {
       @Override
       public AggregationBuffer getNewAggregationBuffer() throws HiveException {
           return null;
       }

       @Override
       public void reset(AggregationBuffer aggregationBuffer) throws HiveException {

       }

       @Override
       public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {

       }

       @Override
       public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
           return null;
       }

       @Override
       public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {

       }

       @Override
       public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
           return null;
       }
   }
}


