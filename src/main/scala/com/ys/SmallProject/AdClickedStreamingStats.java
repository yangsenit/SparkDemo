//package com.ys.SmallProject;
//
//
//
//        import java.sql.Connection;
//        import java.sql.DriverManager;
//        import java.sql.PreparedStatement;
//        import java.sql.ResultSet;
//        import java.sql.SQLException;
//        import java.util.ArrayList;
//        import java.util.Arrays;
//        import java.util.HashMap;
//        import java.util.HashSet;
//        import java.util.Iterator;
//        import java.util.List;
//        import java.util.Map;
//        import java.util.Set;
//        import java.util.concurrent.LinkedBlockingQueue;
//
//        import org.apache.spark.SparkConf;
//        import org.apache.spark.api.java.JavaPairRDD;
//        import org.apache.spark.api.java.JavaRDD;
//        import org.apache.spark.api.java.JavaSparkContext;
//        import org.apache.spark.api.java.function.Function;
//        import org.apache.spark.api.java.function.Function2;
//        import org.apache.spark.api.java.function.PairFunction;
//        import org.apache.spark.api.java.function.VoidFunction;
//        import org.apache.spark.sql.DataFrame;
//        import org.apache.spark.sql.Row;
//        import org.apache.spark.sql.RowFactory;
//        import org.apache.spark.sql.hive.HiveContext;
//        import org.apache.spark.sql.types.DataTypes;
//        import org.apache.spark.sql.types.StructType;
//        import org.apache.spark.streaming.Durations;
//        import org.apache.spark.streaming.api.java.JavaDStream;
//        import org.apache.spark.streaming.api.java.JavaPairDStream;
//        import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//        import org.apache.spark.streaming.api.java.JavaStreamingContext;
//        import org.apache.spark.streaming.kafka.KafkaUtils;
//
//        import com.google.common.base.Optional;
//
//        import kafka.serializer.StringDecoder;
//        import scala.Tuple2;
//
///**
// *
// * 在线处理广告点击流 广告点击的基本数据格式：timestamp、ip、userID、adID、province、city
// *
// * @author hp
// *
// */
//public class AdClickedStreamingStats {
//
//    public static void main(String[] args) {
//
//        SparkConf conf = new SparkConf().setMaster("local[5]").setAppName("AdClickedStreamingStats");
//
//        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
//
//
//        /**
//         * 创建Kafka元数据,来让Spark Streaming这个Kafka Consumer利用
//         */
//        Map<String, String> kafkaParameters = new HashMap<String, String>();
//        kafkaParameters.put("metadata.broker.list", "Master:9092,Worker1:9092,Worker2:9092");
//
//        Set<String> topics = new HashSet<String>();
//        topics.add("AdClicked");
//
//        JavaPairInputDStream<String, String> adClickedStreaming = KafkaUtils.createDirectStream(jsc, String.class,
//                String.class, StringDecoder.class, StringDecoder.class, kafkaParameters, topics);
//        /**
//         * 因为要对黑名单进行在线过滤，而数据是在RDD中的，所以必然使用transform这个函数；
//         * 但是在这里我们必须使用transformToPair，原因是读取进来的Kafka的数据是Pair<String,String>类型的,另外
//         * 一个原因是过滤后的数据要进行进一步处理，所以必须是读进来的Kafka数据的原始类型DStream<String, String>
//         *
//         * 在此：再次说明每个Batch Duration中实际上讲输入的数据就是被一个且仅仅被一个RDD封装的，你可以有多个
//         * InputDstream，但是其实在产生Job的时候，这些不同的InputDstream在Batch
//         * Duration中就相当于Spark基于 HDFS数据操作的不同文件来源而已罢了。
//         */
//        JavaPairDStream<String, String> filteredadClickedStreaming = adClickedStreaming
//                .transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
//
//                    @Override
//                    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
//                        /**
//                         * 在线黑名单过滤思路步骤： 1，从数据库中获取黑名单转换成RDD，即新的RDD实例封装黑名单数据；
//                         * 2，然后把代表黑名单的RDD的实例和Batch
//                         * Duration产生的rdd进行join操作,准确的说是进行
//                         * leftOuterJoin操作，也就是说使用Batch
//                         * Duration产生的rdd和代表黑名单的RDD的实例进行
//                         * leftOuterJoin操作，如果两者都有内容的话，就会是true，否则的话就是false；
//                         *
//                         * 我们要留下的是leftOuterJoin操作结果为false；
//                         *
//                         */
//
//                        List<String> blackListNames = new ArrayList<String>();
//                        JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
//                        jdbcWrapper.doQuery("SELECT * FROM blacklisttable", null, new ExecuteCallBack() {
//
//                            @Override
//                            public void resultCallBack(ResultSet result) throws Exception {
//
//                                while (result.next()) {
//                                    blackListNames.add(result.getString(1));
//                                }
//                            }
//
//                        });
//
//                        List<Tuple2<String, Boolean>> blackListTuple = new ArrayList<Tuple2<String, Boolean>>();
//
//                        for (String name : blackListNames) {
//                            blackListTuple.add(new Tuple2<String, Boolean>(name, true));
//                        }
//
//                        List<Tuple2<String, Boolean>> blackListFromDB = blackListTuple; // 数据来自于查询的黑名单表并且映射成为<String,
//                        // Boolean>
//
//                        JavaSparkContext jsc = new JavaSparkContext(rdd.context());
//
//                        /**
//                         * 黑名单的表中只有userID，但是如果要进行join操作的话，就必须是Key-Value，所以
//                         * 在这里我们需要基于数据表中的数据产生Key-Value类型的数据集合；
//                         */
//                        JavaPairRDD<String, Boolean> blackListRDD = jsc.parallelizePairs(blackListFromDB);
//
//                        /**
//                         * 进行操作的时候肯定是基于userID进行join的，
//                         * 所以必须把传入的rdd进行mapToPair操作转化成为符合 格式的rdd
//                         *
//                         * 广告点击的基本数据格式：timestamp、ip、userID、adID、province、city
//                         */
//
//                        JavaPairRDD<String, Tuple2<String, String>> rdd2Pair = rdd
//                                .mapToPair(new PairFunction<Tuple2<String, String>, String, Tuple2<String, String>>() {
//
//                                    @Override
//                                    public Tuple2<String, Tuple2<String, String>> call(Tuple2<String, String> t)
//                                            throws Exception {
//                                        String userID = t._2.split("\t")[2];
//                                        return new Tuple2<String, Tuple2<String, String>>(userID, t);
//                                    }
//                                });
//
//                        JavaPairRDD<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joined = rdd2Pair
//                                .leftOuterJoin(blackListRDD);
//
//                        JavaPairRDD<String, String> result = joined.filter(
//                                new Function<Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {
//
//                                    @Override
//                                    public Boolean call(Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> v1)
//                                            throws Exception {
//                                        Optional<Boolean> optional = v1._2._2;
//
//                                        if (optional.isPresent() && optional.get()) {
//                                            return false;
//                                        } else {
//                                            return true;
//                                        }
//
//                                    }
//                                }).mapToPair(
//                                new PairFunction<Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>() {
//
//                                    @Override
//                                    public Tuple2<String, String> call(
//                                            Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> t)
//                                            throws Exception {
//                                        // TODO Auto-generated method stub
//                                        return t._2._1;
//                                    }
//                                });
//
//                        return result;
//                    }
//                });
//
//		/*
//		 * 第四步：接下来就像对于RDD编程一样基于DStream进行编程！！！原因是DStream是RDD产生的模板（或者说类），在Spark
//		 * Streaming具体 发生计算前，其实质是把每个Batch的DStream的操作翻译成为对RDD的操作！！！
//		 * 对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
//		 * 广告点击的基本数据格式：timestamp、ip、userID、adID、province、city
//		 */
//
//        JavaPairDStream<String, Long> pairs = filteredadClickedStreaming
//                .mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
//
//                    @Override
//                    public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
//                        String[] splited = t._2.split("\t");
//
//                        String timestamp = splited[0]; // yyyy-MM-dd
//                        String ip = splited[1];
//                        String userID = splited[2];
//                        String adID = splited[3];
//                        String province = splited[4];
//                        String city = splited[5];
//
//                        String clickedRecord = timestamp + "_" + ip + "_" + userID + "_" + adID + "_" + province + "_"
//                                + city;
//
//                        return new Tuple2<String, Long>(clickedRecord, 1L);
//                    }
//                });
//
//		/*
//		 * 第四步：对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
//		 * 计算每个Batch Duration中每个User的广告点击量
//		 */
//        JavaPairDStream<String, Long> adClickedUsers = pairs.reduceByKey(new Function2<Long, Long, Long>() {
//
//            @Override
//            public Long call(Long v1, Long v2) throws Exception {
//                // TODO Auto-generated method stub
//                return v1 + v2;
//            }
//
//        });
//
//        /**
//         *
//         * 计算出什么叫有效的点击？ 1，复杂化的一般都是采用机器学习训练好模型直接在线进行过滤； 2，简单的？可以通过一个Batch
//         * Duration中的点击次数来判断是不是非法广告点击，但是实际上讲非法广告
//         * 点击程序会尽可能模拟真实的广告点击行为，所以通过一个Batch来判断是 不完整的，我们需要对例如一天（也可以是每一个小时）
//         * 的数据进行判断！ 3，比在线机器学习退而求次的做法如下： 例如：一段时间内，同一个IP（MAC地址）有多个用户的帐号访问；
//         * 例如：可以统一一天内一个用户点击广告的次数，如果一天点击同样的广告操作50次的话，就列入黑名单；
//         *
//         * 黑名单有一个重点的特征：动态生成！！！所以每一个Batch Duration都要考虑是否有新的黑名单加入，此时黑名单需要存储起来
//         * 具体存储在什么地方呢，存储在DB/Redis中即可；
//         *
//         * 例如邮件系统中的“黑名单”，可以采用Spark Streaming不断的监控每个用户的操作，如果用户发送邮件的频率超过了设定的值，可以
//         * 暂时把用户列入“黑名单”，从而阻止用户过度频繁的发送邮件。
//         */
//
//        JavaPairDStream<String, Long> filteredClickInBatch = adClickedUsers
//                .filter(new Function<Tuple2<String, Long>, Boolean>() {
//
//                    @Override
//                    public Boolean call(Tuple2<String, Long> v1) throws Exception {
//                        if (1 < v1._2) {
//                            // 更新一下黑名单的数据表
//                            return false;
//                        } else {
//                            return true;
//                        }
//
//                    }
//                });
//
//        // Todo。。。。
//
//		/*
//		 * 此处的print并不会直接出发Job的执行，因为现在的一切都是在Spark Streaming框架的控制之下的，对于Spark
//		 * Streaming 而言具体是否触发真正的Job运行是基于设置的Duration时间间隔的
//		 *
//		 * 诸位一定要注意的是Spark Streaming应用程序要想执行具体的Job，对Dtream就必须有output Stream操作，
//		 * output
//		 * Stream有很多类型的函数触发，类print、saveAsTextFile、saveAsHadoopFiles等，最为重要的一个
//		 * 方法是foraeachRDD,因为Spark
//		 * Streaming处理的结果一般都会放在Redis、DB、DashBoard等上面，foreachRDD
//		 * 主要就是用用来完成这些功能的，而且可以随意的自定义具体数据到底放在哪里！！！
//		 *
//		 */
//        // filteredClickInBatch.print();
//
//        filteredClickInBatch.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
//
//            @Override
//            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
//                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
//
//                    @Override
//                    public void call(Iterator<Tuple2<String, Long>> partition) throws Exception {
//                        /**
//                         * 在这里我们使用数据库连接池的高效读写数据库的方式把数据写入数据库MySQL;
//                         * 由于传入的参数是一个Iterator类型的集合，所以为了更加高效的操作我们需要批量处理
//                         * 例如说一次性插入1000条Record，使用insertBatch或者updateBatch类型的操作；
//                         * 插入的用户信息可以只包含：timestamp、ip、userID、adID、province、city
//                         * 这里面有一个问题：可能出现两条记录的Key是一样的，此时就需要更新累加操作
//                         */
//
//                        List<UserAdClicked> userAdClickedList = new ArrayList<UserAdClicked>();
//
//                        while (partition.hasNext()) {
//                            Tuple2<String, Long> record = partition.next();
//                            String[] splited = record._1.split("\t");
//
//                            UserAdClicked userClicked = new UserAdClicked();
//                            userClicked.setTimestamp(splited[0]);
//                            userClicked.setIp(splited[1]);
//                            userClicked.setUserID(splited[2]);
//                            userClicked.setAdID(splited[3]);
//                            userClicked.setProvince(splited[4]);
//                            userClicked.setCity(splited[5]);
//                            userAdClickedList.add(userClicked);
//
//                        }
//
//                        List<UserAdClicked> inserting = new ArrayList<UserAdClicked>();
//                        List<UserAdClicked> updating = new ArrayList<UserAdClicked>();
//
//                        JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
//
//                        // adclicked
//                        // 表的字段：timestamp、ip、userID、adID、province、city、clickedCount
//                        for (UserAdClicked clicked : userAdClickedList) {
//                            jdbcWrapper.doQuery(
//                                    "SELECT count(1) FROM adclicked WHERE "
//                                            + " timestamp = ? AND userID = ? AND adID = ?",
//                                    new Object[] { clicked.getTimestamp(), clicked.getUserID(), clicked.getAdID() },
//                                    new ExecuteCallBack() {
//
//                                        @Override
//                                        public void resultCallBack(ResultSet result) throws Exception {
//                                            if (result.next()) {
//                                                long count = result.getLong(1);
//                                                clicked.setClickedCount(count);
//                                                updating.add(clicked);
//                                            } else {
//                                                inserting.add(clicked);
//                                            }
//
//                                        }
//                                    });
//                        }
//                        // adclicked
//                        // 表的字段：timestamp、ip、userID、adID、province、city、clickedCount
//                        ArrayList<Object[]> insertParametersList = new ArrayList<Object[]>();
//                        for (UserAdClicked inserRecord : inserting) {
//                            insertParametersList.add(new Object[] { inserRecord.getTimestamp(), inserRecord.getIp(),
//                                    inserRecord.getUserID(), inserRecord.getAdID(), inserRecord.getProvince(),
//                                    inserRecord.getCity(), inserRecord.getClickedCount() });
//                        }
//                        jdbcWrapper.doBatch("INSERT INTO adclicked VALUES(?,?,?,?,?,?,?)", insertParametersList);
//
//                        // adclicked
//                        // 表的字段：timestamp、ip、userID、adID、province、city、clickedCount
//                        ArrayList<Object[]> updateParametersList = new ArrayList<Object[]>();
//                        for (UserAdClicked updateRecord : updating) {
//                            updateParametersList.add(new Object[] { updateRecord.getTimestamp(), updateRecord.getIp(),
//                                    updateRecord.getUserID(), updateRecord.getAdID(), updateRecord.getProvince(),
//                                    updateRecord.getCity(), updateRecord.getClickedCount() });
//                        }
//                        jdbcWrapper.doBatch("UPDATE adclicked set clickedCount = ? WHERE "
//                                + " timestamp = ? AND ip = ? AND userID = ? AND adID = ? AND province = ? "
//                                + "AND city = ? ", updateParametersList);
//
//                    }
//                });
//                return null;
//            }
//
//        });
//
//        JavaPairDStream<String, Long> blackListBasedOnHistory = filteredClickInBatch
//                .filter(new Function<Tuple2<String, Long>, Boolean>() {
//
//                    @Override
//                    public Boolean call(Tuple2<String, Long> v1) throws Exception {
//                        // 广告点击的基本数据格式：timestamp、ip、userID、adID、province、city
//                        String[] splited = v1._1.split("\t");
//
//                        String date = splited[0];
//                        String userID = splited[2];
//                        String adID = splited[3];
//
//                        /**
//                         * 接下来根据date、userID、adID等条件去查询用户点击广告的数据表，获得总的点击次数
//                         * 这个时候基于点击次数判断是否属于黑名单点击 *
//                         */
//
//                        int clickedCountTotalToday = 81;
//
//                        if (clickedCountTotalToday > 50) {
//                            return true;
//                        } else {
//                            return false;
//                        }
//
//                    }
//                });
//
//        /**
//         * 必须对黑名单的整个RDD进行去重操作！！！
//         */
//
//        JavaDStream<String> blackListuserIDtBasedOnHistory = blackListBasedOnHistory
//                .map(new Function<Tuple2<String, Long>, String>() {
//
//                    @Override
//                    public String call(Tuple2<String, Long> v1) throws Exception {
//                        // TODO Auto-generated method stub
//                        return v1._1.split("\t")[2];
//                    }
//                });
//
//        JavaDStream<String> blackListUniqueuserIDtBasedOnHistory = blackListuserIDtBasedOnHistory
//                .transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
//
//                    @Override
//                    public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
//                        // TODO Auto-generated method stub
//                        return rdd.distinct();
//                    }
//                });
//
//        // 下一步写入黑名单数据表中
//
//        blackListUniqueuserIDtBasedOnHistory.foreachRDD(new Function<JavaRDD<String>, Void>() {
//
//            @Override
//            public Void call(JavaRDD<String> rdd) throws Exception {
//                rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
//
//                    @Override
//                    public void call(Iterator<String> t) throws Exception {
//                        /**
//                         * 在这里我们使用数据库连接池的高效读写数据库的方式把数据写入数据库MySQL;
//                         * 由于传入的参数是一个Iterator类型的集合，所以为了更加高效的操作我们需要批量处理
//                         * 例如说一次性插入1000条Record，使用insertBatch或者updateBatch类型的操作；
//                         * 插入的用户信息可以只包含：useID 此时直接插入黑名单数据表即可。
//                         */
//
//                        List<Object[]> blackList = new ArrayList<Object[]>();
//
//                        while (t.hasNext()) {
//                            blackList.add(new Object[] { (Object) t.next() });
//                        }
//                        JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
//                        jdbcWrapper.doBatch("INSERT INTO blacklisttable VALUES (?) ", blackList);
//                    }
//                });
//                return null;
//            }
//        });
//
//        /**
//         * 广告点击累计动态更新,每个updateStateByKey都会在Batch Duration的时间间隔的基础上进行更高点击次数的更新，
//         * 更新之后我们一般都会持久化到外部存储设备上，在这里我们存储到MySQL数据库中；
//         */
//        JavaPairDStream<String, Long> updateStateByKeyDStream = filteredadClickedStreaming
//                .mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
//
//                    @Override
//                    public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
//                        String[] splited = t._2.split("\t");
//
//                        String timestamp = splited[0]; // yyyy-MM-dd
//                        String ip = splited[1];
//                        String userID = splited[2];
//                        String adID = splited[3];
//                        String province = splited[4];
//                        String city = splited[5];
//
//                        String clickedRecord = timestamp + "_" + adID + "_" + province + "_" + city;
//
//                        return new Tuple2<String, Long>(clickedRecord, 1L);
//                    }
//                }).updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
//
//                    @Override
//                    public Optional<Long> call(List<Long> v1, Optional<Long> v2) throws Exception {
//                        /**
//                         * v1:代表是当前的key在当前的Batch
//                         * Duration中出现次数的集合，例如{1,1,1,1,1,1} v2:代表当前key在以前的Batch
//                         * Duration中积累下来的结果；
//                         */
//                        Long clickedTotalHistory = 0L;
//                        if (v2.isPresent()) {
//                            clickedTotalHistory = v2.get();
//                        }
//
//                        for (Long one : v1) {
//                            clickedTotalHistory += one;
//                        }
//
//                        return Optional.of(clickedTotalHistory);
//                    }
//                });
//
//        updateStateByKeyDStream.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
//
//            @Override
//            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
//                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
//
//                    @Override
//                    public void call(Iterator<Tuple2<String, Long>> partition) throws Exception {
//                        /**
//                         * 在这里我们使用数据库连接池的高效读写数据库的方式把数据写入数据库MySQL;
//                         * 由于传入的参数是一个Iterator类型的集合，所以为了更加高效的操作我们需要批量处理
//                         * 例如说一次性插入1000条Record，使用insertBatch或者updateBatch类型的操作；
//                         * 插入的用户信息可以只包含：timestamp、adID、province、city
//                         * 这里面有一个问题：可能出现两条记录的Key是一样的，此时就需要更新累加操作
//                         */
//
//                        List<AdClicked> adClickedList = new ArrayList<AdClicked>();
//
//                        while (partition.hasNext()) {
//                            Tuple2<String, Long> record = partition.next();
//                            String[] splited = record._1.split("\t");
//
//                            AdClicked adClicked = new AdClicked();
//                            adClicked.setTimestamp(splited[0]);
//                            adClicked.setAdID(splited[1]);
//                            adClicked.setProvince(splited[2]);
//                            adClicked.setCity(splited[3]);
//                            adClicked.setClickedCount(record._2);
//
//                            adClickedList.add(adClicked);
//
//                        }
//
//                        JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
//
//                        List<AdClicked> inserting = new ArrayList<AdClicked>();
//                        List<AdClicked> updating = new ArrayList<AdClicked>();
//
//                        // adclicked
//                        // 表的字段：timestamp、ip、userID、adID、province、city、clickedCount
//                        for (AdClicked clicked : adClickedList) {
//                            jdbcWrapper.doQuery(
//                                    "SELECT count(1) FROM adclickedcount WHERE "
//                                            + " timestamp = ? AND adID = ? AND province = ? AND city = ? ",
//                                    new Object[] { clicked.getTimestamp(), clicked.getAdID(), clicked.getProvince(),
//                                            clicked.getCity() },
//                                    new ExecuteCallBack() {
//
//                                        @Override
//                                        public void resultCallBack(ResultSet result) throws Exception {
//                                            if (result.next()) {
//                                                long count = result.getLong(1);
//                                                clicked.setClickedCount(count);
//                                                updating.add(clicked);
//                                            } else {
//                                                inserting.add(clicked);
//                                            }
//
//                                        }
//                                    });
//                        }
//                        // adclicked
//                        // 表的字段：timestamp、ip、userID、adID、province、city、clickedCount
//                        ArrayList<Object[]> insertParametersList = new ArrayList<Object[]>();
//                        for (AdClicked inserRecord : inserting) {
//                            insertParametersList.add(new Object[] { inserRecord.getTimestamp(), inserRecord.getAdID(),
//                                    inserRecord.getProvince(), inserRecord.getCity(), inserRecord.getClickedCount() });
//                        }
//                        jdbcWrapper.doBatch("INSERT INTO adclickedcount VALUES(?,?,?,?,?)", insertParametersList);
//
//                        // adclicked
//                        // 表的字段：timestamp、ip、userID、adID、province、city、clickedCount
//                        ArrayList<Object[]> updateParametersList = new ArrayList<Object[]>();
//                        for (AdClicked updateRecord : updating) {
//                            updateParametersList.add(new Object[] {updateRecord.getClickedCount()  ,updateRecord.getTimestamp(), updateRecord.getAdID(),
//                                    updateRecord.getProvince(), updateRecord.getCity()
//                            });
//                        }
//                        jdbcWrapper.doBatch(
//                                "UPDATE adclickedcount set clickedCount = ? WHERE "
//                                        + " timestamp = ? AND adID = ? AND province = ? AND city = ? ",
//                                updateParametersList);
//
//                    }
//                });
//                return null;
//            }
//        });
//
//        /**
//         * 对广告点击进行TopN的计算，计算出每天每个省份的Top5排名的广告； 因为我们直接对RDD进行操作，所以使用了transform算子；
//         */
//        updateStateByKeyDStream.transform(new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
//
//            @Override
//            public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {
//                JavaRDD<Row> rowRDD = rdd.mapToPair(new PairFunction<Tuple2<String, Long>, String, Long>() {
//
//                    @Override
//                    public Tuple2<String, Long> call(Tuple2<String, Long> t) throws Exception {
//                        String[] splited = t._1.split("_");
//
//                        String timestamp = splited[0]; // yyyy-MM-dd
//                        String adID = splited[1];
//                        String province = splited[2];
//
//                        String clickedRecord = timestamp + "_" + adID + "_" + province;
//
//                        return new Tuple2<String, Long>(clickedRecord, t._2);
//                    }
//                }).reduceByKey(new Function2<Long, Long, Long>() {
//
//                    @Override
//                    public Long call(Long v1, Long v2) throws Exception {
//                        // TODO Auto-generated method stub
//                        return v1 + v2;
//                    }
//                }).map(new Function<Tuple2<String, Long>, Row>() {
//
//                    @Override
//                    public Row call(Tuple2<String, Long> v1) throws Exception {
//                        String[] splited = v1._1.split("_");
//
//                        String timestamp = splited[0]; // yyyy-MM-dd
//                        String adID = splited[1];
//                        String province = splited[2];
//
//                        return RowFactory.create(timestamp, adID, province, v1._2);
//                    }
//                });
//
//                StructType structType = DataTypes.createStructType(
//                        Arrays.asList(DataTypes.createStructField("timstamp", DataTypes.StringType, true),
//                                DataTypes.createStructField("adID", DataTypes.StringType, true),
//                                DataTypes.createStructField("province", DataTypes.StringType, true),
//                                DataTypes.createStructField("clickedCount", DataTypes.StringType, true)));
//
//                HiveContext hiveContext = new HiveContext(rdd.context());
//
//                DataFrame df = hiveContext.createDataFrame(rowRDD, structType);
//
//                df.registerTempTable("topNTableSource");
//
//                DataFrame result = hiveContext.sql("SELECT timstamp,adID,province,clickedCount FROM"
//                        + " (SELECT timstamp,adID,province,clickedCount, ROW_NUMBER() "
//                        + "OVER(PARTITION BY province ORDER BY clickedCount DESC) rank"
//                        + " FROM topNTableSource) subquery " + "WHERE rank <= 5");
//
//                return result.toJavaRDD();
//            }
//
//        }).foreachRDD(new Function<JavaRDD<Row>, Void>() {
//
//            @Override
//            public Void call(JavaRDD<Row> rdd) throws Exception {
//
//                rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
//
//                    @Override
//                    public void call(Iterator<Row> t) throws Exception {
//
//                        List<AdProvinceTopN> adProvinceTopN = new ArrayList<AdProvinceTopN>();
//
//                        while (t.hasNext()) {
//                            Row row = t.next();
//                            AdProvinceTopN item = new AdProvinceTopN();
//                            item.setTimestamp(row.getString(0));
//                            item.setAdID(row.getString(1));
//                            item.setProvince(row.getString(2));
//                            item.setClickedCount(row.getString(3));
//
//                            adProvinceTopN.add(item);
//                        }
//
//                        JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
//
//                        Set<String> set = new HashSet<String>();
//                        for (AdProvinceTopN item : adProvinceTopN) {
//                            set.add(item.getTimestamp() + "_" + item.getProvince());
//                        }
//
//                        // adclicked
//                        // 表的字段：timestamp、ip、userID、adID、province、city、clickedCount
//                        ArrayList<Object[]> deleteParametersList = new ArrayList<Object[]>();
//                        for (String deleteRecord : set) {
//                            String[] splited = deleteRecord.split("_");
//                            deleteParametersList.add(new Object[] { splited[0], splited[1] });
//                        }
//                        jdbcWrapper.doBatch("DELETE FROM adprovincetopn WHERE timestamp = ? AND province = ?",
//                                deleteParametersList);
//
//                        // adprovincetopn
//                        // 表的字段：timestamp、adID、province、clickedCount
//                        ArrayList<Object[]> insertParametersList = new ArrayList<Object[]>();
//                        for (AdProvinceTopN updateRecord : adProvinceTopN) {
//                            insertParametersList.add(new Object[] { updateRecord.getTimestamp(), updateRecord.getAdID(),
//                                    updateRecord.getProvince(), updateRecord.getClickedCount() });
//                        }
//                        jdbcWrapper.doBatch("INSERT INTO adprovincetopn VALUES (?,?,?,?) ", insertParametersList);
//
//                    }
//                });
//
//                return null;
//
//            }
//        });
//
//        /**
//         * 计算过去半个小时内广告点击的趋势
//         * 用户广告点击信息可以只包含：timestamp、ip、userID、adID、province、city
//         */
//        filteredadClickedStreaming.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {
//
//            @Override
//            public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
//                String[] splited = t._2.split("\t");
//
//                String adID = splited[3];
//
//                String time = splited[0]; //Todo：后续需要重构代码实现时间戳和分钟的转换提取，此处需要提取出该广告的点击分钟单位
//
//                return new Tuple2<String, Long>(time + "_" + adID, 1L);
//            }
//        }).reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
//
//                                    @Override
//                                    public Long call(Long v1, Long v2) throws Exception {
//                                        // TODO Auto-generated method stub
//                                        return v1 + v2;
//                                    }
//                                }, new Function2<Long, Long, Long>() {
//
//                                    @Override
//                                    public Long call(Long v1, Long v2) throws Exception {
//                                        // TODO Auto-generated method stub
//                                        return v1 - v2;
//                                    }
//                                },
//                Durations.minutes(30), Durations.minutes(5)).foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {
//
//            @Override
//            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
//                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {
//
//                    @Override
//                    public void call(Iterator<Tuple2<String, Long>> partition) throws Exception {
//
//                        List<AdTrendStat>adTrend = new ArrayList<AdTrendStat>();
//
//                        while(partition.hasNext()){
//                            Tuple2<String, Long> record = partition.next();
//                            String[] splited = record._1.split("_");
//                            String time = splited[0];
//                            String adID = splited[1];
//                            Long clickedCount = record._2;
//
//                            /**
//                             * 在插入数据到数据库的时候具体需要哪些字段？time、adID、clickedCount；
//                             * 而我们通过J2EE技术进行趋势绘图的时候肯定是需要年、月、日、时、分这个维度的，所有
//                             * 我们在这里需要年月日、小时、分钟这些时间维度；
//                             */
//
//                            AdTrendStat adTrendStat= new AdTrendStat();
//                            adTrendStat.setAdID(adID);
//                            adTrendStat.setClickedCount(clickedCount);
//                            adTrendStat.set_date(time);//Todo:获取年月日
//                            adTrendStat.set_hour(time);//Todo:获取小时
//                            adTrendStat.set_minute(time);//Todo:获取分钟
//
//                            adTrend.add(adTrendStat);
//
//                        }
//
//                        List<AdTrendStat> inserting = new ArrayList<AdTrendStat>();
//                        List<AdTrendStat> updating = new ArrayList<AdTrendStat>();
//
//                        JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
//
//                        // adclickedtrend
//                        // 表的字段：date、hour、minute、adID、clickedCount
//                        for (AdTrendStat clicked : adTrend) {
//                            AdTrendCountHistory adTrendCountHistory = new AdTrendCountHistory();
//
//                            jdbcWrapper.doQuery(
//                                    "SELECT count(1) FROM adclickedtrend WHERE "
//                                            + " date = ? AND hour = ? AND minute = ? AND adID = ?",
//                                    new Object[] { clicked.get_date(), clicked.get_hour(),
//                                            clicked.get_minute(),clicked.getAdID() },
//                                    new ExecuteCallBack() {
//
//                                        @Override
//                                        public void resultCallBack(ResultSet result) throws Exception {
//                                            if (result.next()) {
//
//                                                long count = result.getLong(1);
//                                                adTrendCountHistory.setClickedCountHistory(count);
//                                                updating.add(clicked);
//                                            } else {
//                                                inserting.add(clicked);
//                                            }
//
//                                        }
//                                    });
//                        }
//                        // adclickedtrend
//                        // 表的字段：date、hour、minute、adID、clickedCount
//                        ArrayList<Object[]> insertParametersList = new ArrayList<Object[]>();
//                        for (AdTrendStat inserRecord : inserting) {
//                            insertParametersList.add(new Object[] { inserRecord.get_date(),
//                                    inserRecord.get_hour(), inserRecord.get_minute(), inserRecord.getClickedCount(),
//                            });
//                        }
//                        jdbcWrapper.doBatch("INSERT INTO adclickedtrend VALUES(?,?,?,?)", insertParametersList);
//
//                        // adclickedtrend
//                        // 表的字段：date、hour、minute、adID、clickedCount
//                        ArrayList<Object[]> updateParametersList = new ArrayList<Object[]>();
//                        for (AdTrendStat updateRecord : updating) {
//                            updateParametersList.add(new Object[] {updateRecord.getClickedCount(),
//                                    updateRecord.get_date(),
//                                    updateRecord.get_hour(), updateRecord.get_minute() ,updateRecord.getAdID()});
//                        }
//                        jdbcWrapper.doBatch("UPDATE adclickedtrend set clickedCount = ? WHERE "
//                                        + " date = ? AND hour = ? AND minute = ? AND adID = ?"
//                                , updateParametersList);
//
//                    }
//                });
//                return null;
//            }
//        });
//
//		/*
//		 * Spark
//		 * Streaming执行引擎也就是Driver开始运行，Driver启动的时候是位于一条新的线程中的，当然其内部有消息循环体，用于
//		 * 接受应用程序本身或者Executor中的消息；
//		 */
//        jsc.start();
//
//        jsc.awaitTermination();
//        jsc.close();
//
//    }
//
//}
//
//class JDBCWrapper {
//
//    private static JDBCWrapper jdbcInstance = null;
//    private static LinkedBlockingQueue<Connection> dbConnectionPool = new LinkedBlockingQueue<Connection>();
//
//    static {
//        try {
//            Class.forName("com.mysql.jdbc.Driver");
//        } catch (ClassNotFoundException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//    }
//
//    public static JDBCWrapper getJDBCInstance() {
//        if (jdbcInstance == null) {
//
//            synchronized (JDBCWrapper.class) {
//                if (jdbcInstance == null) {
//                    jdbcInstance = new JDBCWrapper();
//                }
//            }
//
//        }
//
//        return jdbcInstance;
//    }
//
//    private JDBCWrapper() {
//
//        for (int i = 0; i < 10; i++) {
//
//            try {
//                Connection conn = DriverManager.getConnection("jdbc:mysql://Master:3306/sparkstreaming", "root",
//                        "root");
//                dbConnectionPool.put(conn);
//            } catch (Exception e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//
//        }
//
//    }
//
//    public synchronized Connection getConnection() {
//        while (0 == dbConnectionPool.size()) {
//            try {
//                Thread.sleep(20);
//            } catch (InterruptedException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//        }
//
//        return dbConnectionPool.poll();
//    }
//
//    public int[] doBatch(String sqlText, List<Object[]> paramsList) {
//
//        Connection conn = getConnection();
//        PreparedStatement preparedStatement = null;
//        int[] result = null;
//        try {
//            conn.setAutoCommit(false);
//            preparedStatement = conn.prepareStatement(sqlText);
//
//            for (Object[] parameters : paramsList) {
//                for (int i = 0; i < parameters.length; i++) {
//                    preparedStatement.setObject(i + 1, parameters[i]);
//                }
//
//                preparedStatement.addBatch();
//            }
//
//            result = preparedStatement.executeBatch();
//
//            conn.commit();
//
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        } finally {
//            if (preparedStatement != null) {
//                try {
//                    preparedStatement.close();
//                } catch (SQLException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            }
//
//            if (conn != null) {
//                try {
//                    dbConnectionPool.put(conn);
//                } catch (InterruptedException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            }
//        }
//
//        return result;
//    }
//
//    public void doQuery(String sqlText, Object[] paramsList, ExecuteCallBack callBack) {
//
//        Connection conn = getConnection();
//        PreparedStatement preparedStatement = null;
//        ResultSet result = null;
//        try {
//            preparedStatement = conn.prepareStatement(sqlText);
//            for (int i = 0; i < paramsList.length; i++) {
//                preparedStatement.setObject(i + 1, paramsList[i]);
//            }
//            result = preparedStatement.executeQuery();
//            callBack.resultCallBack(result);
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        } finally {
//            if (preparedStatement != null) {
//                try {
//                    preparedStatement.close();
//                } catch (SQLException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            }
//
//            if (conn != null) {
//                try {
//                    dbConnectionPool.put(conn);
//                } catch (InterruptedException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            }
//        }
//
//    }
//}
//
//interface ExecuteCallBack {
//    void resultCallBack(ResultSet result) throws Exception;
//}
//
//class UserAdClicked {
//    private String timestamp;
//    private String ip;
//    private String userID;
//    private String adID;
//    private String province;
//    private String city;
//    private Long clickedCount;
//
//    public Long getClickedCount() {
//        return clickedCount;
//    }
//
//    public void setClickedCount(Long clickedCount) {
//        this.clickedCount = clickedCount;
//    }
//
//    public String getTimestamp() {
//        return timestamp;
//    }
//
//    public void setTimestamp(String timestamp) {
//        this.timestamp = timestamp;
//    }
//
//    public String getIp() {
//        return ip;
//    }
//
//    public void setIp(String ip) {
//        this.ip = ip;
//    }
//
//    public String getUserID() {
//        return userID;
//    }
//
//    public void setUserID(String userID) {
//        this.userID = userID;
//    }
//
//    public String getAdID() {
//        return adID;
//    }
//
//    public void setAdID(String adID) {
//        this.adID = adID;
//    }
//
//    public String getProvince() {
//        return province;
//    }
//
//    public void setProvince(String province) {
//        this.province = province;
//    }
//
//    public String getCity() {
//        return city;
//    }
//
//    public void setCity(String city) {
//        this.city = city;
//    }
//}
//
//class AdClicked {
//    private String timestamp;
//    private String adID;
//    private String province;
//    private String city;
//    private Long clickedCount;
//
//    public String getTimestamp() {
//        return timestamp;
//    }
//
//    public void setTimestamp(String timestamp) {
//        this.timestamp = timestamp;
//    }
//
//    public String getAdID() {
//        return adID;
//    }
//
//    public void setAdID(String adID) {
//        this.adID = adID;
//    }
//
//    public String getProvince() {
//        return province;
//    }
//
//    public void setProvince(String province) {
//        this.province = province;
//    }
//
//    public String getCity() {
//        return city;
//    }
//
//    public void setCity(String city) {
//        this.city = city;
//    }
//
//    public Long getClickedCount() {
//        return clickedCount;
//    }
//
//    public void setClickedCount(Long clickedCount) {
//        this.clickedCount = clickedCount;
//    }
//
//}
//
//class AdProvinceTopN {
//    private String timestamp;
//    private String adID;
//
//    public String getTimestamp() {
//        return timestamp;
//    }
//
//    public void setTimestamp(String timestamp) {
//        this.timestamp = timestamp;
//    }
//
//    public String getAdID() {
//        return adID;
//    }
//
//    public void setAdID(String adID) {
//        this.adID = adID;
//    }
//
//    public String getProvince() {
//        return province;
//    }
//
//    public void setProvince(String province) {
//        this.province = province;
//    }
//
//    public String getClickedCount() {
//        return clickedCount;
//    }
//
//    public void setClickedCount(String clickedCount) {
//        this.clickedCount = clickedCount;
//    }
//
//    private String province;
//    private String clickedCount;
//}
//
//
//class AdTrendStat{
//    private String _date;
//    private String _hour;
//    private String _minute;
//    public String get_date() {
//        return _date;
//    }
//    public void set_date(String _date) {
//        this._date = _date;
//    }
//    public String get_hour() {
//        return _hour;
//    }
//    public void set_hour(String _hour) {
//        this._hour = _hour;
//    }
//    public String get_minute() {
//        return _minute;
//    }
//    public void set_minute(String _minute) {
//        this._minute = _minute;
//    }
//    public String getAdID() {
//        return adID;
//    }
//    public void setAdID(String adID) {
//        this.adID = adID;
//    }
//    public Long getClickedCount() {
//        return clickedCount;
//    }
//    public void setClickedCount(Long clickedCount) {
//        this.clickedCount = clickedCount;
//    }
//    private String adID;
//    private Long clickedCount;
//}
//
//class AdTrendCountHistory{
//    private Long clickedCountHistory;
//
//    public Long getClickedCountHistory() {
//        return clickedCountHistory;
//    }
//
//    public void setClickedCountHistory(Long clickedCountHistory) {
//        this.clickedCountHistory = clickedCountHistory;
//    }
//}