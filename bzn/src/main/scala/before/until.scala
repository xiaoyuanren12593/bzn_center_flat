package before

import java.text.{NumberFormat, SimpleDateFormat}
import java.util.regex.Pattern
import java.util.{Calendar, Date}

import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FsShell, Path}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Months}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by a2589 on 2018/4/2.
  */
trait until {
  //Hbase执行truncate操作
  def truncate_hbase(sc: SparkContext, table: String): Unit
  = {

    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")


    conf.set(TableInputFormat.INPUT_TABLE, table)

    val usersRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    val count: Long = usersRDD.count()
    if (count != 0) {
      // 取得一个数据库连接对象
      val admin = ConnectionFactory.createConnection(conf).getAdmin

      // 取得一个数据库元数据操作对象
      System.out.println("---------------清空表 START-----------------")

      // 取得目标数据表的表名对象
      val tableName = TableName.valueOf(table)

      // 设置表状态为无效
      admin.disableTable(tableName)

      // 清空指定表的数据
      admin.truncateTable(tableName, true)

      System.out.println("---------------清空表 End-----------------")

    }
  }


  //将时间转换为时间戳
  def currentTimeL(str: String): Long
  = {
    val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

    val insured_create_time_curr_one: DateTime = DateTime.parse(str, format)
    val insured_create_time_curr_Long: Long = insured_create_time_curr_one.getMillis

    insured_create_time_curr_Long
  }

  //得到当前的时间
  def getNowTime(): String
  = {
    //得到当前的日期
    val now: Date = new Date()
    val dateFormatOne: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
    val now_Date: String = dateFormatOne.format(now)
    now_Date
  }

  //当前时间的前三个月是第几天
  def ThreeMonth(): String
  = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
    val date = new Date()
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.MONTH, -3)
    val dates = calendar.getTime()
    val end: String = sdf.format(dates)
    end
  }

  //得到当前时间是周几
  def getWeekOfDate(strDate: String): Int
  = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(strDate)

    val weekDays = Array(7, 1, 2, 3, 4, 5, 6)
    val cal = Calendar.getInstance
    cal.setTime(date)
    var w = cal.get(Calendar.DAY_OF_WEEK) - 1
    if (w < 0) w = 0
    val res = weekDays(w)
    res
  }

  //得到现在是几几年
  def getNowYear(date: Date, dateFormat: SimpleDateFormat): Int = {
    //得到long类型当前时间//得到long类型当前时间
    dateFormat.format(date).toInt
  }

  //删除日期的小数点
  def deletePoint(strs: String): String = {
    var str = strs
    if (str.indexOf(".") > 0) {
      str = str.replaceAll("0+?$", ""); //去掉多余的0
      str = str.replaceAll("[.]$", ""); //如最后一位是.则去掉
    }
    str
  }

  //判断一个字符串是否含有数字
  def HasDigit(content: String): Boolean = {
    var flag = false
    val p = Pattern.compile(".*\\d+.*")
    val m = p.matcher(content)
    if (m.matches) flag = true
    flag
  }

  //计算一个日期，距离当前相隔几个月
  def getMonth(str: String): String = {
    val s = getNowTime()
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val start = formatter.parseDateTime(str)
    val end = formatter.parseDateTime(s.split(" ")(0))
    val months = Months.monthsBetween(start, end).getMonths
    months + ""
  }

  //算法统计相同
  def bruteForceStringMatch(source: String, pattern: String): Int = {
    val slen = source.length
    val plen = pattern.length
    val s = source.toCharArray
    val p = pattern.toCharArray
    var i = 0
    var j = 0
    if (slen < plen) -1 //如果主串长度小于模式串，直接返回-1，匹配失败
    else {
      while ( {
        i < slen && j < plen
      }) if (s(i) == p(j)) { //如果i,j位置上的字符匹配成功就继续向后匹配
        i += 1
        j += 1
      }
      else {
        i = i - (j - 1) //i回溯到主串上一次开始匹配下一个位置的地方

        j = 0 //j重置，模式串从开始再次进行匹配

      }
      if (j == plen) { //匹配成功
        i - j
      }
      else -1 //匹配失败
    }
  }

  //出现的次数
  def count_num(arr: Array[String], num: String): Int = {
    var count = 0
    var i = 0

    while ( {
      i < arr.length
    }) {
      if (arr(i) == num) count += 1

      {
        i += 1;
        i - 1
      }
    }
    count
  }

  //得到2个日期之间的所有日期(当天和3个月之前的)
  def getBeg_End(): ArrayBuffer[String] = {
    val sdf = new SimpleDateFormat("yyyyMMdd")

    //得到过去第三个月的日期
    val c = Calendar.getInstance
    c.setTime(new Date)
    c.add(Calendar.MONTH, -3)
    val m3 = c.getTime
    val mon3 = sdf.format(m3)


    //得到今天的日期
    val cc = Calendar.getInstance
    cc.setTime(new Date)
    val day = cc.getTime
    val day_time = sdf.format(day)


    //得到他们相间的所有日期
    val arr: ArrayBuffer[String] = ArrayBuffer[String]()
    val date_start = sdf.parse(mon3)
    //    val date_start = sdf.parse("20161007")
    val date_end = sdf.parse(day_time)
    //    val date_end = sdf.parse("20161008")
    var date = date_start
    val cd = Calendar.getInstance //用Calendar 进行日期比较判断

    while (date.getTime <= date_end.getTime) {
      arr += sdf.format(date)
      cd.setTime(date)
      cd.add(Calendar.DATE, 1); //增加一天 放入集合
      date = cd.getTime
    }

    arr

  }


  //得到2个日期之间的所有天数
  def getBeg_End_one_two(mon3: String, day_time: String): ArrayBuffer[String]
  = {
    val sdf = new SimpleDateFormat("yyyyMMdd")

    //得到过去第三个月的日期
    val c = Calendar.getInstance
    c.setTime(new Date)
    c.add(Calendar.MONTH, -3)
    val m3 = c.getTime


    //得到今天的日期
    val cc = Calendar.getInstance
    cc.setTime(new Date)
    val day = cc.getTime


    //得到他们相间的所有日期
    val arr: ArrayBuffer[String] = ArrayBuffer[String]()
    val date_start = sdf.parse(mon3)
    //    val date_start = sdf.parse("20161007")
    val date_end = sdf.parse(day_time)
    //    val date_end = sdf.parse("20161008")
    var date = date_start
    val cd = Calendar.getInstance //用Calendar 进行日期比较判断

    while (date.getTime <= date_end.getTime) {
      arr += sdf.format(date)
      cd.setTime(date)
      cd.add(Calendar.DATE, 1); //增加一天 放入集合
      date = cd.getTime
    }
    arr

  }

  //得到2个日期之间的所有月份
  def getBeg_End_one_two_month(mon3: String, day_time: String): ArrayBuffer[String]
  = {
    val sdf = new SimpleDateFormat("yyyyMM")

    //得到今天的日期
    val cc = Calendar.getInstance
    cc.setTime(new Date)
    val day = cc.getTime


    //得到他们相间的所有日期
    val arr: ArrayBuffer[String] = ArrayBuffer[String]()
    val date_start = sdf.parse(mon3)
    //    val date_start = sdf.parse("20161007")
    val date_end = sdf.parse(day_time)
    //    val date_end = sdf.parse("20161008")
    var date = date_start
    val cd = Calendar.getInstance //用Calendar 进行日期比较判断

    while (date.getTime <= date_end.getTime) {
      arr += sdf.format(date)
      cd.setTime(date)
      cd.add(Calendar.MONTH, 1); //增加一天 放入集合
      date = cd.getTime
    }
    arr
  }

  //企业风险::得到城市的编码
  def city_code(sqlContext: HiveContext): collection.Map[String, String]
  = {
    val d_city_grade = sqlContext.sql("select * from odsdb_prd.d_city_grade").select("city_name", "city_code").filter("length(city_name)>1 and city_code is not null")
    val d_city_grade_map: collection.Map[String, String] = d_city_grade.map(x => {
      val city_name = x.getAs("city_name").toString
      val city_code = x.getAs("city_code").toString
      (city_name, city_code)
    }).collectAsMap()
    d_city_grade_map
  }

  //企业风险::得到hbase标签
  def getHbase_label(tepOne: RDD[((ImmutableBytesWritable, Result), String, String)], d_city_grade_map: collection.Map[String, String]): RDD[(Array[Double], String)]
  = {
    //遍历输出
    val numberFormat = NumberFormat.getInstance
    val vectors: RDD[(Array[Double], String)] = tepOne.map(x => {
      val work_one = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, s"worktype_1_count".getBytes))
      val work_one_res = if (work_one != null && x._3 != null) s"${numberFormat.format(work_one.toFloat / x._3.toFloat * 100)}%" else "0"

      val work_two = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, s"worktype_2_count".getBytes))
      val work_two_res = if (work_two != null && x._3 != null) s"${numberFormat.format(work_two.toFloat / x._3.toFloat * 100)}%" else "0"

      val work_three = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, s"worktype_3_count".getBytes))
      val work_three_res = if (work_three != null && x._3 != null) s"${numberFormat.format(work_three.toFloat / x._3.toFloat * 100)}%" else "0"

      val work_four = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, s"worktype_4_count".getBytes))
      val work_four_res = if (work_four != null && x._3 != null) s"${numberFormat.format(work_four.toFloat / x._3.toFloat * 100)}%" else "0"

      val work_five = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, s"worktype_5_count".getBytes))
      val work_five_res = if (work_five != null && x._3 != null) s"${numberFormat.format(work_five.toFloat / x._3.toFloat * 100)}%" else "0"

      //企业报案件数
      val report_num = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "report_num".getBytes))
      val report_num_res = if (report_num != null) report_num else 0


      //死亡案件统计
      val death_num = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "death_num".getBytes))
      val death_num_res = if (death_num != null) death_num else 0


      //伤残案件数
      val disability_num = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "disability_num".getBytes))
      val disability_num_res = if (disability_num != null) disability_num else 0

      //已赚保费
      val charged_premium = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "charged_premium".getBytes))
      val charged_premium_res = if (charged_premium != null) charged_premium.replaceAll(",", "") else "0"

      //实际已赔付金额
      val all_compensation = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "all_compensation".getBytes))
      val all_compensation_res = if (all_compensation != null) all_compensation.replaceAll(",", "") else "0"

      //预估总赔付金额
      val pre_all_compensation = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "pre_all_compensation".getBytes))
      val pre_all_compensation_res = if (pre_all_compensation != null) pre_all_compensation.replaceAll(",", "") else "0"


      //平均出险周期
      val avg_aging_risk_before = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "avg_aging_risk".getBytes))
      val avg_aging_risk = if (avg_aging_risk_before != null) avg_aging_risk_before else "0"


      //企业的潜在人员规模
      val ent_potential_scale_before = Bytes.toString(x._1._2.getValue("baseinfo".getBytes, "ent_potential_scale".getBytes))
      val ent_potential_scale = if (ent_potential_scale_before != null) ent_potential_scale_before else "0"

      //员工平均年龄
      val ent_employee_age = Bytes.toString(x._1._2.getValue("baseinfo".getBytes, "ent_employee_age".getBytes))
      val ent_employee_age_res = if (ent_employee_age != null) ent_employee_age else "0"


      //城市
      val city_node = Bytes.toString(x._1._2.getValue("baseinfo".getBytes, "ent_city".getBytes))
      val ent_city = d_city_grade_map.getOrElse(city_node, "0")


      //当前在保人数
      val cur_insured_persons_before = Bytes.toString(x._1._2.getValue("insureinfo".getBytes, "cur_insured_persons".getBytes))
      val cur_insured_persons = if (cur_insured_persons_before != null) cur_insured_persons_before else "0"

      val work_level_array = Array(work_one_res, work_two_res, work_three_res, work_four_res, work_five_res)
      val work_type_number = work_level_array.zipWithIndex
      //哪个工种的占比最多，就找出对应的工种
      val work_type = work_type_number.map(x => {
        (x._1.replaceAll("%", "").replaceAll(",", "").toDouble, x._2 + 1)
      }).reduce((x1, x2) => {
        if (x1._1 >= x2._1) x1 else x2
      })._2 + ""


      //ent_id|rowKey
      val kv = x._1._2.getRow
      val rowkey = Bytes.toString(kv)
      val man = if (x._2 != null) x._2.split("-")(0).replaceAll("%", "") else "0"
      val woman = if (x._2 != null) x._2.split("-")(1).replaceAll("%", "") else "0"
      val ss = (s"哪个工种占比最多:$work_type\t当前在保:$cur_insured_persons\t男生占比:$man\t女生占比:$woman\t员工平均年龄:$ent_employee_age_res" +
        s"\t死亡案件数:$death_num_res\t伤残案件数:$disability_num_res" +
        s"\t报案数:$report_num_res\t平均出险周期:$avg_aging_risk" +
        s"\t企业的潜在人员规模:$ent_potential_scale\t$ent_city" +
        s"\t实际赔付额度:$all_compensation_res\t预估赔付额度:$pre_all_compensation_res\t已赚保费:$charged_premium_res", rowkey)

      (Array(work_type, cur_insured_persons, man, woman,
        ent_employee_age_res, death_num_res, disability_num_res,
        report_num_res, avg_aging_risk, ent_potential_scale, ent_city,
        all_compensation_res, pre_all_compensation_res, charged_premium_res).map(_.toString.toDouble), rowkey)
    })
    vectors
  }


  //得到企业标签数据
  def getHbase_value(sc:SparkContext): RDD[(ImmutableBytesWritable, Result)]  ={
    /**
      * 第一步:创建一个JobConf
      **/
    //定义HBase的配置
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_enterprise_vT")

    val usersRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    usersRDD
  }

  def KeyValueToString(keyValues: Array[KeyValue], json: JSONObject,ent_name:String): String = {

    val it = keyValues.iterator
    val res = new StringBuilder
    while (it.hasNext) {
      val end = it.next()
      val row = Bytes.toString(end.getRow)
      val family = Bytes.toString(end.getFamily) //列族
      val qual = Bytes.toString(end.getQualifier) //字段
      val value = Bytes.toString(end.getValue) //字段值

      //      res.append(row + "->" + family + "->" + qual + "->" + value + ",")
      json.put("row", row)
      json.put("family", family)
      json.put("qual", qual)
      json.put("value", value)
      res.append(json.toString + ";")
    }
    s"${ent_name}mk6${ res.substring(0, res.length - 1)}"

  }


  //得到2个日期之间的所有天数:该方法只适用于Enter_risk_everyday该类
  //  def getBeg_End_one_two(mon3:String,day_time:String): ArrayBuffer[String] = {
  //    val sdf = new SimpleDateFormat("yyyy/MM/dd")
  //
  //    //得到过去第三个月的日期
  //    val c = Calendar.getInstance
  //    c.setTime(new Date)
  //    c.add(Calendar.MONTH, -3)
  //    val m3 = c.getTime
  //
  //
  //    //得到今天的日期
  //    val cc = Calendar.getInstance
  //    cc.setTime(new Date)
  //    val day = cc.getTime
  //
  //
  //    //得到他们相间的所有日期
  //    val arr: ArrayBuffer[String] = ArrayBuffer[String]()
  //    val date_start = sdf.parse(mon3)
  //    //    val date_start = sdf.parse("20161007")
  //    val date_end = sdf.parse(day_time)
  //    //    val date_end = sdf.parse("20161008")
  //    var date = date_start
  //    val cd = Calendar.getInstance //用Calendar 进行日期比较判断
  //
  //    while (date.getTime <= date_end.getTime) {
  //      arr += sdf.format(date)
  //      cd.setTime(date)
  //      cd.add(Calendar.DATE, 1); //增加一天 放入集合
  //      date = cd.getTime
  //    }
  //    arr
  //
  //  }


}
