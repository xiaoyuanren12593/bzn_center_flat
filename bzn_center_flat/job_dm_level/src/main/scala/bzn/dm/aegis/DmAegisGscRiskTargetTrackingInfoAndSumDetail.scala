package bzn.dm.aegis

import bzn.dm.util.SparkUtil
import bzn.job.common.DataBaseUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2020/2/5
  * Time:15:36
  * describe: 神盾-国寿财3月底赔付率目标达成追踪
  **/
object DmAegisGscRiskTargetTrackingInfoAndSumDetail extends SparkUtil with DataBaseUtil{
  case class hisDiskPartition(channel_name_disk:String,his_disk_partition:String)
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4

    val res1 = getAegisGscRiskTargetTrackingInfoData(hiveContext)

//    val user103 = "mysql.username.103"
//    val pass103 = "mysql.password.103"
//    val url103 = "mysql_url.103.dmdb"
    val driver = "mysql.driver"
    val tableName1 = "dm_aegis_gsc_risk_target_tracking_info_detail"
    val tableName2 = "dm_aegis_gsc_risk_target_tracking_summary_detail"
    val user106 = "mysql.username.106"
    val pass106 = "mysql.password.106"
    val url106 = "mysql.url.106.dmdb"
    
    saveASMysqlTable(res1._1: DataFrame, tableName1: String, SaveMode.Overwrite,user106:String,pass106:String,driver:String,url106:String)
    saveASMysqlTable(res1._2: DataFrame, tableName2: String, SaveMode.Overwrite,user106:String,pass106:String,driver:String,url106:String)

    sc.stop()
  }

  /**
    * 得到gsc目标追踪的明细数据
    * @param sqlContext 上下文
    */
  def getAegisGscRiskTargetTrackingInfoData(sqlContext:HiveContext): (DataFrame,DataFrame) = {

    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    val hisDiskPartitionData: Seq[hisDiskPartition] = Seq(
      hisDiskPartition(	"临沂淼信劳务服务有限公司",	"A"	),
      hisDiskPartition(	"湖南源哲方略企业管理咨询有限公司",	"C"	),
      hisDiskPartition(	"烟台思汇人力资源服务有限公司",	"B"	),
      hisDiskPartition(	"哈尔滨六合人力资源服务有限公司",	"B"	),
      hisDiskPartition(	"青岛新方向企业管理有限公司",	"A"	),
      hisDiskPartition(	"青岛华之瑞企业管理有限公司",	"C"	),
      hisDiskPartition(	"邹平信达人力资源服务有限公司",	"B"	),
      hisDiskPartition(	"湖北云珅企业管理服务有限公司",	"A"	),
      hisDiskPartition(	"泰安众鑫人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"青岛世纪华劳企业管理有限公司",	"A"	),
      hisDiskPartition(	"上海万鑫劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"德州百业劳务服务有限公司",	"B"	),
      hisDiskPartition(	"北京京达永欣企业管理有限公司",	"A"	),
      hisDiskPartition(	"泊头市弘远企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"青岛北航人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"保定百优劳务派遣有限公司",	"B"	),
      hisDiskPartition(	"重庆合众安法律咨询有限公司",	"A"	),
      hisDiskPartition(	"湖南天胜人力资源有限公司",	"C"	),
      hisDiskPartition(	"唐山市丰南区优瑞人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"临沂行健人力资源有限公司",	"C"	),
      hisDiskPartition(	"山东明睿企业管理咨询服务有限公司",	"A"	),
      hisDiskPartition(	"滨州市汇联人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"青岛招才通企业管理有限公司",	"B"	),
      hisDiskPartition(	"宁波浙邦人力资源有限公司",	"A"	),
      hisDiskPartition(	"广州聚才企业管理有限责任公司",	"A"	),
      hisDiskPartition(	"北京润亚环宇建筑工程有限公司",	"A"	),
      hisDiskPartition(	"山东携众人力资源有限公司",	"B"	),
      hisDiskPartition(	"武汉三惠人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"乐陵盛安人力资源有限公司",	"A"	),
      hisDiskPartition(	"江西大唐人力资源集团有限公司",	"A"	),
      hisDiskPartition(	"惠州企一通企业管理有限公司",	"C"	),
      hisDiskPartition(	"湖北生生人力资源股份有限公司",	"A"	),
      hisDiskPartition(	"重庆翔耀保险咨询服务有限公司",	"B"	),
      hisDiskPartition(	"重庆欧达慧哲企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"重庆福跃广企业管理有限公司",	"A"	),
      hisDiskPartition(	"东营冠通劳务有限公司",	"A"	),
      hisDiskPartition(	"沧州天硕源人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"青岛仟佰职人力资源有限公司",	"B"	),
      hisDiskPartition(	"江苏八方人力资源有限公司",	"A"	),
      hisDiskPartition(	"山东麦克斯企业咨询服务有限公司",	"A"	),
      hisDiskPartition(	"武汉众生缘人力资源有限公司",	"B"	),
      hisDiskPartition(	"襄阳点派人力资源有限公司",	"A"	),
      hisDiskPartition(	"辽宁迈格人力资源服务有限公司",	"B"	),
      hisDiskPartition(	"项城市腾龙实业有限公司",	"A"	),
      hisDiskPartition(	"沈阳易通盛人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"众美(山东)人力资源管理咨询有限公司",	"C"	),
      hisDiskPartition(	"定州市润达人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"山东帮企人力资源有限公司",	"A"	),
//      hisDiskPartition(	"玉田县鸦鸿桥玉玲陶瓷商店",	"A"	),
      hisDiskPartition(	"阜阳创赢人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"高密市汇格劳务服务有限公司",	"B"	),
      hisDiskPartition(	"徐州市卓远人力资源有限公司",	"A"	),
      hisDiskPartition(	"赣州市南康区联业人力资源有限公司",	"B"	),
      hisDiskPartition(	"南京睿雯人力资源有限公司",	"A"	),
      hisDiskPartition(	"深圳市丽星清洁服务有限公司",	"A"	),
      hisDiskPartition(	"烟台辰星人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"南昌市越秀房地产开发有限公司",	"A"	),
      hisDiskPartition(	"江西赣鄱人力资源市场发展有限公司",	"A"	),
      hisDiskPartition(	"山东智扬人力资源有限公司",	"C"	),
      hisDiskPartition(	"珠海佰合人力资源有限公司",	"A"	),
      hisDiskPartition(	"重庆迈思腾人力资源管理咨询有限公司",	"C"	),
      hisDiskPartition(	"易安仁才（北京）人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"上海勤森包装材料有限公司",	"A"	),
      hisDiskPartition(	"天津薪食尚餐饮管理有限公司",	"A"	),
      hisDiskPartition(	"江西智客供应链管理有限公司",	"B"	),
      hisDiskPartition(	"苏州纳智杰人力资源有限公司",	"A"	),
      hisDiskPartition(	"上饶市卓越人力资源有限公司",	"A"	),
      hisDiskPartition(	"深圳鑫众源人力资源有限公司",	"A"	),
      hisDiskPartition(	"烟台市欢赢人力资源有限公司",	"A"	),
      hisDiskPartition(	"湖南智财网络技术有限公司",	"A"	),
      hisDiskPartition(	"天津三社劳动服务有限公司",	"A"	),
      hisDiskPartition(	"张家口平安人力资源有限公司",	"A"	),
      hisDiskPartition(	"山东智慧鲁滕企业管理咨询有限公司",	"C"	),
      hisDiskPartition(	"邹城市众品人力资源有限公司",	"A"	),
      hisDiskPartition(	"淄博众诚人力资源有限公司周村分公司",	"A"	),
      hisDiskPartition(	"潍坊非凡人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"福州众和信达人力资源有限公司",	"A"	),
      hisDiskPartition(	"山东利轩企业管理有限公司",	"A"	),
      hisDiskPartition(	"河南众程人力资源有限公司",	"A"	),
      hisDiskPartition(	"湖南阳光启航人力资源管理服务有限公司",	"A"	),
      hisDiskPartition(	"高密市柴沟镇玉刚单板厂",	"A"	),
      hisDiskPartition(	"临沂泰华人力资源有限公司",	"B"	),
      hisDiskPartition(	"淄博新乾劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"衡阳南方崛起信息咨询有限公司",	"A"	),
      hisDiskPartition(	"山东达能人力资源有限公司",	"A"	),
      hisDiskPartition(	"湖南潇湘人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"青岛钰琳前程人力资源有限公司东营分公司",	"B"	),
      hisDiskPartition(	"保定领驭人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"常州市奇波机械厂",	"B"	),
      hisDiskPartition(	"石家庄森海人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"贵州三赢劳务有限责任公司",	"A"	),
      hisDiskPartition(	"云南康言人力资源有限公司",	"B"	),
      hisDiskPartition(	"包头市仁明人力资源有限公司",	"A"	),
      hisDiskPartition(	"浙江禾祥建筑劳务有限公司",	"A"	),
      hisDiskPartition(	"南京云亿朵企业管理有限公司",	"B"	),
      hisDiskPartition(	"重庆精辉誉企业管理服务有限公司",	"C"	),
      hisDiskPartition(	"青岛玖元人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"山东岛城速招人力资源管理咨询有限公司",	"A"	),
      hisDiskPartition(	"湖北领才人力资源有限公司",	"A"	),
      hisDiskPartition(	"烟台晟燃人力资源有限公司",	"A"	),
      hisDiskPartition(	"泰安志诚人力资源有限公司",	"A"	),
      hisDiskPartition(	"湖北华夏创业管理顾问有限公司",	"C"	),
      hisDiskPartition(	"甘肃众和人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"阜阳市星驾电子科技有限公司",	"A"	),
      hisDiskPartition(	"山东优创企业管理有限公司",	"C"	),
      hisDiskPartition(	"漳州吉胜劳务派遣有限公司",	"C"	),
      hisDiskPartition(	"石家庄华保圣险人力资源有限公司",	"A"	),
      hisDiskPartition(	"佛山市顺德区桂达企业管理顾问有限公司",	"B"	),
      hisDiskPartition(	"广州华路卓企业管理咨询有限公司",	"C"	),
      hisDiskPartition(	"深圳市超级蓝领网络科技服务有限公司",	"A"	),
      hisDiskPartition(	"徐州安企保险代理有限公司",	"A"	),
      hisDiskPartition(	"青岛天行健人力资源管理有限公司",	"B"	),
      hisDiskPartition(	"河南省汇智人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"青岛百优特企业管理有限公司",	"C"	),
      hisDiskPartition(	"青岛精匠劳务服务有限公司",	"C"	),
      hisDiskPartition(	"肇庆市瑞隆电子有限公司",	"A"	),
      hisDiskPartition(	"湖南五八财税管理咨询有限公司",	"A"	),
      hisDiskPartition(	"重庆博来杰人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"青岛雅睿企业管理服务有限公司",	"A"	),
      hisDiskPartition(	"晶品服务外包(江苏)有限公司",	"B"	),
      hisDiskPartition(	"重庆维特利人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"山东盛弘劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"苏州德菲尔外包服务有限公司",	"A"	),
      hisDiskPartition(	"沧州市海瑞通信工程服务有限公司",	"A"	),
      hisDiskPartition(	"苏州宏尔盛企业管理服务有限公司",	"B"	),
      hisDiskPartition(	"深圳市天和劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"河南工多多人力资源有限公司",	"A"	),
      hisDiskPartition(	"临沂天德人力资源有限公司",	"A"	),
      hisDiskPartition(	"苏州聚恩企业管理服务有限公司",	"B"	),
      hisDiskPartition(	"泰安市泰兴劳务派遣有限公司",	"C"	),
      hisDiskPartition(	"苏州杰尔佳服务外包有限公司",	"A"	),
      hisDiskPartition(	"湖南蓝桥人力资源有限公司",	"A"	),
      hisDiskPartition(	"绿能中环（武汉）科技有限公司",	"A"	),
      hisDiskPartition(	"北京香海餐饮有限责任公司",	"B"	),
      hisDiskPartition(	"天津市亿邦盛劳务服务有限公司",	"C"	),
      hisDiskPartition(	"宁夏劳谦人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"湖北天窗亮话人力资源有限公司",	"A"	),
      hisDiskPartition(	"临沂鸿儒人力资源服务有限公司",	"B"	),
      hisDiskPartition(	"山东奶牛网络科技有限公司",	"A"	),
      hisDiskPartition(	"湖南玖玥物流有限公司",	"A"	),
      hisDiskPartition(	"苏州市鼎博企业服务有限公司",	"A"	),
      hisDiskPartition(	"北京壹心壹翼科技有限公司",	"A"	),
      hisDiskPartition(	"曹县汇商联众人力资源有限公司",	"C"	),
      hisDiskPartition(	"黑龙江众德商务服务有限公司",	"A"	),
      hisDiskPartition(	"常州鑫泰劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"广州华劳人力资源有限公司",	"A"	),
      hisDiskPartition(	"湖北东方俊才人力资源有限公司",	"A"	),
      hisDiskPartition(	"江苏雅风家具制造有限公司",	"A"	),
      hisDiskPartition(	"重庆卓博企业管理有限公司",	"A"	),
      hisDiskPartition(	"重庆壹零八人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"天津众晟鑫劳务服务有限公司",	"B"	),
      hisDiskPartition(	"江苏成功人力资源有限公司",	"A"	),
      hisDiskPartition(	"青岛鼎煜人力资源有限公司",	"A"	),
      hisDiskPartition(	"慈溪城宇物业服务有限公司",	"A"	),
      hisDiskPartition(	"临沂聚才德人力资源有限公司",	"A"	),
      hisDiskPartition(	"佛山市顺德区乐从镇辉鸿人力资源信息咨询服务部",	"A"	),
      hisDiskPartition(	"天津众辉劳务服务有限公司",	"C"	),
      hisDiskPartition(	"宁波市众一人力资源有限公司",	"A"	),
      hisDiskPartition(	"诸城市江山工贸有限公司",	"A"	),
      hisDiskPartition(	"长沙三鼎人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"青岛中劳广汇人力资源有限公司",	"A"	),
      hisDiskPartition(	"北京优全智汇信息技术有限公司",	"A"	),
      hisDiskPartition(	"吉林省亚设人力资源服务有限责任公司",	"A"	),
      hisDiskPartition(	"四川英思达人力资源管理有限公司重庆分公司",	"B"	),
      hisDiskPartition(	"廊坊市新华劳务派遣有限公司",	"C"	),
      hisDiskPartition(	"山东众润人力资源有限公司",	"C"	),
      hisDiskPartition(	"河北卫人人力资源开发有限公司",	"A"	),
      hisDiskPartition(	"上海瑟悟网络科技有限公司",	"A"	),
      hisDiskPartition(	"高密市海能人力资源有限公司",	"A"	),
      hisDiskPartition(	"中智安徽经济技术合作有限公司",	"A"	),
      hisDiskPartition(	"九江市桂达人力资源有限公司",	"A"	),
      hisDiskPartition(	"北京震兴东方劳务服务有限公司",	"A"	),
      hisDiskPartition(	"湖南嘉英人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"青岛零伍叁贰人力资源有限公司",	"C"	),
      hisDiskPartition(	"临沂智贤人力资源有限公司",	"B"	),
      hisDiskPartition(	"宁波三泰劳务派遣服务有限公司萧山分公司",	"B"	),
      hisDiskPartition(	"济宁市逸凡人力资源有限责任公司",	"A"	),
      hisDiskPartition(	"漳州锦杰劳务派遣有限公司",	"B"	),
      hisDiskPartition(	"高密市优升企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"山东联劳教育科技有限公司",	"A"	),
      hisDiskPartition(	"重庆安安杜浙企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"铜陵市天骥人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"淄博铭泰劳务派遣有限公司",	"B"	),
      hisDiskPartition(	"上海琮玺企业服务外包有限公司",	"C"	),
//      hisDiskPartition(	"中联达人力资源(山东)有限公司",	"A"	),
      hisDiskPartition(	"云南高创人才服务有限公司",	"A"	),
      hisDiskPartition(	"日照益百家企业服务有限公司",	"B"	),
      hisDiskPartition(	"湖南蓝桥人力资源集团有限公司",	"A"	),
      hisDiskPartition(	"武汉衡业人力资源有限公司",	"B"	),
      hisDiskPartition(	"重庆渝足企业管理咨询集团有限公司",	"A"	),
      hisDiskPartition(	"山东仕达永盛人力资源有限公司",	"C"	),
      hisDiskPartition(	"河南仁瑞人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"黑龙江川海人力资源服务集团有限公司",	"A"	),
      hisDiskPartition(	"内蒙古万众炜业科技环保股份公司府谷分公司",	"A"	),
      hisDiskPartition(	"深圳市思杰劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"青岛红杉思维企业管理咨询有限公司",	"B"	),
      hisDiskPartition(	"日照蓝塔企业管理咨询有限公司",	"C"	),
      hisDiskPartition(	"上海韩帛服饰有限公司",	"A"	),
      hisDiskPartition(	"临沂乐邦人力资源服务有限公司",	"B"	),
      hisDiskPartition(	"新乡市开元劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"山东韵铭人力资源有限公司",	"C"	),
      hisDiskPartition(	"河北慧思维人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"辽宁沃锐达人力资源有限公司",	"A"	),
      hisDiskPartition(	"北京三合和企业管理有限公司",	"A"	),
      hisDiskPartition(	"山西英之蓝人力资源服务有限公司",	"B"	),
      hisDiskPartition(	"广州市时代人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"德州骏骁人力资源有限公司",	"A"	),
      hisDiskPartition(	"青岛中信保诚企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"青岛晟鑫泽企业管理有限公司",	"A"	),
      hisDiskPartition(	"江苏产和企业服务有限公司",	"B"	),
      hisDiskPartition(	"漳州市利众劳务派遣有限公司",	"C"	),
      hisDiskPartition(	"嘉兴东呈人力资源有限公司",	"C"	),
      hisDiskPartition(	"江西企邦人力资源有限公司",	"A"	),
      hisDiskPartition(	"漳州市合兴人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"苏州才聘人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"中企动力服务外包淮安有限公司",	"A"	),
      hisDiskPartition(	"深圳全赢劳务派遣有限公司",	"C"	),
      hisDiskPartition(	"重庆恒至本企业管理咨询服务有限公司",	"A"	),
      hisDiskPartition(	"青岛玮玮人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"高密中联人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"江西金手指劳务派遣有限公司",	"C"	),
      hisDiskPartition(	"河南应中科技有限公司",	"C"	),
      hisDiskPartition(	"北京中锐博远人力资源顾问有限公司",	"A"	),
      hisDiskPartition(	"深圳高众实业有限公司",	"A"	),
      hisDiskPartition(	"滨州中联达人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"青岛天众营销管理有限公司",	"A"	),
      hisDiskPartition(	"齐远人力资源服务（廊坊）有限公司",	"A"	),
      hisDiskPartition(	"南京苏程人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"山东省工联在线人力资源有限公司",	"A"	),
      hisDiskPartition(	"通州区川姜镇裸眠家纺经营部",	"A"	),
      hisDiskPartition(	"山东大德永盛企业管理有限公司",	"B"	),
      hisDiskPartition(	"保定旌铭人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"长沙冀森人力资源有限公司",	"A"	),
      hisDiskPartition(	"桐乡广源人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"淮安方元人力资源有限公司",	"A"	),
      hisDiskPartition(	"北京聚才振邦企业管理顾问有限公司",	"B"	),
      hisDiskPartition(	"山东绿迅人力资源有限公司",	"C"	),
      hisDiskPartition(	"山东联企人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"青岛众冠企业管理服务有限公司",	"A"	),
      hisDiskPartition(	"山东汇创人力资源有限公司",	"C"	),
      hisDiskPartition(	"重庆光音企业管理有限公司",	"B"	),
      hisDiskPartition(	"陕西诚邦人力资源有限公司",	"A"	),
      hisDiskPartition(	"东莞韵翔装卸搬运服务有限公司",	"C"	),
      hisDiskPartition(	"濮阳市中劳人力资源服务有限公司",	"B"	),
      hisDiskPartition(	"福建博厚人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"菏泽联业人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"武汉九型人力资源有限公司",	"C"	),
//      hisDiskPartition(	"北京翔乐丽通人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"成都青汇百川企业管理有限公司",	"A"	),
      hisDiskPartition(	"河北和正人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"德州兆旭人力资源有限公司",	"A"	),
      hisDiskPartition(	"绿能中环(武汉)科技有限公司",	"A"	),
      hisDiskPartition(	"青岛玖创盛业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"青岛英智博人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"企服（北京）科技有限公司",	"A"	),
      hisDiskPartition(	"青岛中骏锦诚投资管理有限公司",	"C"	),
      hisDiskPartition(	"中山市永耀人力资源服务有限公司",	"B"	),
      hisDiskPartition(	"包头高新人力资源派遣有限责任公司",	"A"	),
      hisDiskPartition(	"河北云千人力资源有限公司",	"C"	),
      hisDiskPartition(	"河南华辰人力资源服务有限公司",	"B"	),
      hisDiskPartition(	"深圳市百裕劳务分包有限公司",	"A"	),
      hisDiskPartition(	"湖南慧睿管理咨询有限公司",	"A"	),
      hisDiskPartition(	"天津百姓人力资源服务有限公司",	"B"	),
      hisDiskPartition(	"山东众信盈通企业管理有限公司",	"A"	),
      hisDiskPartition(	"广西华千谷旅游开发有限公司",	"A"	),
      hisDiskPartition(	"沈阳戴弗德人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"重庆玺扬阳企业管理有限公司",	"B"	),
      hisDiskPartition(	"中岳企业管理（山东）有限公司",	"C"	),
      hisDiskPartition(	"青岛圣利安人力资源有限公司",	"A"	),
      hisDiskPartition(	"山东沃聘实业有限公司",	"A"	),
      hisDiskPartition(	"河南省淇正人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"内蒙古卓悦人力资源服务有限责任公司",	"A"	),
      hisDiskPartition(	"天津思诚企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"常州明盛服务外包有限公司",	"A"	),
      hisDiskPartition(	"长沙嘉英人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"苏州市捷之诚企业服务有限公司",	"A"	),
      hisDiskPartition(	"青岛中汇成人力资源有限公司",	"A"	),
      hisDiskPartition(	"徐州晶浩企业管理服务有限公司",	"A"	),
      hisDiskPartition(	"聊城市首创企业管理咨询有限公司",	"B"	),
      hisDiskPartition(	"华秀劳务派遣(上海)有限公司",	"A"	),
      hisDiskPartition(	"天津文鼎劳务派遣有限公司",	"B"	),
      hisDiskPartition(	"常州智伴智能科技有限公司",	"A"	),
      hisDiskPartition(	"亲亲小保（北京金色华勤数据服务有限公司）",	"A"	),
      hisDiskPartition(	"邢台耀航人力资源服务有限公司",	"B"	),
      hisDiskPartition(	"重庆贝贤人力资源管理咨询有限公司",	"A"	),
      hisDiskPartition(	"青岛汇安人力资源有限公司",	"A"	),
      hisDiskPartition(	"山东优途人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"武汉和诚义建筑劳务有限公司",	"B"	),
      hisDiskPartition(	"福建富民云外包服务有限公司",	"A"	),
      hisDiskPartition(	"保定务本人力资源服务有限公司",	"B"	),
      hisDiskPartition(	"大连弘基人力资源有限公司",	"A"	),
      hisDiskPartition(	"武汉福汉德汽车零部件有限公司",	"A"	),
      hisDiskPartition(	"河南智信劳务派遣服务有限公司",	"A"	),
      hisDiskPartition(	"济宁宏通人力资源有限公司",	"C"	),
      hisDiskPartition(	"青岛企无忧管理服务有限公司",	"C"	),
      hisDiskPartition(	"四川才库人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"宿迁联业人力资源有限公司",	"C"	),
      hisDiskPartition(	"曹县山海源人力资源发展有限公司",	"A"	),
      hisDiskPartition(	"江苏晨露企业发展服务有限公司",	"A"	),
      hisDiskPartition(	"河北鑫桥人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"重庆汇通世纪人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"合肥骐骥汽车销售服务有限公司",	"A"	),
      hisDiskPartition(	"泉州蓝服人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"河南联业人力资源有限公司",	"B"	),
      hisDiskPartition(	"深圳市顺程劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"青岛君诺人力资源有限公司",	"A"	),
      hisDiskPartition(	"广州市集群车宝汽车服务连锁有限公司",	"A"	),
      hisDiskPartition(	"珠海市汇英人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"石家庄森海人力资源服务有限公司高邑分公司",	"A"	),
      hisDiskPartition(	"保定领贤人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"青岛智名劳务服务有限公司",	"A"	),
      hisDiskPartition(	"成都万华永盛企业管理有限公司",	"C"	),
      hisDiskPartition(	"山东志德人力资源有限公司",	"A"	),
      hisDiskPartition(	"新乡市三鑫人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"湖北融智兴业人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"空间无限人力资源管理顾问有限公司",	"A"	),
      hisDiskPartition(	"河南贤泽人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"北京德众汇才企业管理有限公司",	"B"	),
      hisDiskPartition(	"淮安利晟人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"包头市汇德人力资源有限公司",	"A"	),
      hisDiskPartition(	"无锡市慷扬企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"潍坊市正宏人力资源服务有限公司",	"B"	),
      hisDiskPartition(	"广东友力智能科技有限公司",	"A"	),
      hisDiskPartition(	"青岛金诺诚人力资源有限公司",	"A"	),
      hisDiskPartition(	"河北赤诚人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"青岛众诚和企业管理咨询有限公司",	"B"	),
      hisDiskPartition(	"青岛易博人力资源管理有限公司济南分公司",	"A"	),
      hisDiskPartition(	"河北润盈人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"湖南天池家政产业管理有限公司",	"C"	),
      hisDiskPartition(	"湖南天纳宏邦企业服务有限公司",	"A"	),
      hisDiskPartition(	"三河市众合智佳企业管理有限公司",	"B"	),
      hisDiskPartition(	"北京百思特人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"青岛恒晟人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"常州德轩人力资源有限公司",	"A"	),
      hisDiskPartition(	"甘肃安博人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"贵阳久泰锦云劳务有限公司",	"A"	),
      hisDiskPartition(	"河北建博人力资源有限公司",	"C"	),
      hisDiskPartition(	"智汇方圆人才服务（江苏）有限公司",	"B"	),
      hisDiskPartition(	"佛山市旭辉五金发展有限公司",	"B"	),
      hisDiskPartition(	"杭州佳才人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"河南天河保安服务有限公司",	"A"	),
      hisDiskPartition(	"武汉优德人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"湖南中仁人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"滨州川海聚英人力资源有限公司",	"A"	),
      hisDiskPartition(	"青岛云顶人力资源有限公司",	"B"	),
      hisDiskPartition(	"青岛中企联人力资源开发有限公司",	"A"	),
      hisDiskPartition(	"江苏鼎峰机电设备有限公司",	"C"	),
//      hisDiskPartition(	"合肥启路人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"天津新诺人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"湖南创亿人力资源有限责任公司",	"C"	),
      hisDiskPartition(	"湖南才智管理咨询有限公司",	"B"	),
      hisDiskPartition(	"上海仕优劳务派遣有限公司",	"C"	),
      hisDiskPartition(	"江西红日人力资源有限公司余干分公司",	"A"	),
      hisDiskPartition(	"天津振华祥平玻璃有限公司",	"A"	),
      hisDiskPartition(	"山东中企泰盛人力资源有限公司",	"C"	),
      hisDiskPartition(	"宁夏瑞力坤人力资源有限公司",	"B"	),
      hisDiskPartition(	"福建省利锋人力资源有限公司",	"A"	),
      hisDiskPartition(	"山东友邦人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"河北旭尧人力资源服务有限责任公司",	"A"	),
      hisDiskPartition(	"淄博众诚人力资源有限公司",	"C"	),
      hisDiskPartition(	"河北简凡人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"保定市铭基劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"北京鑫众联合企业管理有限公司",	"A"	),
      hisDiskPartition(	"宁夏庆隆裕企业管理有限公司",	"C"	),
      hisDiskPartition(	"昆明厚致百盈企业管理有限公司",	"A"	),
      hisDiskPartition(	"日照顺和物流有限公司",	"A"	),
      hisDiskPartition(	"青岛笃信贤人力资源有限公司",	"A"	),
      hisDiskPartition(	"河南万邦人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"深圳三惠人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"珠海人多多人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"长治市职星多企业管理咨询有限公司",	"C"	),
      hisDiskPartition(	"青岛华夏通人力资源有限公司",	"C"	),
      hisDiskPartition(	"青岛紫玥人力资源有限公司",	"A"	),
      hisDiskPartition(	"宁夏劳联人力资源服务连锁有限公司",	"A"	),
      hisDiskPartition(	"广州市精诚人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"山东华三人力资源有限公司",	"C"	),
      hisDiskPartition(	"日照市合发人力资源有限公司",	"A"	),
      hisDiskPartition(	"曹县华中人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"重庆威凌盾企业管理咨询有限公司",	"C"	),
      hisDiskPartition(	"北京蓝箭空间科技有限公司",	"A"	),
      hisDiskPartition(	"山东百纳人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"河北凯盛医药科技有限公司",	"A"	),
      hisDiskPartition(	"青岛聚宏达劳务服务有限公司",	"C"	),
      hisDiskPartition(	"义乌市英蓝人力资源有限公司",	"A"	),
      hisDiskPartition(	"河北亚际会人力资源有限公司",	"C"	),
      hisDiskPartition(	"河北善能机电设备安装工程有限公司",	"A"	),
      hisDiskPartition(	"夏津众合至诚劳动服务有限公司",	"A"	),
      hisDiskPartition(	"广州市浩洋人力资源有限公司",	"B"	),
      hisDiskPartition(	"辽宁北松人力资源有限公司",	"A"	),
      hisDiskPartition(	"临沂德聚仁合人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"武汉百世英才人力资源有限公司",	"A"	),
      hisDiskPartition(	"苏州金梧桐人力资源有限公司",	"A"	),
      hisDiskPartition(	"山东坤润乐橙人力资源有限公司",	"B"	),
      hisDiskPartition(	"青岛卓信人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"云南邦建建筑劳务有限公司",	"A"	),
      hisDiskPartition(	"重庆千锐宏企业管理有限公司",	"A"	),
      hisDiskPartition(	"滨州市贤才管理咨询有限公司",	"A"	),
      hisDiskPartition(	"上海蜜情焙浓餐饮管理有限公司",	"A"	),
      hisDiskPartition(	"武汉职多多网络科技有限公司",	"A"	),
      hisDiskPartition(	"沈阳优才网络科技有限公司",	"B"	),
      hisDiskPartition(	"青岛泽信人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"广州翰德人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"北京外企人力资源服务济南有限公司",	"A"	),
      hisDiskPartition(	"江苏贤达人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"湖南和泽人力资源有限公司",	"A"	),
      hisDiskPartition(	"青岛联达人力资源有限公司",	"A"	),
//      hisDiskPartition(	"菏泽雇主保人力资源有限公司",	"A"	),
      hisDiskPartition(	"深圳市众勤商务咨询有限公司",	"A"	),
      hisDiskPartition(	"扬州点金人力资源有限公司",	"A"	),
      hisDiskPartition(	"泰安阳铭人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"南京易联碧诚人力资源顾问有限公司",	"A"	),
      hisDiskPartition(	"日照市光辉人力资源有限公司",	"A"	),
      hisDiskPartition(	"衡阳市泰合人力资源有限公司",	"A"	),
      hisDiskPartition(	"济宁市诺嘉人力资源服务有限公司",	"B"	),
      hisDiskPartition(	"山东机场吉翔航空食品有限公司",	"A"	),
      hisDiskPartition(	"北京福得利企业管理服务有限公司",	"A"	),
      hisDiskPartition(	"天津民安人力资源有限公司",	"A"	),
      hisDiskPartition(	"安徽京玺人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"济南海乐人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"苏州石博士石材养护工程有限公司",	"A"	),
      hisDiskPartition(	"高邮市少游企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"武汉师洋飞人力资源有限公司",	"A"	),
      hisDiskPartition(	"河北劳联人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"南昌鸣晨人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"南京斐洛企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"钰琨轩人力资源服务(重庆)有限公司",	"A"	),
      hisDiskPartition(	"江苏中毅德人力资源有限公司",	"A"	),
      hisDiskPartition(	"济南中仁企业管理有限公司",	"A"	),
      hisDiskPartition(	"嘉兴市骏通人力资源有限公司",	"A"	),
      hisDiskPartition(	"重庆腾迈人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"厦门蓝服服务外包有限公司",	"A"	),
      hisDiskPartition(	"青岛易锦宏企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"河南鸿盛人力资源咨询服务有限公司",	"A"	),
      hisDiskPartition(	"南京九诚劳务服务有限公司",	"C"	),
      hisDiskPartition(	"东莞市粤达人力资源管理咨询有限公司",	"A"	),
      hisDiskPartition(	"重庆东胜企业管理咨询有限公司",	"A"	),
//      hisDiskPartition(	"淄博晟博人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"青岛冠仕隆实业有限公司",	"B"	),
      hisDiskPartition(	"广州华智人力资源服务有限公司",	"A"	),
//      hisDiskPartition(	"烟台珢瑆工程机械有限公司",	"A"	),
      hisDiskPartition(	"山东汇森人力资源有限公司",	"A"	),
      hisDiskPartition(	"武汉威捷凯物流有限公司",	"A"	),
      hisDiskPartition(	"上海裕夫建筑工程服务中心",	"A"	),
      hisDiskPartition(	"广州市标顶餐饮管理有限公司",	"A"	),
      hisDiskPartition(	"河北君澈人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"临沂雇主无忧企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"无锡九方皋企业管理咨询有限公司",	"C"	),
      hisDiskPartition(	"嘉兴巨企人力资源有限公司",	"A"	),
      hisDiskPartition(	"泉州启航人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"东莞市星韵搬运装卸服务有限公司",	"A"	),
      hisDiskPartition(	"北京应中科技有限公司",	"A"	),
      hisDiskPartition(	"湖南融丰人力资源有限公司",	"A"	),
      hisDiskPartition(	"招远平安人力劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"青岛汉为企业管理服务有限公司",	"A"	),
      hisDiskPartition(	"海南大凯消防安全工程有限公司",	"A"	),
      hisDiskPartition(	"觅职猫科技（山东）有限公司",	"A"	),
      hisDiskPartition(	"河北昆仑保安服务有限公司",	"C"	),
      hisDiskPartition(	"沐山科技（天津）有限责任公司",	"C"	),
      hisDiskPartition(	"重庆聚亿森人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"潍坊展宇人力资源有限公司",	"A"	),
      hisDiskPartition(	"淄博众华人力资源有限公司",	"C"	),
      hisDiskPartition(	"北京诚鑫万荣咨询服务有限公司",	"A"	),
//      hisDiskPartition(	"青岛万航管理服务有限公司",	"A"	),
      hisDiskPartition(	"佛山市南海区九江灿锋家具配件厂",	"A"	),
      hisDiskPartition(	"常州起跑线人力资源有限公司",	"A"	),
      hisDiskPartition(	"江西产联商贸有限公司",	"A"	),
      hisDiskPartition(	"河南省金銮劳务服务有限公司",	"A"	),
      hisDiskPartition(	"上海卓民建设工程有限公司",	"A"	),
      hisDiskPartition(	"山东千里马人力资源有限公司",	"A"	),
      hisDiskPartition(	"禹州市宏达建筑有限责任公司",	"A"	),
      hisDiskPartition(	"青岛顺怡航物流有限公司",	"A"	),
      hisDiskPartition(	"北京长平建筑工程有限公司",	"A"	),
      hisDiskPartition(	"北京伏牛堂餐饮文化有限公司",	"B"	),
      hisDiskPartition(	"扬州锐熠人力资源有限公司",	"A"	),
      hisDiskPartition(	"齐远人力资源服务(廊坊)有限公司",	"A"	),
      hisDiskPartition(	"中山市报捷人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"石家庄圣业劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"苏州宝基外包服务有限公司",	"A"	),
      hisDiskPartition(	"黑龙江中浩人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"重庆盟合人力资源有限公司",	"B"	),
      hisDiskPartition(	"深圳市前海一脉人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"常熟市鸿丰纺织厂",	"A"	),
      hisDiskPartition(	"深圳市装业劳动服务有限公司",	"A"	),
      hisDiskPartition(	"遂平县三禾人力资源有限公司",	"A"	),
      hisDiskPartition(	"佛山市玖邦家具有限公司",	"A"	),
      hisDiskPartition(	"康保县众联易达人力资源开发有限公司",	"A"	),
      hisDiskPartition(	"江西省金盾人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"任丘市保联网络科技有限公司",	"A"	),
      hisDiskPartition(	"曹县鼎盛人力资源有限公司",	"A"	),
      hisDiskPartition(	"山东中烁人力资源有限公司",	"C"	),
      hisDiskPartition(	"顺德区杏坛镇信朝木制品加工厂",	"A"	),
//      hisDiskPartition(	"重庆中辉企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"广西正大人力资源有限公司",	"A"	),
      hisDiskPartition(	"山东文鼎人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"湖南智财网络科技有限公司",	"A"	),
      hisDiskPartition(	"济宁汇聚人力资源有限公司",	"C"	),
//      hisDiskPartition(	"青岛旭松康大食品有限公司",	"A"	),
      hisDiskPartition(	"陕西凌旭建筑工程有限公司",	"A"	),
      hisDiskPartition(	"上海薪木人力资源有限公司",	"A"	),
      hisDiskPartition(	"襄阳世捷人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"荆州聚鑫人力资源有限公司",	"A"	),
      hisDiskPartition(	"青岛万荣环保工程设备有限公司",	"A"	),
      hisDiskPartition(	"龙口市资联人力资源有限公司",	"A"	),
      hisDiskPartition(	"安阳市正品人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"北京驿府酒店控股有限公司",	"A"	),
//      hisDiskPartition(	"长沙湘智人力资源管理有限公司长沙县分公司",	"A"	),
      hisDiskPartition(	"广州市番禺华丰制衣洗水有限公司",	"A"	),
      hisDiskPartition(	"清远市敬天香食品有限公司",	"B"	),
      hisDiskPartition(	"无锡翼东劳务派遣服务有限公司",	"A"	),
      hisDiskPartition(	"山西壹元商务信息咨询服务有限公司",	"A"	),
      hisDiskPartition(	"铜仁市制衡人力资源保障有限公司",	"A"	),
      hisDiskPartition(	"青岛海普教育咨询有限公司",	"A"	),
      hisDiskPartition(	"石家庄易尚人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"青岛智泽人力资源有限公司",	"A"	),
      hisDiskPartition(	"重庆天竹人力资源管理有限公司",	"B"	),
      hisDiskPartition(	"青岛恒丰人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"山东慧源人力资源有限公司",	"A"	),
      hisDiskPartition(	"江西省同济人力资源集团有限公司",	"A"	),
      hisDiskPartition(	"安徽才联人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"青岛鸿运前程劳务有限公司",	"C"	),
      hisDiskPartition(	"重庆宜水企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"鱼台汇思前程人力资源有限公司",	"C"	),
      hisDiskPartition(	"公司",	"A"	),
      hisDiskPartition(	"山东盛嘉园汽车综合服务有限公司",	"A"	),
      hisDiskPartition(	"山东嘉豪电子商务有限公司",	"C"	),
      hisDiskPartition(	"广州有道人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"北京佳强保安服务有限公司京鑫分公司",	"A"	),
      hisDiskPartition(	"济南清扬物业管理有限公司",	"A"	),
      hisDiskPartition(	"新乡市鼎盛人力资源开发有限公司",	"A"	),
      hisDiskPartition(	"青岛鑫诚劳联劳务有限公司",	"A"	),
      hisDiskPartition(	"北京公瑾科技有限公司",	"A"	),
      hisDiskPartition(	"湖南华顺人力资源服务管理股份有限公司",	"B"	),
      hisDiskPartition(	"苏州唯怡卓网络科技有限公司",	"A"	),
      hisDiskPartition(	"天津汇智东方人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"晶品服务外包（江苏）有限公司",	"A"	),
      hisDiskPartition(	"潍坊文博机械有限公司",	"A"	),
      hisDiskPartition(	"陕西璀璨建设工程有限公司",	"A"	),
      hisDiskPartition(	"苏州聚德晟企业管理有限公司",	"A"	),
//      hisDiskPartition(	"河北祥锐人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"荣成市崖头中劳职业介绍所",	"A"	),
      hisDiskPartition(	"武汉奈斯人力资源有限公司",	"A"	),
      hisDiskPartition(	"山东众企汇企业服务有限公司",	"A"	),
      hisDiskPartition(	"长沙九兄弟人力资源有限公司",	"A"	),
      hisDiskPartition(	"邦芒服务外包（苏州）有限公司",	"A"	),
      hisDiskPartition(	"苏州欧唛圣机电科技有限公司",	"A"	),
      hisDiskPartition(	"襄阳市金升劳务有限公司",	"A"	),
      hisDiskPartition(	"保准牛测试",	"A"	),
      hisDiskPartition(	"江西赣成人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"深圳市敏捷人力资源管理有限公司",	"A"	),
//      hisDiskPartition(	"唐山勋臣人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"招远瑞林建筑工程有限公司",	"A"	),
//      hisDiskPartition(	"淄博泽通人力资源有限公司",	"A"	),
      hisDiskPartition(	"沧州市晶华玻璃制品有限公司",	"A"	),
      hisDiskPartition(	"重庆百辉人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"武汉冠鹰人力资源有限公司",	"A"	),
      hisDiskPartition(	"青岛扬子国际经济技术合作有限公司",	"A"	),
      hisDiskPartition(	"昆山沿沪劳务派遣有限公司",	"B"	),
      hisDiskPartition(	"山东燕翔人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"湖北搭夥人力资源有限公司",	"A"	),
      hisDiskPartition(	"北京方胜人才服务有限公司",	"A"	),
      hisDiskPartition(	"武汉众聘人力资源有限公司",	"A"	),
      hisDiskPartition(	"同行保险经纪有限公司淄博分公司",	"A"	),
      hisDiskPartition(	"邯郸冀南新区蓬辉建筑工程有限公司",	"A"	),
      hisDiskPartition(	"南京霖联企业管理有限公司",	"A"	),
      hisDiskPartition(	"南昌高新人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"广州甲骨基华企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"蓝箭航天技术有限公司",	"A"	),
      hisDiskPartition(	"重庆信力畅企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"徐州青创人力资源开发有限公司",	"A"	),
      hisDiskPartition(	"武汉万象启航人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"武汉吉昌人力资源有限公司",	"A"	),
      hisDiskPartition(	"山东红海人力资源有限公司",	"A"	),
      hisDiskPartition(	"武汉荟杰人力资源有限公司",	"A"	),
      hisDiskPartition(	"东莞汇鑫货运代理有限公司",	"A"	),
      hisDiskPartition(	"湖南湘酬人力资源有限公司",	"A"	),
      hisDiskPartition(	"沙河市华锦德玻璃制品有限公司",	"A"	),
      hisDiskPartition(	"青岛华兴鲁强劳务服务有限公司",	"A"	),
      hisDiskPartition(	"陕西赵雅体育文化传播有限公司",	"A"	),
      hisDiskPartition(	"诚望服务外包(昆山)有限公司",	"B"	),
      hisDiskPartition(	"山东劳联旺福浩人力资源有限公司",	"A"	),
      hisDiskPartition(	"上海瑞方企业管理有限公司",	"A"	),
      hisDiskPartition(	"山东佳园物业发展有限公司济宁分公司",	"A"	),
      hisDiskPartition(	"北京信慧行商务服务有限公司",	"A"	),
//      hisDiskPartition(	"邯郸市蓝翎无忧人力资源有限公司",	"A"	),
      hisDiskPartition(	"北京鑫海博达劳务派遣有限公司",	"B"	),
      hisDiskPartition(	"威驰盛联服务外包(常州)有限公司",	"A"	),
//      hisDiskPartition(	"青岛佳程佳经济信息咨询有限公司",	"A"	),
      hisDiskPartition(	"中人(北京)人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"江西我当家科技服务有限公司",	"A"	),
      hisDiskPartition(	"招远市北方化工有限公司",	"A"	),
      hisDiskPartition(	"苏州巴洛克建筑装饰工程有限公司",	"A"	),
      hisDiskPartition(	"东莞市盛唐包装技术有限公司",	"A"	),
      hisDiskPartition(	"江西铜城人力资源有限责任公司",	"A"	),
      hisDiskPartition(	"苏州涌智企业服务有限公司",	"A"	),
      hisDiskPartition(	"北京市荣满技术服务有限公司",	"A"	),
      hisDiskPartition(	"杭州恒达瓶业有限公司",	"A"	),
      hisDiskPartition(	"广州穗百人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"临沂志恒人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"武汉江南世纪人力资源集团有限公司",	"A"	),
      hisDiskPartition(	"常州天朋人力资源有限公司",	"A"	),
      hisDiskPartition(	"米阳物联网技术（嘉兴）有限公司",	"A"	),
      hisDiskPartition(	"青岛仟佰职人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"丹阳市智深人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"山东兴福好纸品有限公司",	"A"	),
      hisDiskPartition(	"银顿建设(北京)有限公司",	"A"	),
      hisDiskPartition(	"贵州森悦宏谦劳务有限公司",	"A"	),
      hisDiskPartition(	"宁波英博人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"深圳市鹏鑫建筑劳务有限公司",	"A"	),
      hisDiskPartition(	"山东昊达劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"深圳市鑫众源保洁服务有限公司",	"A"	),
      hisDiskPartition(	"上海天盒餐饮管理有限公司",	"A"	),
      hisDiskPartition(	"上海众山劳务派遣服务有限公司",	"A"	),
      hisDiskPartition(	"北京永盛劳务派遣有限公司",	"C"	),
      hisDiskPartition(	"北京京都护卫保安服务有限公司",	"C"	),
//      hisDiskPartition(	"广州云宝号首饰有限公司",	"A"	),
      hisDiskPartition(	"宁波将遇良才人力资源有限公司",	"A"	),
//      hisDiskPartition(	"广州集群车宝汽车服务有限公司",	"A"	),
      hisDiskPartition(	"阜阳兴阜人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"苏州英仕达人力资源有限公司",	"A"	),
      hisDiskPartition(	"北京万古恒信科技有限公司",	"A"	),
      hisDiskPartition(	"深圳市景泰诚人力资源有限公司",	"A"	),
      hisDiskPartition(	"深圳市卓越人劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"厦门连盟劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"亿卓人力资源管理咨询（重庆）有限公司",	"A"	),
      hisDiskPartition(	"南京富高家政服务有限公司",	"A"	),
      hisDiskPartition(	"烟台万众人力资源有限公司",	"A"	),
      hisDiskPartition(	"深圳市博宇文化发展有限公司",	"A"	),
      hisDiskPartition(	"山东远洋人力资源有限公司",	"A"	),
      hisDiskPartition(	"重庆振华保安服务有限公司",	"B"	),
      hisDiskPartition(	"吉林省国诚大药房连锁有限公司",	"A"	),
      hisDiskPartition(	"常州华致赢企业管理有限公司",	"A"	),
      hisDiskPartition(	"重庆嘉钡思企业管理咨询有限公司",	"C"	),
      hisDiskPartition(	"上海帅松劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"荆州市赛服人力资源有限公司",	"A"	),
      hisDiskPartition(	"广州市穗邦人力资源有限公司",	"A"	),
      hisDiskPartition(	"青岛劳联华凯人力资源有限公司",	"A"	),
      hisDiskPartition(	"内蒙古中创人力资源产业发展有限公司",	"A"	),
      hisDiskPartition(	"沈阳名仕人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"鄂州葛店开发区鼎胜劳务服务有限公司",	"A"	),
      hisDiskPartition(	"山东中战人力资源有限公司",	"C"	),
      hisDiskPartition(	"徐州汉鼎建筑劳务有限公司",	"A"	),
      hisDiskPartition(	"广州市国鹏货运代理有限公司",	"A"	),
      hisDiskPartition(	"福建省泉州市助众人力资源发展有限公司",	"A"	),
      hisDiskPartition(	"上海靖蕙企业服务有限公司",	"A"	),
      hisDiskPartition(	"河南仕林人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"深圳鹏讯网络信息有限公司",	"A"	),
      hisDiskPartition(	"南安市三一企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"苏州元聚贤企业管理服务有限公司",	"A"	),
      hisDiskPartition(	"金华才享人力资源有限公司",	"A"	),
      hisDiskPartition(	"湖南广鑫人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"湖北智财网络技术有限公司",	"A"	),
      hisDiskPartition(	"恒超源洗净科技（深圳）有限公司",	"A"	),
      hisDiskPartition(	"山东诚德祥人力资源服务有限公司新乡分公司",	"A"	),
      hisDiskPartition(	"河北诺亚人力资源开发有限公司",	"A"	),
      hisDiskPartition(	"武汉泰晟元人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"青岛盛川人力资源有限公司",	"A"	),
      hisDiskPartition(	"华秀劳务派遣（上海）有限公司",	"A"	),
      hisDiskPartition(	"青岛瑞成九洲企业管理有限公司成武分公司",	"A"	),
      hisDiskPartition(	"苏州市隆兴消防设备有限公司",	"A"	),
      hisDiskPartition(	"安徽优之才人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"惠州市驰程人力资源有限公司",	"A"	),
      hisDiskPartition(	"上海骋顺企业管理有限公司",	"A"	),
      hisDiskPartition(	"常熟通威无纺制品有限公司",	"C"	),
      hisDiskPartition(	"诚望服务外包（昆山）有限公司",	"A"	),
      hisDiskPartition(	"昆山广斌企业管理有限公司",	"A"	),
//      hisDiskPartition(	"赣州市民兴人力资源有限公司",	"A"	),
      hisDiskPartition(	"深圳市鲁豫劳务派遣有限公司",	"A"	),
//      hisDiskPartition(	"山东蓝桥人力资源服务有限公司",	"A"	),
//      hisDiskPartition(	"上海农圣农业发展有限公司",	"A"	),
      hisDiskPartition(	"东莞市鸿泰劳务派遣有限公司",	"A"	),
      hisDiskPartition(	"深圳市鹏讯网络科技有限公司",	"A"	),
      hisDiskPartition(	"株洲易栈产业服务有限公司",	"A"	),
//      hisDiskPartition(	"广东水玲珑商务服务有限公司",	"A"	),
      hisDiskPartition(	"株洲翰林人力资源有限责任公司",	"A"	),
      hisDiskPartition(	"山东丰卓建筑工程有限公司",	"A"	),
      hisDiskPartition(	"济宁市驰骋人力资源有限责任公司",	"A"	),
      hisDiskPartition(	"常熟市华之鑫汽车配件有限公司",	"A"	),
//      hisDiskPartition(	"山东天成人力资源有限公司",	"A"	),
//      hisDiskPartition(	"河南豫鲁源商业运营管理有限公司",	"A"	),
      hisDiskPartition(	"江西壹号农业科技有限公司",	"A"	),
      hisDiskPartition(	"珠海展望物流有限公司",	"A"	),
      hisDiskPartition(	"沂水劳联劳务服务有限公司",	"A"	),
      hisDiskPartition(	"南京常捷防水工程有限公司",	"C"	),
      hisDiskPartition(	"湖南榕聚企业管理有限公司",	"A"	),
      hisDiskPartition(	"北京永昌楼宇保洁服务有限公司",	"A"	),
      hisDiskPartition(	"赣州人事易人力资源管理服务有限公司",	"A"	),
      hisDiskPartition(	"海安缘林展示展览有限公司",	"A"	),
      hisDiskPartition(	"青岛一诚一实业有限公司",	"A"	),
      hisDiskPartition(	"欧瑞德(廊坊)人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"深圳保保人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"通州区平潮镇有为金属制品厂",	"A"	),
      hisDiskPartition(	"武汉金蚂蚁建筑劳务有限公司",	"C"	),
      hisDiskPartition(	"济宁汇鑫人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"重庆优盾建筑防水工程有限公司",	"C"	),
      hisDiskPartition(	"潍坊雨安防水工程有限公司",	"A"	),
      hisDiskPartition(	"东莞市骆银机械工程有限公司",	"A"	),
      hisDiskPartition(	"重庆新起点人力资源服有限公司",	"A"	),
      hisDiskPartition(	"上海闽岳劳务派遣有限公司渭南分公司",	"A"	),
      hisDiskPartition(	"湖南粤盛人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"衢州吴泽施工劳务有限公司",	"A"	),
      hisDiskPartition(	"广州龙鑫蓄热工业炉有限公司",	"A"	),
      hisDiskPartition(	"上海几加文化传播发展有限公司",	"A"	),
      hisDiskPartition(	"浙江榆阳电子有限公司",	"A"	),
      hisDiskPartition(	"青岛翰廷工业技术有限公司",	"A"	),
      hisDiskPartition(	"衡阳市华智人力资源有限公司",	"A"	),
      hisDiskPartition(	"北京迈恩特企业管理服务有限公司",	"A"	),
      hisDiskPartition(	"北京京信泰富科技有限公司",	"A"	),
      hisDiskPartition(	"无锡热点信息科技有限公司",	"C"	),
      hisDiskPartition(	"湖南怡美环境管理发展有限公司",	"C"	),
      hisDiskPartition(	"湖南尚玖人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"中山市盟富人力资源管理有限公司",	"A"	),
      hisDiskPartition(	"青岛众鑫天诚企业管理咨询有限公司",	"A"	),
      hisDiskPartition(	"江苏博傲木业有限公司",	"A"	),
//      hisDiskPartition(	"山东省博兴县锦隆钢板有限公司",	"A"	),
      hisDiskPartition(	"河南中原防水防腐保温工程有限公司",	"A"	),
      hisDiskPartition(	"江苏兰尔博人力资源有限公司",	"A"	),
      hisDiskPartition(	"北京国信领航人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"张家口仕诚人力资源服务有限公司",	"C"	),
      hisDiskPartition(	"天津元发劳务服务有限公司",	"A"	),
//      hisDiskPartition(	"衡阳市锦捷人力资源有限公司",	"A"	),
      hisDiskPartition(	"逗霸（上海）电子商务有限公司",	"A"	),
      hisDiskPartition(	"沙河市安玻玻璃制品有限公司",	"A"	),
      hisDiskPartition(	"诸城市鹏鑫精密机械厂",	"A"	),
      hisDiskPartition(	"太康县伯乐企业管理服务有限公司",	"A"	),
      hisDiskPartition(	"青岛瑞安兴茂实业有限公司",	"A"	),
      hisDiskPartition(	"广西汇天昌劳务有限公司",	"A"	),
//      hisDiskPartition(	"江苏学新信息科技有限公司",	"A"	),
      hisDiskPartition(	"石家庄市龙威运输有限公司",	"A"	),
      hisDiskPartition(	"济南众邦人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"河南蓝图人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"河南嘉图人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"中山市双德人力资源有限公司",	"A"	),
      hisDiskPartition(	"北京德知航创科技有限责任公司",	"A"	),
      hisDiskPartition(	"高密市孚信人力资源服务有限公司",	"A"	),
//      hisDiskPartition(	"夏津云泰互联人力资源有限公司",	"A"	),
      hisDiskPartition(	"武汉九型管理咨询有限公司",	"A"	),
      hisDiskPartition(	"南昌力琪机械制造有限公司",	"A"	),
      hisDiskPartition(	"广西欣龙预拌混凝土有限公司",	"A"	),
//      hisDiskPartition(	"天津市益鑫人力资源服务有限公司",	"A"	),
      hisDiskPartition(	"山东滨惠安装工程有限公司",	"A"	),
      hisDiskPartition(	"安徽启源企业管理服务有限公司",	"C"	),
      hisDiskPartition(	"北京众信和诚企业管理有限公司",	"A"	),
      hisDiskPartition(	"湖北捷聘人力资源有限公司",	"A"	),
      hisDiskPartition(	"爱玛客服务产业(中国)有限公司廊坊分公司",	"A"	),
      hisDiskPartition(	"保定俱旺人力资源有限公司",	"A"	)
    )

    hisDiskPartitionData.toDF().registerTempTable("hisDiskPartitionDf")

    /**
      * 基础数据
      */
    sqlContext.sql(
      """
        |with channel_region as (
        |select * from (
        |select channel_name as channel_name_rand,holder_name,holder_province,holder_city,
        |row_number() over (partition by channel_name,holder_name order by policy_start_date desc) as rand1
        |from dwdb.dw_employer_baseinfo_detail
        |where insure_company_short_name = '国寿财'
        |and product_code not in ('17000001','LGB000001')
        |) t
        |where t.rand1 = 1
        |)
        |
        |select a.*,b.*,c.*,d.*,e.*,f.*
        |from
        |(
        |select policy_id,policy_code,holder_name as ent_name,channel_name,sku_coverage,
        |sku_charge_type,sku_price,sku_ratio,sku_append,sale_name,team_name,biz_operator,business_source,
        |consumer_category,policy_start_date,policy_end_date,holder_province,holder_city,big_policy
        |from dwdb.dw_employer_baseinfo_detail
        |where insure_company_short_name = '国寿财'
        |and product_code not in ('17000001','LGB000001')
        |) a
        |left join
        |(
        |select policy_code as policy_code_work,profession_type,
        |sum(case when bzn_work_risk = '1' then 1 else 0 end) as one_risk_count,
        |sum(case when bzn_work_risk = '2' then 1 else 0 end) as two_risk_count,
        |sum(case when bzn_work_risk = '3' then 1 else 0 end) as three_risk_count,
        |sum(case when bzn_work_risk = '4' then 1 else 0 end) as four_risk_count,
        |sum(case when bzn_work_risk = '5' then 1 else 0 end) as five_risk_count,
        |sum(case when bzn_work_risk = '6' then 1 else 0 end) as six_risk_count,
        |sum(case when bzn_work_risk not in ( '1','2','3','4','5','6') then 1 else 0 end) as other_risk_count
        |from dwdb.dw_work_type_matching_claim_detail
        |group by policy_code,profession_type
        |) b
        |on a.policy_code = b.policy_code_work
        |left join
        |(
        |select distinct x.channel_name_rand,y.holder_province as channel_province,y.holder_city as channel_city
        |from channel_region x
        |join channel_region y
        |on x.channel_name_rand = y.holder_name
        |) c
        |on a.channel_name = c.channel_name_rand
        |left join
        |(
        |select policy_id as policy_id_insured,sum(count) as person_count,
        |sum(case when day_id = regexp_replace(substr(cast(now() as string),1,10),'-','') then count else 0 end) as curr_insured from dwdb.dw_policy_curr_insured_detail
        |where day_id <= regexp_replace(substr(cast(now() as string),1,10),'-','')
        |group by policy_id
        |) d
        |on a.policy_id = d.policy_id_insured
        |left join
        |(
        |select policy_id as policy_id_premium,sum(premium) as charge_premium ,
        |sum(case when day_id>= '20200101' and day_id <= regexp_replace(substr(cast(now() as string),1,10),'-','') then premium else 0 end ) as 2020_to_now_charge_premium
        |from dwdb.dw_policy_everyday_premium_detail
        |where day_id <= regexp_replace(substr(cast(now() as string),1,10),'-','')
        |group by policy_id
        |) e
        |on a.policy_id = e.policy_id_premium
        |left join
        |(
        |select policy_code as policy_code_claim,sum(res_pay) as res_pay,count(case_no) as case_no,
        |sum(case when case_type = '死亡' then res_pay else 0 end) as dead_res,
        |sum(case when case_type = '伤残' then res_pay else 0 end) as dis_res,
        |sum(case when case_type = '死亡' then 1 else 0 end) as dead_case_no,
        |sum(case when case_type = '伤残' then 1 else 0 end) as dis_case_no,
        |sum(case when risk_date_res >= '2020-01-01 00:00:00' then res_pay else 0 end) as 2020_to_now_res_pay
        |from dwdb.dw_policy_claim_detail
        |group by policy_code
        |) f
        |on a.policy_code = f.policy_code_claim
      """.stripMargin)
      .registerTempTable("base_info_temp")

    sqlContext.sql(
      """
        |select x.channel_name,z.c_ten_day_long_desc,z.c_month_id,sum(z.premium) as month_charge_premium,sum(c.res_pay) as month_res_pay
        |from dwdb.dw_employer_baseinfo_detail x
        |left join
        |(
        |select a.policy_id,a.day_id,b.c_ten_day_long_desc,b.c_month_id,sum(a.premium) as premium
        |from dwdb.dw_policy_everyday_premium_detail a
        |left join odsdb.ods_t_day_dimension b
        |on a.day_id = b.c_day_id
        |group by a.policy_id,a.day_id,b.c_ten_day_long_desc,b.c_month_id
        |) z
        |on x.policy_id = z.policy_id
        |left join
        |(
        |select policy_id , regexp_replace(substr(cast(risk_date_res as string),1,10),'-','') as day_id,sum(res_pay) as res_pay from dwdb.dw_policy_claim_detail
        |group by policy_id , regexp_replace(substr(cast(risk_date_res as string),1,10),'-','')
        |) c
        |on x.policy_id = c.policy_id and z.day_id = c.day_id
        |where  x.insure_company_short_name = '国寿财' and x.product_code not in ('17000001','LGB000001')
        |and x.one_level_pdt_cate = '蓝领外包'
        |group by x.channel_name,z.c_ten_day_long_desc,z.c_month_id
      """.stripMargin)
      .registerTempTable("sum_info_temp")

    /**
      * 基础数据进行汇总，得到是否稳定、当前分盘、初投、新老客
      */
    sqlContext.sql(
      """
        |select channel_name as channel_name_sum,
        |min(policy_start_date) as first_time,
        |case when (case when sum(res_pay) is null then 0 else sum(res_pay) end)/sum(charge_premium) <= 0.7 then 'A'
        |when (case when sum(res_pay) is null then 0 else sum(res_pay) end)/sum(charge_premium) > 0.7 and (case when sum(res_pay) is null then 0 else sum(res_pay) end)/sum(charge_premium) <= 1.3 then 'B'
        |when (case when sum(res_pay) is null then 0 else sum(res_pay) end)/sum(charge_premium) > 1.3 then 'C'
        |else null end as disk_partition,
        |case when months_between(now(),min(policy_start_date)) >= 6 then '是'
        |else '否' end as is_stabilize,
        |case when min(policy_start_date) >= '2020-01-01 00:00:00' then '新客' else '老客' end as cus_type
        |from base_info_temp
        |where charge_premium > 0
        |group by channel_name
      """.stripMargin)
      .registerTempTable("cusInfoData")

    val res2 = sqlContext.sql(
      """
        |select a.*,b.*,c.*,date_format(now(),'yyyy-MM-dd HH:mm:ss') as dw_create_time from sum_info_temp a
        |left join cusInfoData b
        |on a.channel_name = b.channel_name_sum
        |left join hisDiskPartitionDf c
        |on a.channel_name = channel_name_disk
      """.stripMargin)
      .drop("channel_name_sum")
      .drop("channel_name_disk")

    /**
      * 将基础数据和汇总数据进行合并
      */
    val res1 = sqlContext.sql(
      """
        |select a.*,b.*,c.*,date_format(now(),'yyyy-MM-dd HH:mm:ss') as dw_create_time
        |from base_info_temp a
        |left join cusInfoData b
        |on a.channel_name = b.channel_name_sum
        |left join hisDiskPartitionDf c
        |on a.channel_name = c.channel_name_disk
      """.stripMargin)
      .drop("channel_name_sum")
      .drop("channel_name_rand")
      .drop("policy_id_insured")
      .drop("policy_id_premium")
      .drop("policy_code_claim")
      .drop("policy_id")
      .drop("channel_name_disk")
      .drop("policy_code_work")

    (res1,res2)
  }
}