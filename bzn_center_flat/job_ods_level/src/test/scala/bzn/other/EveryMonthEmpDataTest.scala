package bzn.other

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import bzn.dw.premium.DwPolicyInsuredDayIdDetailTest.getBeg_End_one_two
import bzn.job.common.Until
import bzn.ods.util.SparkUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:xiaoYuanRen
  * Date:2019/10/24
  * Time:20:50
  * describe: this is new class
  **/
object EveryMonthEmpDataTest extends  SparkUtil with Until{
    def main(args: Array[String]): Unit = {
    System.setProperty ("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo (appName, "")

    val sc = sparkConf._2
    val hiveContext = sparkConf._4
    dwPolicyInsuredDayIdDetail(hiveContext)
    sc.stop ()
  }
  def dwPolicyInsuredDayIdDetail(sqlContext:HiveContext) = {
    import sqlContext.implicits._
    sqlContext.udf.register ("getUUID", () => (java.util.UUID.randomUUID () + "").replace ("-", ""))
    sqlContext.udf.register ("getNow", () => {
      val df = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format (new Date ()) // new Date()为获取当前系统时间
      (date + "")
    })
    /**
      * 读取保单表
      */
    val odsPolicyDetail =
      sqlContext.sql ("select id,policy_id, policy_code, product_code, policy_start_date, policy_end_date, policy_status,holder_name from odsdb.ods_policy_detail")
        .where ("length(policy_code) > 0 and policy_status in (0,1,-1)" +
          " and policy_start_date >= '2018-01-01 00:00:00' and " +
          "policy_end_date is not null")
        .repartition (200)
        .cache ()

    /**
      * 读取产品表
      */
    val odsProductDetail =
      sqlContext.sql ("select product_code as product_code_slave, one_level_pdt_cate from odsdb.ods_product_detail")
        .where ("one_level_pdt_cate = '蓝领外包'")
        .cache ()

    val odsEntGuzhuSalesmanDetail =
      sqlContext.sql("select ent_name,case when channel_name = '直客' then ent_name else channel_name end as channel_name1," +
        "case when channel_name = '直客' then channel_name else '渠道' end as channel_name from odsdb.ods_ent_guzhu_salesman_detail")
  //   .where("channel_name1 in ('北京诠道科技有限公司',\n'福建博厚人力资源服务有限公司',\n'广西前之锦人力资源有限责任公司',\n'广西协佳劳务服务有限公司',\n'广西协佳劳务派遣有限公司',\n'广州甲骨基华企业管理咨询有限公司',\n'广州市浩洋人力资源有限公司',\n'广州市正新人力资源服务有限公司',\n'广州微速保网络科技有限公司',\n'河南博普奥外包服务有限公司',\n'湖北生生人力资源股份有限公司',\n'湖北中联易通人力资源开发有限公司',\n'湖南安博人云科技有限公司',\n'湖南才智管理咨询有限公司',\n'湖南华顺人力资源服务管理股份有限公司',\n'湖南潇湘人力资源服务有限公司',\n'惠州企一通企业管理有限公司',\n'济南众邦人力资源服务有限公司',\n'江苏成功人力资源有限公司',\n'江西大唐人力资源集团有限公司',\n'江西省同济人力资源集团有限公司',\n'兰州中劳联人力资源服务有限公司',\n'沐山科技（天津）有限责任公司',\n'内蒙古中创人力资源产业发展有限公司',\n'亲亲小保（北京金色华勤数据服务有限公司）',\n'青岛华之瑞企业管理有限公司',\n'山东携众人力资源有限公司',\n'山东优创企业管理有限公司',\n'天津嘉桐木业有限公司',\n'天津永泰保险代理有限公司',\n'无锡新深度工业服务外包有限公司',\n'浙江昊基人力资源服务有限公司',\n'重庆壹零八人力资源管理有限公司',\n'北京大华启天工业技术有限公司',\n'北京协荣保险经纪有限公司',\n'北京壹心壹翼科技有限公司',\n'大童保险服务有限公司青岛西海岸营业部',\n'德州德仁人力资源有限公司',\n'福建南安市科宏劳务有限公司',\n'广东树信云科技有限公司',\n'广东友力智能科技有限公司',\n'广州番禺信合纸品有限公司',\n'广州市易新人力资源服务有限公司',\n'广州正金珠宝设计制作有限公司',\n'何晓林',\n'河北鼎昊物业服务有限公司',\n'湖北东方俊才人力资源有限公司',\n'湖北智财网络技术有限公司',\n'湖南环宇家政产业管理有限公司',\n'湖南慧睿管理咨询有限公司',\n'湖南怡美环境管理发展有限公司',\n'湖南智财网络技术有限公司',\n'湖南智财网络科技有限公司',\n'吉林省国诚大药房连锁有限公司',\n'江苏学新信息科技有限公司',\n'乐陵盛安人力资源有限公司',\n'龙口市资联人力资源有限公司',\n'青岛圣利安人力资源有限公司',\n'青岛威凯通企业管理有限公司',\n'青岛中信保诚企业管理咨询有限公司',\n'山东麦克斯企业咨询服务有限公司',\n'桐乡广源人力资源服务有限公司',\n'突泉县正匀土石方工程有限公司',\n'武汉华顺人力资源有限公司',\n'武汉江南世纪人力资源集团有限公司',\n'西安永创信息技术有限公司',\n'禹州市宏达建筑有限责任公司',\n'郑州金鼎电力有限公司禹州分公司',\n'重庆合众安法律咨询有限公司',\n'珠海市大湾人力有限公司')")
      .where("channel_name1 in ('阿斯达所多',\n'艾派克人力资源服务（武汉）有限公司',\n'爱玛客服务产业(中国)有限公司廊坊分公司',\n'安徽才联人力资源管理有限公司',\n'安徽启源企业管理服务有限公司',\n'安徽耀盛人力资源有限公司',\n'邦海（天津）供应链管理有限公司',\n'邦芒服务外包（苏州）有限公司',\n'包头高新人力资源派遣有限责任公司',\n'包头市仁明人力资源有限公司',\n'保定百优劳务派遣有限公司',\n'保定和社区外包服务有限公司',\n'保定领驭人力资源服务有限公司',\n'保定四象鼎诚电子商务有限公司',\n'保准牛测试',\n'北京艾瑞海航科技有限公司',\n'北京百思特人力资源服务有限公司',\n'北京城市展望物业管理有限公司',\n'北京德勤嘉晟国际咨询服务有限公司',\n'北京德知航创科技有限责任公司',\n'北京德众汇才企业管理有限公司',\n'北京方胜人才服务有限公司',\n'北京伏牛堂餐饮文化有限公司',\n'北京福得利企业管理服务有限公司',\n'北京福瑞祥达科技有限公司',\n'北京高校圈科技发展有限公司',\n'北京公瑾科技有限公司',\n'北京国信领航人力资源服务有限公司',\n'北京荟晟市场营销策划有限公司',\n'北京京达永欣企业管理有限公司',\n'北京京泰康瑞科技有限公司',\n'北京聚才振邦企业管理顾问有限公司',\n'北京聚丰时代仓储服务有限公司',\n'北京蓝箭空间科技有限公司',\n'北京彭博意欧人力资源服务有限公司',\n'北京润天下管理咨询有限公司',\n'北京三合和企业管理有限公司',\n'北京商惠人力资源管理咨询有限公司',\n'北京市龙翔源物流有限公司',\n'北京外企人力资源服务济南有限公司',\n'北京外企视业网技术有限公司',\n'北京香海餐饮有限责任公司',\n'北京小红帽网络科技有限责任公司',\n'北京新诚优聘咨询服务有限公司',\n'北京鑫诚寰宇网络科技有限公司',\n'北京鑫海博达劳务派遣有限公司',\n'北京鑫众联合企业管理有限公司',\n'北京信慧行商务服务有限公司',\n'北京应中科技有限公司',\n'北京永盛劳务派遣有限公司',\n'北京优全智汇信息技术有限公司',\n'北京优全智汇信息技术有限公司保准牛',\n'北京长平建筑工程有限公司',\n'北京震兴东方劳务服务有限公司',\n'北京中锐博远人力资源顾问有限公司',\n'北京众力精机科技有限公司',\n'北京众鑫百代商务服务有限公司',\n'滨州川海聚英人力资源有限公司',\n'滨州市汇联人力资源服务有限公司',\n'沧州市海瑞通信工程服务有限公司',\n'沧州天硕源人力资源服务有限公司',\n'曹县汇商联众人力资源有限公司',\n'曹县山海源人力资源发展有限公司',\n'测试啦啦啦啦',\n'常熟市华之鑫汽车配件有限公司',\n'常熟通威无纺制品有限公司',\n'常熟英格玛企业管理服务有限公司',\n'常州起跑线人力资源有限公司',\n'常州天朋人力资源有限公司',\n'常州鑫泰劳务派遣有限公司',\n'郴州保力人力资源服务有限公司',\n'成都佳娜网络科技有限公司',\n'成都青汇百川企业管理有限公司',\n'成都万华永盛企业管理有限公司',\n'成都众和盛网络科技有限公司',\n'淳安天汇企业管理服务有限公司',\n'大连弘基人力资源有限公司',\n'登封中联登电水泥有限公司',\n'定州市润达人力资源服务有限公司',\n'东莞汇鑫货运代理有限公司',\n'东莞市鸿泰劳务派遣有限公司',\n'东莞市聚强实业发展有限公司',\n'东莞市骐点人力资源管理有限公司',\n'东莞市三凰众人力资源管理咨询有限公司',\n'东莞市鑫创人力资源有限公司',\n'东莞市粤达人力资源管理咨询有限公司',\n'逗霸（上海）电子商务有限公司',\n'鄂尔多斯市众擎对外劳务承包有限责任公司',\n'鄂州葛店开发区鼎胜劳务服务有限公司',\n'鄂州慧民人力资源有限公司',\n'鄂州市恒基智能装备制造有限公司',\n'佛山市万发人力资源服务有限公司',\n'阜阳创赢人力资源服务有限公司',\n'阜阳市星驾电子科技有限公司',\n'甘肃安博人力资源服务有限公司',\n'甘肃众和人力资源服务有限公司',\n'高邮市少游企业管理咨询有限公司',\n'公司',\n'沽源县环创网络科技有限公司',\n'广东龙达影业股份有限公司',\n'广东隆信劳务服务有限公司',\n'广东宇通企业管理有限公司',\n'广西华千谷旅游开发有限公司',\n'广西汇天昌劳务有限公司',\n'广西欣龙预拌混凝土有限公司',\n'广西正大人力资源有限公司',\n'广州华路卓企业管理咨询有限公司',\n'广州聚才企业管理有限责任公司',\n'广州市标顶餐饮管理有限公司',\n'广州市国鹏货运代理有限公司',\n'广州市精诚人力资源服务有限公司',\n'广州市澜特管理咨询有限公司',\n'广州市时代人力资源服务有限公司',\n'广州值聘网络科技有限公司',\n'贵州三赢劳务有限责任公司',\n'邯郸市锐胜人力资源有限公司',\n'杭州创意人力资源服务有限公司金溪分公司',\n'杭州佳才人力资源管理有限公司',\n'杭州市恒达瓶业有限公司',\n'河北赤诚人力资源服务有限公司',\n'河北方胜人力资源服务有限公司',\n'河北和协号劳务派遣有限公司',\n'河北泓舜企业管理服务有限公司',\n'河北建博人力资源有限公司',\n'河北凯盛医药科技有限公司',\n'河北诺亚人力资源开发有限公司',\n'河北润盈人力资源服务有限公司',\n'河北卫人人力资源开发有限公司',\n'河北旭尧人力资源服务有限责任公司',\n'河北亚际会人力资源有限公司',\n'河北云千人力资源有限公司',\n'河南百硕商务服务有限公司开封分公司',\n'河南工多多人力资源有限公司',\n'河南鸿联九五人力资源有限公司',\n'河南鸿盛人力资源咨询服务有限公司',\n'河南华旗人力资源服务有限公司',\n'河南荟萃人力资源服务有限公司',\n'河南吉至企业管理咨询有限公司',\n'河南尖峰时刻人力资源服务有限公司',\n'河南金马人力资源服务有限公司',\n'河南蓝图人力资源服务有限公司',\n'河南隆昌人力资源服务有限公司',\n'河南鹏劳人力资源管理有限公司',\n'河南千朵人力资源服务有限公司',\n'河南省汇智人力资源服务有限公司',\n'河南贤泽人力资源服务有限公司',\n'河南阳明人力资源服务有限公司',\n'河南优企人力资源管理有限公司',\n'河南正邦人力资源有限公司',\n'河南智策外包服务有限公司',\n'河南智信劳务派遣服务有限公司',\n'河南众程人力资源有限公司',\n'菏泽联业人力资源服务有限公司',\n'黑龙江川海人力资源服务集团有限公司',\n'黑龙江大益智信人力资源服务有限公司',\n'恒超源洗净科技（深圳）有限公司',\n'衡阳市泰合人力资源有限公司',\n'湖北大楚人才服务有限公司',\n'湖北华夏创业管理顾问有限公司',\n'湖北荐杰人力资源有限公司',\n'湖北领才人力资源有限公司',\n'湖北纳杰人力资源有限公司',\n'湖北融智兴业人力资源服务有限公司',\n'湖北盛世通九人力资源管理有限公司',\n'湖北易海人力资源有限公司',\n'湖南创亿人力资源有限公司',\n'湖南庆旺保洁服务有限公司',\n'湖南天池家政产业管理有限公司',\n'湖南天纳宏邦企业服务有限公司',\n'湖南天胜人力资源有限公司',\n'湖南源哲方略企业管理咨询有限公司',\n'湖南粤盛人力资源服务有限公司',\n'湖州百客网络科技有限公司',\n'淮安方元人力资源有限公司',\n'淮安市精英人力资源有限公司',\n'济南龙腾人力资源管理有限公司',\n'济南英雄山建筑安装工程有限公司',\n'济宁宏通人力资源有限公司',\n'济宁汇鑫人力资源服务有限公司',\n'济源市万瑞人力资源服务有限公司',\n'嘉兴东呈人力资源有限公司',\n'嘉兴广源人力资源有限公司宿迁分公司',\n'嘉兴恒翔人力资源有限公司',\n'嘉兴乐邦人力资源服务有限公司',\n'嘉兴市泓磊人力资源有限公司',\n'嘉兴顺和人力资源有限公司',\n'嘉兴威利人力资源有限公司',\n'江苏佰仕凯服务外包有限公司',\n'江苏鼎峰机电设备有限公司',\n'江苏兰尔博人力资源有限公司',\n'江苏南联物流有限公司',\n'江苏雅风家具制造有限公司',\n'江西出蓝投资管理有限公司',\n'江西赣鄱人力资源市场发展有限公司',\n'江西金手指劳务派遣有限公司',\n'江西省聚力人力资源开发有限公司',\n'江西省聚人人力资源开发有限公司',\n'江西省君源人力资源服务有限公司',\n'江西我当家科技服务有限公司',\n'金华才享人力资源有限公司',\n'荆州聚鑫人力资源有限公司',\n'径圆（上海）信息技术有限公司',\n'开封惠民人力资源有限公司',\n'开平市马冈镇梁金想木材加工场',\n'空间无限人力资源管理顾问有限公司',\n'昆明厚致百盈企业管理有限公司',\n'昆山沿沪劳务派遣有限公司',\n'兰考腾达人力资源有限公司',\n'蓝箭航天技术有限公司',\n'廊坊市新华劳务派遣有限公司',\n'廊坊正略企业管理咨询有限公司',\n'辽宁北松人力资源有限公司',\n'辽宁沃锐达人力资源有限公司',\n'聊城市湖景名仕苑网络科技有限公司',\n'聊城市首创企业管理咨询有限公司',\n'聊城市淘九天网络科技有限公司',\n'临沂德聚仁合人力资源服务有限公司',\n'临沂淼信劳务服务有限公司',\n'龙岩市好工作人才服务有限公司',\n'绿能中环（武汉）科技有限公司',\n'麦斯特人力资源有限公司',\n'米阳物联网技术（嘉兴）有限公司',\n'南安市三一企业管理咨询有限公司',\n'南昌汇聚天成人力资源有限公司',\n'南昌米优文化传媒有限公司',\n'南昌鸣晨人力资源服务有限公司',\n'南京斐洛企业管理咨询有限公司',\n'南京九诚劳务服务有限公司',\n'南京霖联企业管理有限公司',\n'南京睿雯人力资源有限公司',\n'南京易联碧诚人力资源顾问有限公司',\n'南京云亿朵企业管理有限公司',\n'南宁市前锦人力资源有限责任公司',\n'内蒙古万众炜业科技环保股份公司府谷分公司',\n'内蒙古卓悦人力资源服务有限责任公司',\n'宁波博普奥企业服务有限公司',\n'宁波三泰劳务派遣服务有限公司萧山分公司',\n'宁波市众一人力资源有限公司',\n'宁波新诚优聘服务外包有限公司',\n'宁波英博人力资源服务有限公司',\n'宁夏劳联人力资源服务连锁有限公司',\n'宁夏瑞力坤人力资源有限公司',\n'欧瑞德(廊坊)人力资源服务有限公司',\n'蓬莱市万众劳务派遣有限公司',\n'濮阳市中劳人力资源服务有限公司',\n'企服（北京）科技有限公司',\n'青岛百优特企业管理有限公司',\n'青岛笃信贤人力资源有限公司',\n'青岛广元垣锦劳务服务有限公司',\n'青岛汉为企业管理服务有限公司',\n'青岛恒达人力资源管理有限公司',\n'青岛恒晟人力资源服务有限公司',\n'青岛华夏通人力资源有限公司',\n'青岛华兴鲁强劳务服务有限公司',\n'青岛汇安人力资源有限公司',\n'青岛玖创盛业管理咨询有限公司',\n'青岛聚宏达劳务服务有限公司',\n'青岛君诺人力资源有限公司',\n'青岛零伍叁贰人力资源有限公司',\n'青岛诺亚环球人力资源有限公司',\n'青岛企无忧管理服务有限公司',\n'青岛泉沣达人力资源有限公司',\n'青岛瑞成九洲企业管理有限公司成武分公司',\n'青岛三洋汇人力资源集团有限公司',\n'青岛天众营销管理有限公司',\n'青岛玮玮人力资源服务有限公司',\n'青岛鑫诚劳联劳务有限公司',\n'青岛一诚一实业有限公司',\n'青岛英智博人力资源服务有限公司',\n'青岛钰琳前程人力资源有限公司东营分公司',\n'青岛招才通企业管理有限公司',\n'青岛智名劳务服务有限公司',\n'青岛中骏锦诚投资管理有限公司',\n'青岛中企联人力资源开发有限公司',\n'青岛粥全粥到伟业酒店管理有限公司',\n'泉州汇思人力资源管理有限公司',\n'日照蓝塔企业管理咨询有限公司',\n'日照鑫合人力资源有限公司',\n'容城县佳运制衣厂',\n'三河市众合智佳企业管理有限公司',\n'厦门蓝服服务外包有限公司',\n'山东滨惠安装工程有限公司',\n'山东达能人力资源有限公司',\n'山东大德永盛企业管理有限公司',\n'山东岛城速招人力资源管理咨询有限公司',\n'山东昊达劳务派遣有限公司',\n'山东红海人力资源有限公司',\n'山东机场吉翔航空食品有限公司',\n'山东坤润乐橙人力资源有限公司',\n'山东联劳教育科技有限公司',\n'山东省工联在线人力资源有限公司',\n'山东盛嘉园汽车综合服务有限公司',\n'山东文鼎人力资源服务有限公司',\n'山东远洋人力资源有限公司',\n'山东众信盈通企业管理有限公司',\n'上海渤源劳务派遣有限公司',\n'上海琮玺企业服务外包有限公司',\n'上海方智劳务派遣有限公司',\n'上海佛思电气有限公司',\n'上海韩帛服饰有限公司',\n'上海几加文化传播发展有限公司',\n'上海锦道人力资源有限公司',\n'上海良钧劳务派遣有限公司',\n'上海蜜情焙浓餐饮管理有限公司',\n'上海勤森包装材料有限公司',\n'上海瑞方企业管理有限公司',\n'上海瑟悟网络科技有限公司',\n'上海仕优劳务派遣有限公司',\n'上海天盒餐饮管理有限公司',\n'上海西第科投资管理有限公司',\n'上海裕夫建筑工程服务中心',\n'上海智钦机电系统工程有限公司',\n'上海卓民建设工程有限公司',\n'上饶市卓越人力资源有限公司',\n'深圳高众实业有限公司',\n'深圳全赢劳务派遣有限公司',\n'深圳市百裕劳务分包有限公司',\n'深圳市博宇文化发展有限公司',\n'深圳市后河财富金融服务有限公司',\n'深圳市景泰诚人力资源有限公司',\n'深圳市丽星清洁服务有限公司',\n'深圳市马马马劳务派遣有限公司',\n'深圳市敏捷人力资源管理有限公司',\n'深圳市仕通智达企业管理咨询有限公司',\n'深圳市思杰劳务派遣有限公司',\n'深圳市淘活科技有限公司',\n'深圳市天和劳务派遣有限公司',\n'深圳市伍洲劳务派遣有限公司',\n'深圳市众勤商务咨询有限公司',\n'深圳万祥人力资源有限公司',\n'深圳鑫众源人力资源有限公司',\n'神速店小二餐饮管理服务有限公司',\n'沈阳戴弗德人力资源服务有限公司',\n'沈阳优才网络科技有限公司',\n'生活半径（北京）信息技术有限公司',\n'石家庄森海人力资源服务有限公司',\n'石家庄森海人力资源服务有限公司高邑分公司',\n'石家庄市龙威运输有限公司',\n'石家庄悦享网络科技有限公司',\n'四川才库人力资源服务有限公司',\n'四川英思达人力资源管理有限公司重庆分公司',\n'苏州国腾企业管理有限公司',\n'苏州宏尔盛企业管理服务有限公司',\n'苏州金梧桐人力资源有限公司',\n'苏州聚德晟企业管理有限公司',\n'苏州聚恩企业管理服务有限公司',\n'苏州欧唛圣机电科技有限公司',\n'苏州市隆兴消防设备有限公司',\n'苏州唯怡卓网络科技有限公司',\n'苏州涌智企业服务有限公司',\n'遂平县三禾人力资源有限公司',\n'太康县伯乐企业管理服务有限公司',\n'泰安众鑫人力资源服务有限公司',\n'唐山市丰南区优瑞人力资源服务有限公司',\n'天保中天科技（天津）有限公司',\n'天津思诚企业管理咨询有限公司',\n'天津思含意境企业营销策划有限公司',\n'天津新诺人力资源服务有限公司',\n'天津薪食尚餐饮管理有限公司',\n'天津众辉劳务服务有限公司',\n'天津众晟鑫劳务服务有限公司',\n'通州区平潮镇有为金属制品厂',\n'同行保险经纪有限公司淄博分公司',\n'推推人力资源（上海）有限公司',\n'威海兆丰电焊有限公司',\n'威海兆丰建筑工程有限公司',\n'威海中复西港船艇有限公司',\n'潍坊市正宏人力资源服务有限公司',\n'温州微猫网络科技有限公司',\n'无锡爱派人力资源有限公司',\n'无锡九方皋企业管理咨询有限公司',\n'无锡优派人力资源服务有限公司',\n'芜湖智领人力资源有限公司',\n'武安市鼎锋餐饮服务有限公司',\n'武汉安捷瑞达物流服务有限公司',\n'武汉百世英才人力资源有限公司',\n'武汉德威热力股份有限公司',\n'武汉福汉德汽车零部件有限公司',\n'武汉和诚义建筑劳务有限公司',\n'武汉衡业人力资源有限公司',\n'武汉吉昌人力资源有限公司',\n'武汉佳信英才人力资源服务有限公司',\n'武汉金航标劳务服务有限责任公司',\n'武汉金来人力资源有限公司',\n'武汉九型人力资源有限公司',\n'武汉前程似锦人力资源有限公司',\n'武汉三惠人力资源服务有限公司',\n'武汉仕达人力资源服务有限公司',\n'武汉通衢人力资源有限公司',\n'武汉万象启航人力资源服务有限公司',\n'武汉威盾劳务派遣有限责任公司',\n'武汉新途宇亚人力资源有限公司',\n'武汉优德人力资源服务有限公司',\n'武汉众生缘人力资源有限公司',\n'西安志同企业管理咨询有限公司',\n'西宁中夏人力资源有限公司',\n'夏津众合至诚劳动服务有限公司',\n'襄阳点派人力资源有限公司',\n'项城市腾龙实业有限公司',\n'新疆百疆汇网络信息科技有限公司',\n'新乡市达旺人力资源服务有限公司',\n'新乡市开元劳务派遣有限公司',\n'新乡市三鑫人力资源服务有限公司',\n'邢台耀航人力资源服务有限公司',\n'兴安盟易广思含企业管理咨询有限公司',\n'宿迁联业人力资源有限公司',\n'徐州安企保险代理有限公司',\n'徐州市卓远人力资源有限公司',\n'烟台辰星人力资源服务有限公司',\n'烟台晟燃人力资源有限公司',\n'烟台万众人力资源有限公司',\n'扬州点金人力资源有限公司',\n'亿卓人力资源管理咨询（重庆）有限公司',\n'义乌市英蓝人力资源有限公司',\n'云南高创人才服务有限公司',\n'云南康言人力资源有限公司',\n'云南仁贤人力资源管理有限公司',\n'张家口平安人力资源有限公司',\n'漳州吉胜劳务派遣有限公司',\n'漳州锦杰劳务派遣有限公司',\n'漳州市利众劳务派遣有限公司',\n'长安和耐家具经销处',\n'长安润启白佛车辆管理服务中心',\n'长春市信德美餐饮管理有限公司榆树分公司',\n'长沙冀森人力资源有限公司',\n'浙江名天动力单车科技有限公司',\n'浙江万马高分子材料有限公司',\n'浙江榆阳电子有限公司',\n'郑州龙恩人力资源服务有限公司',\n'郑州市管城区米萨格烘焙工作室',\n'智汇方圆人才服务（江苏）有限公司',\n'中山汇盈人力资源管理有限公司',\n'中智安徽经济技术合作有限公司',\n'重庆贝贤人力资源管理咨询有限公司',\n'重庆博来杰人力资源管理有限公司',\n'重庆东高商务咨询服务有限公司',\n'重庆光音企业管理有限公司',\n'重庆恒至本企业管理咨询服务有限公司',\n'重庆汇通世纪人力资源管理有限公司',\n'重庆乐众人力资源管理有限公司',\n'重庆迈思腾人力资源管理咨询有限公司',\n'重庆蜜糖网络科技有限公司',\n'重庆欧达慧哲企业管理咨询有限公司',\n'重庆千能机械制造有限公司',\n'重庆千能实业有限公司',\n'重庆千锐宏企业管理有限公司',\n'重庆世季如咖餐饮管理有限公司',\n'重庆市蜕迹装饰设计有限公司',\n'重庆市万州区劳务经济开发公司',\n'重庆速送科技有限公司',\n'重庆天竹人力资源管理有限公司',\n'重庆威凌盾企业管理咨询有限公司',\n'重庆维特利人力资源管理有限公司',\n'重庆玺扬阳企业管理有限公司',\n'重庆翔耀保险咨询服务有限公司',\n'重庆新起点人力资源服有限公司',\n'重庆渝足企业管理咨询集团有限公司',\n'重庆振华保安服务有限公司',\n'重庆众谊劳务派遣服务有限公司',\n'周口市匠人劳务派遣有限公司',\n'珠海市大湾人力资源有限公司',\n'珠海市恒瑞星劳务派遣有限公司',\n'珠海市汇英人力资源服务有限公司',\n'珠海展望物流有限公司',\n'诸城市江山工贸有限公司',\n'诸城市鹏鑫精密机械厂',\n'淄博铭泰劳务派遣有限公司',\n'淄博众诚人力资源有限公司')")

    /**
      * 保单和产品进行关联 得到产品为蓝领外包（雇主）的所有保单,并算出每日保单信息
      */
    val policyAndProductTemp1 = odsPolicyDetail.join (odsProductDetail, odsPolicyDetail ("product_code") === odsProductDetail ("product_code_slave")).cache ()
      .selectExpr("policy_id","policy_start_date","policy_end_date","holder_name")

    val policyAndProductTemp = policyAndProductTemp1.join(odsEntGuzhuSalesmanDetail,policyAndProductTemp1("holder_name")===odsEntGuzhuSalesmanDetail("ent_name"))
      .selectExpr("policy_id","policy_start_date","policy_end_date","holder_name","channel_name1")


    val policyAndProductOne = policyAndProductTemp
      .selectExpr("policy_id","policy_start_date","policy_end_date","channel_name1")
      .mapPartitions(rdd => {
        rdd.flatMap(x => {
          val channelName1 = x.getAs[String]("channel_name1")
          val policyStartDate = x.getAs[Timestamp]("policy_start_date").toString
          val policyEndDate = x.getAs[Timestamp]("policy_end_date").toString
          val res = getBeg_End_one_two_month(policyStartDate, policyEndDate)
            .map(day_id => {
            (channelName1,day_id)
          })
          res
        })
      })
      .distinct()//每个客户每个月有唯一值
      .toDF("channel_name1","day_id")

    val policyAndProductTwo = policyAndProductOne.selectExpr("channel_name1 as channel_name1_slave","day_id as day_id_salve")
    val policyAndProductTwoRes = policyAndProductOne.join(policyAndProductTwo,'channel_name1 === 'channel_name1_slave,"leftouter")
      .map(x => {
        val channelName1 = x.getAs[String]("channel_name1")
        val dayId = x.getAs[String]("day_id")
        val dayIdSalve = x.getAs[String]("day_id_salve")
        val count = if(dayIdSalve == null ){
          0
        }else if(dateAddOneMonth(dayId) != dayIdSalve){
          0
        }else {
          1
        }
        ((channelName1,dayId),count)
      }).reduceByKey(_+_)//判断这个客户是否在下个月出现过，如果累加值 = 1 说明在下个月在保
        .map(x => {
          (x._1._2,(x._2,1))
        }).reduceByKey((x1,x2) =>{
          val one = x1._1+x2._1
          val two = x1._2 + x2._2
      (one,two)
    }).map(x => (x._1,x._2._1,x._2._2))
      .toDF("day_id","count_continue","count")


    //policyAndProductOne.rdd.repartition(1).saveAsTextFile("C:\\Users\\xingyuan\\Desktop\\未完成 2\\11.数据仓库项目搭建\\提数\\雇主续投数据1")
    policyAndProductTwoRes.rdd.repartition(1).saveAsTextFile("C:\\Users\\xingyuan\\Desktop\\未完成 2\\11.数据仓库项目搭建\\提数\\雇主续投数据1")
    policyAndProductOne.show()
  }
}
