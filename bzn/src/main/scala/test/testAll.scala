package test

import Util.Spark_Util
import org.apache.spark.sql.hive.HiveContext

object testAll {
  def main(args: Array[String]): Unit = {
    var str = "{\"followUserIds\":[],\"createUserId\":1161,\"city\":1875,\"customFieldJson\":\"[{\\\"id\\\":4793,\\\"contentType\\\":\\\"chained_droplist\\\",\\\"content\\\":[1],\\\"fieldCode\\\":\\\"CustomField_4793\\\"},{\\\"id\\\":4803,\\\"contentType\\\":\\\"text\\\",\\\"content\\\":\\\"付斌\\\",\\\"fieldCode\\\":\\\"CustomField_4803\\\"},{\\\"id\\\":4805,\\\"contentType\\\":\\\"text\\\",\\\"content\\\":\\\"13733541475\\\",\\\"fieldCode\\\":\\\"CustomField_4805\\\"},{\\\"id\\\":4795,\\\"contentType\\\":\\\"droplist\\\",\\\"content\\\":6,\\\"fieldCode\\\":\\\"CustomField_4795\\\"},{\\\"id\\\":4825,\\\"contentType\\\":\\\"droplist\\\",\\\"content\\\":1,\\\"fieldCode\\\":\\\"CustomField_4825\\\"},{\\\"id\\\":4807,\\\"contentType\\\":\\\"droplist\\\",\\\"content\\\":1,\\\"fieldCode\\\":\\\"CustomField_4807\\\"}]\",\"customerHighSeaId\":110,\"source\":1,\"coordinatorUserIds\":[],\"deleteFlag\":0,\"province\":1851,\"officeId\":418,\"id\":1239945,\"locked\":0,\"email\":\"\",\"customerHighSea\":{\"deleteFlag\":0,\"memberUserIdArray\":[],\"name\":\"公司获客客户公海\",\"memberOfficeIdArray\":[],\"id\":110,\"masterUserIdArray\":[]},\"changeTimestamp\":\"2019-01-28 18:01:29.689\",\"fieldMap\":{\"CustomField_4807\":{\"fieldCode\":\"CustomField_4807\",\"id\":4807,\"contentType\":\"droplist\",\"content\":1},\"CustomField_4793\":{\"fieldCode\":\"CustomField_4793\",\"id\":4793,\"contentType\":\"chained_droplist\",\"content\":[1]},\"CustomField_4795\":{\"fieldCode\":\"CustomField_4795\",\"id\":4795,\"contentType\":\"droplist\",\"content\":6},\"CustomField_4825\":{\"fieldCode\":\"CustomField_4825\",\"id\":4825,\"contentType\":\"droplist\",\"content\":1},\"CustomField_4803\":{\"fieldCode\":\"CustomField_4803\",\"id\":4803,\"contentType\":\"text\",\"content\":\"付斌\"},\"CustomField_4805\":{\"fieldCode\":\"CustomField_4805\",\"id\":4805,\"contentType\":\"text\",\"content\":\"13733541475\"}},\"focusArray\":[],\"customerHighSeaReturnCount\":0,\"mobile\":\"\",\"userGroupPermissionIds\":[],\"businessPermissionList\":[],\"officePermissionIds\":[],\"updateTime\":\"2019-01-28 18:01:29\",\"telephone\":\"\",\"businessCoordinatorList\":[],\"classification\":3,\"master\":{\"deleteFlag\":0,\"admin\":false,\"id\":1161,\"roleList\":[],\"realname\":\"邓平\"},\"showDetail\":1,\"masterUserId\":1161,\"lastFollowTime\":\"2019-01-28 18:01:30\",\"tagList\":[],\"companyId\":379,\"createTime\":\"2019-01-28 18:01:29\",\"provinceEntity\":{\"name\":\"湖北省\",\"id\":1851},\"district\":0,\"notFollowDays\":0,\"name\":\"房县人和人力资源开发有限公司\",\"businessCategory\":{\"deleteFlag\":0,\"name\":\"默认业务类型\"},\"businessCategoryId\":229,\"highSeaStatus\":4,\"leadsDuplicateFlag\":1,\"createUser\":{\"deleteFlag\":0,\"admin\":false,\"id\":1161,\"roleList\":[],\"realname\":\"邓平\"},\"masterOffice\":{\"name\":\"面销部南区\",\"id\":418,\"masterUserIdArray\":[]}}"

    if(str.contains("updateUser")){
      println("you")
    }else{
      println("wu")
    }

    for(x <- 0 to -1-1){
      println("123")
    }

    //程序用户名
    System.setProperty("HADOOP_USER_NAME", "root")

    var test = "CustomField_4790  {\"fieldCode\":\"CustomField_4790\",\"id\":4790,\"contentType\":\"date\"}"

    if(test.contains("content")){
      println("123")
    }

    var res = "name=北京图为先科技有限公司\u0001CustomField_4795=16"
    println(res.split("\\u0001").length)
    println("\\u0001"+"\u0001")
    var len = 0
    for (x <- 0 to len-2){

    }

  }
}
