package bzn.ods.policy

import bzn.job.common.DataBaseUtil
import bzn.util.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2020/2/13
  * Time:10:29
  * describe: 源头元数据监控-阻断性
  **/
object OdsMonitorSourceAndSourceDataDetailTest extends SparkUtil with DataBaseUtil with OdsMonitorTool{
  def main (args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val hqlContext = sparkConf._4
    getCentralBaseMonitor(hqlContext)
    sc.stop()
  }

  /**
    * 2.0核心库监控
    * @param sqlContext 上下文
    */
  def getCentralBaseMonitor(sqlContext:HiveContext) = {
    sqlContext.udf.register("getUUID",()=>(java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("returnColNew", (source:String,local:String) => returnColNew(source:String,local:String))
    sqlContext.udf.register("returnColUpdata", (source:String,local:String) => returnColUpdata(source:String,local:String))
    sqlContext.udf.register("returnColDefault", (sourceCol:String,localCol:String,source:String,local:String) => returnColDefault(sourceCol:String,localCol:String,source:String,local:String))
    sqlContext.udf.register("returnColIsNull", (sourceCol:String,localCol:String,source:String,local:String) => returnColIsNull(sourceCol:String,localCol:String,source:String,local:String))
    sqlContext.udf.register("returnColDataType", (sourceCol:String,localCol:String,source:String,local:String) => returnColDataType(sourceCol:String,localCol:String,source:String,local:String))
    sqlContext.udf.register("returnColKey", (sourceCol:String,localCol:String,source:String,local:String) => returnColKey(sourceCol:String,localCol:String,source:String,local:String))
    sqlContext.udf.register("returnColType", (sourceCol:String,localCol:String,source:String,local:String) => returnColType(sourceCol:String,localCol:String,source:String,local:String))
    sqlContext.udf.register("returnColStringLength", (sourceCol:String,localCol:String,source:Long,local:Long) => returnColStringLength(sourceCol:String,localCol:String,source:Long,local:Long))
    sqlContext.udf.register("returnColNumTypeLength", (sourceCol:String,localCol:String,source:Long,local:Long) => returnColNumTypeLength(sourceCol:String,localCol:String,source:Long,local:Long))
    sqlContext.udf.register("returnColNumTypeScalaLength", (sourceCol:String,localCol:String,source:Long,local:Long) => returnColNumTypeScalaLength(sourceCol:String,localCol:String,source:Long,local:Long))
    sqlContext.udf.register("returnColTimeTypeLength", (sourceCol:String,localCol:String,source:Long,local:Long) => returnColTimeTypeLength(sourceCol:String,localCol:String,source:Long,local:Long))
    import sqlContext.implicits._

    val inforSchema106 = "COLUMNS"
    val inforSchema2 = "columns_inforSchema_2" //2。0系统元数据表
    val inforSchema1 = "columns_inforSchema_1" //1。0系统元数据表
    val officialUrl = "mysql.url.106"
    val officialUrlInforSchema = "mysql.url.106.information.schema"
    val officialUser = "mysql.username.106"
    val officialPass = "mysql.password.106"
    val driver = "mysql.driver"

    val user103 = "mysql.username.103"
    val pass103 = "mysql.password.103"
    val url103 = "mysql_url.103.odsdb"

    /**
      * 读取本地元数据信息
      */
    val inforSchema106Data: DataFrame = readMysqlTable(sqlContext: SQLContext, inforSchema106: String,officialUser:String,officialPass:String,driver:String,officialUrlInforSchema:String)
      .selectExpr(
        "TABLE_SCHEMA",
        "TABLE_NAME",
        "COLUMN_NAME",
        "COLUMN_DEFAULT",
        "IS_NULLABLE",
        "DATA_TYPE",
        "CHARACTER_MAXIMUM_LENGTH",
        "NUMERIC_PRECISION",
        "NUMERIC_SCALE",
        "DATETIME_PRECISION",
        "COLLATION_NAME",
        "COLUMN_TYPE",
        "COLUMN_KEY"
      )

    /**
      * 读取1.0核心库元数据信息
      */
    val inforSchema1Data: DataFrame = readMysqlTable(sqlContext: SQLContext, inforSchema1: String,officialUser:String,officialPass:String,driver:String,officialUrl:String)
      .selectExpr(
        "TABLE_SCHEMA                 as TABLE_SCHEMA_SOURCE",
        "TABLE_NAME                   as TABLE_NAME_SOURCE",
        "CONCAT(TABLE_NAME,'_bznapi') as TABLE_NAME_API", //接口库
        "CONCAT(TABLE_NAME,'_bznprd') as TABLE_NAME_PRD",//生产库
        "COLUMN_NAME                  as COLUMN_NAME_SOURCE",
        "COLUMN_DEFAULT               as COLUMN_DEFAULT_1",
        "IS_NULLABLE                  as IS_NULLABLE_1",
        "DATA_TYPE                    as DATA_TYPE_1",
        "CHARACTER_MAXIMUM_LENGTH     as CHARACTER_MAXIMUM_LENGTH_1",
        "NUMERIC_PRECISION            as NUMERIC_PRECISION_1",
        "NUMERIC_SCALE                as NUMERIC_SCALE_1",
        "DATETIME_PRECISION           as DATETIME_PRECISION_1",
        "COLLATION_NAME               as COLLATION_NAME_1",
        "COLUMN_TYPE                  as COLUMN_TYPE_1",
        "COLUMN_KEY                   as COLUMN_KEY_1"
      )

    /**
      * 读取2.0核心库元数据信息
      */
    val inforSchema2Data: DataFrame = readMysqlTable(sqlContext: SQLContext, inforSchema2: String,officialUser:String,officialPass:String,driver:String,officialUrl:String)
      .selectExpr(
        "TABLE_SCHEMA                 as TABLE_SCHEMA_SOURCE",
        "TABLE_NAME                   as TABLE_NAME_SOURCE",
        "CONCAT(TABLE_NAME,'_bzncen') as TABLE_NAME_CEN", //核心库
        "CONCAT(TABLE_NAME,'_bznbusi') as TABLE_NAME_BUSI",//业管库
        "CONCAT(TABLE_NAME,'_bznmana') as TABLE_NAME_MANA",
        "COLUMN_NAME                  as COLUMN_NAME_SOURCE",
        "COLUMN_DEFAULT               as COLUMN_DEFAULT_2",
        "IS_NULLABLE                  as IS_NULLABLE_2",
        "DATA_TYPE                    as DATA_TYPE_2",
        "CHARACTER_MAXIMUM_LENGTH     as CHARACTER_MAXIMUM_LENGTH_2",
        "NUMERIC_PRECISION            as NUMERIC_PRECISION_2",
        "NUMERIC_SCALE                as NUMERIC_SCALE_2",
        "DATETIME_PRECISION           as DATETIME_PRECISION_2",
        "COLLATION_NAME               as COLLATION_NAME_2",
        "COLUMN_TYPE                  as COLUMN_TYPE_2",
        "COLUMN_KEY                   as COLUMN_KEY_2"
      )

    val resCen = inforSchema106Data.join(inforSchema2Data,'TABLE_NAME==='TABLE_NAME_CEN and 'COLUMN_NAME==='COLUMN_NAME_SOURCE,"leftouter")
      .selectExpr(
        "TABLE_SCHEMA",
        "TABLE_SCHEMA_SOURCE",
        "TABLE_NAME",
        "TABLE_NAME_SOURCE",
        "COLUMN_NAME",
        "COLUMN_NAME_SOURCE",
        "returnColNew(COLUMN_NAME_SOURCE,COLUMN_NAME) as col_is_new_rule",
        "returnColUpdata(COLUMN_NAME_SOURCE,COLUMN_NAME) as col_is_update_rule",
        "returnColDefault(COLUMN_NAME_SOURCE,COLUMN_NAME,COLUMN_DEFAULT_2,COLUMN_DEFAULT) as col_default_value_rule",
        "returnColIsNull(COLUMN_NAME_SOURCE,COLUMN_NAME,IS_NULLABLE_2,IS_NULLABLE) as col_is_null_rule",
        "returnColDataType(COLUMN_NAME_SOURCE,COLUMN_NAME,DATA_TYPE_2,DATA_TYPE) as col_data_type_rule",
        "case when CHARACTER_MAXIMUM_LENGTH_2 is not null and CHARACTER_MAXIMUM_LENGTH is not null then " +
          "returnColStringLength(COLUMN_NAME_SOURCE,COLUMN_NAME,CHARACTER_MAXIMUM_LENGTH_2,CHARACTER_MAXIMUM_LENGTH) else -1 end as col_string_length_rule",
        "case when NUMERIC_PRECISION_2 is not null and NUMERIC_PRECISION is not null then " +
          "returnColNumTypeLength(COLUMN_NAME_SOURCE,COLUMN_NAME,NUMERIC_PRECISION_2,NUMERIC_PRECISION) else -1 end as col_number_length_rule",
        "case when NUMERIC_SCALE_2 is not null and NUMERIC_SCALE is not null then " +
          "returnColNumTypeLength(COLUMN_NAME_SOURCE,COLUMN_NAME,NUMERIC_SCALE_2,NUMERIC_SCALE) else -1 end as col_number_scala_rule",
        "case when DATETIME_PRECISION_2 is not null and DATETIME_PRECISION is not null then " +
          "returnColNumTypeLength(COLUMN_NAME_SOURCE,COLUMN_NAME,DATETIME_PRECISION_2,DATETIME_PRECISION) else -1 end as col_time_length_rule",
        "returnColType(COLUMN_NAME_SOURCE,COLUMN_NAME,COLUMN_TYPE_2,COLUMN_TYPE) as col_type_rule",
        "returnColKey(COLUMN_NAME_SOURCE,COLUMN_NAME,COLUMN_KEY_2,COLUMN_KEY) as col_key_rule"
    )
      .where("TABLE_SCHEMA_SOURCE = 'bzn_central' and TABLE_NAME in ('b_policy_product_plan_bzncen','b_policy_subject_bzncen','b_policy_bzncen','b_policy_subject_company_bzncen','bs_product_bzncen','b_policy_installment_plan_bzncen','b_policy_holder_company_bzncen','b_policy_preservation_bzncen','b_policy_holder_person_bzncen','b_policy_preservation_subject_person_slave_bzncen','b_policy_subject_person_slave_bzncen')")

    val resBusi = inforSchema106Data.join(inforSchema2Data,'TABLE_NAME==='TABLE_NAME_BUSI and 'COLUMN_NAME==='COLUMN_NAME_SOURCE,"leftouter")
      .selectExpr(
        "TABLE_SCHEMA",
        "TABLE_SCHEMA_SOURCE",
        "TABLE_NAME",
        "TABLE_NAME_SOURCE",
        "COLUMN_NAME",
        "COLUMN_NAME_SOURCE",
        "returnColNew(COLUMN_NAME_SOURCE,COLUMN_NAME) as col_is_new_rule",
        "returnColUpdata(COLUMN_NAME_SOURCE,COLUMN_NAME) as col_is_update_rule",
        "returnColDefault(COLUMN_NAME_SOURCE,COLUMN_NAME,COLUMN_DEFAULT_2,COLUMN_DEFAULT) as col_default_value_rule",
        "returnColIsNull(COLUMN_NAME_SOURCE,COLUMN_NAME,IS_NULLABLE_2,IS_NULLABLE) as col_is_null_rule",
        "returnColDataType(COLUMN_NAME_SOURCE,COLUMN_NAME,DATA_TYPE_2,DATA_TYPE) as col_data_type_rule",
        "case when CHARACTER_MAXIMUM_LENGTH_2 is not null and CHARACTER_MAXIMUM_LENGTH is not null then " +
          "returnColStringLength(COLUMN_NAME_SOURCE,COLUMN_NAME,CHARACTER_MAXIMUM_LENGTH_2,CHARACTER_MAXIMUM_LENGTH) else -1 end as col_string_length_rule",
        "case when NUMERIC_PRECISION_2 is not null and NUMERIC_PRECISION is not null then " +
          "returnColNumTypeLength(COLUMN_NAME_SOURCE,COLUMN_NAME,NUMERIC_PRECISION_2,NUMERIC_PRECISION) else -1 end as col_number_length_rule",
        "case when NUMERIC_SCALE_2 is not null and NUMERIC_SCALE is not null then " +
          "returnColNumTypeLength(COLUMN_NAME_SOURCE,COLUMN_NAME,NUMERIC_SCALE_2,NUMERIC_SCALE) else -1 end as col_number_scala_rule",
        "case when DATETIME_PRECISION_2 is not null and DATETIME_PRECISION is not null then " +
          "returnColNumTypeLength(COLUMN_NAME_SOURCE,COLUMN_NAME,DATETIME_PRECISION_2,DATETIME_PRECISION) else -1 end as col_time_length_rule",
        "returnColType(COLUMN_NAME_SOURCE,COLUMN_NAME,COLUMN_TYPE_2,COLUMN_TYPE) as col_type_rule",
        "returnColKey(COLUMN_NAME_SOURCE,COLUMN_NAME,COLUMN_KEY_2,COLUMN_KEY) as col_key_rule"
      )
      .where("TABLE_SCHEMA_SOURCE = 'bzn_business' and TABLE_NAME in ('dict_zh_plan_bznbusi','dict_china_life_plan_group_bznbusi','t_proposal_bznbusi','t_proposal_product_plan_bznbusi','dict_chinalife_wyb_plan_bznbusi','dict_china_life_plan_bznbusi','t_proposal_holder_company_bznbusi')")

    val resManage = inforSchema106Data.join(inforSchema2Data,'TABLE_NAME==='TABLE_NAME_MANA and 'COLUMN_NAME==='COLUMN_NAME_SOURCE,"leftouter")
      .selectExpr(
        "TABLE_SCHEMA",
        "TABLE_SCHEMA_SOURCE",
        "TABLE_NAME",
        "TABLE_NAME_SOURCE",
        "COLUMN_NAME",
        "COLUMN_NAME_SOURCE",
        "returnColNew(COLUMN_NAME_SOURCE,COLUMN_NAME) as col_is_new_rule",
        "returnColUpdata(COLUMN_NAME_SOURCE,COLUMN_NAME) as col_is_update_rule",
        "returnColDefault(COLUMN_NAME_SOURCE,COLUMN_NAME,COLUMN_DEFAULT_2,COLUMN_DEFAULT) as col_default_value_rule",
        "returnColIsNull(COLUMN_NAME_SOURCE,COLUMN_NAME,IS_NULLABLE_2,IS_NULLABLE) as col_is_null_rule",
        "returnColDataType(COLUMN_NAME_SOURCE,COLUMN_NAME,DATA_TYPE_2,DATA_TYPE) as col_data_type_rule",
        "case when CHARACTER_MAXIMUM_LENGTH_2 is not null and CHARACTER_MAXIMUM_LENGTH is not null then " +
          "returnColStringLength(COLUMN_NAME_SOURCE,COLUMN_NAME,CHARACTER_MAXIMUM_LENGTH_2,CHARACTER_MAXIMUM_LENGTH) else -1 end as col_string_length_rule",
        "case when NUMERIC_PRECISION_2 is not null and NUMERIC_PRECISION is not null then " +
          "returnColNumTypeLength(COLUMN_NAME_SOURCE,COLUMN_NAME,NUMERIC_PRECISION_2,NUMERIC_PRECISION) else -1 end as col_number_length_rule",
        "case when NUMERIC_SCALE_2 is not null and NUMERIC_SCALE is not null then " +
          "returnColNumTypeLength(COLUMN_NAME_SOURCE,COLUMN_NAME,NUMERIC_SCALE_2,NUMERIC_SCALE) else -1 end as col_number_scala_rule",
        "case when DATETIME_PRECISION_2 is not null and DATETIME_PRECISION is not null then " +
          "returnColNumTypeLength(COLUMN_NAME_SOURCE,COLUMN_NAME,DATETIME_PRECISION_2,DATETIME_PRECISION) else -1 end as col_time_length_rule",
        "returnColType(COLUMN_NAME_SOURCE,COLUMN_NAME,COLUMN_TYPE_2,COLUMN_TYPE) as col_type_rule",
        "returnColKey(COLUMN_NAME_SOURCE,COLUMN_NAME,COLUMN_KEY_2,COLUMN_KEY) as col_key_rule"
      )
      .where("TABLE_SCHEMA_SOURCE = 'bzn_manage' and TABLE_NAME in ('bs_channel_bznmana')")

    val resPrd= inforSchema106Data.join(inforSchema1Data,'TABLE_NAME==='TABLE_NAME_PRD and 'COLUMN_NAME==='COLUMN_NAME_SOURCE,"leftouter")
      .selectExpr(
        "TABLE_SCHEMA",
        "TABLE_SCHEMA_SOURCE",
        "TABLE_NAME",
        "TABLE_NAME_SOURCE",
        "COLUMN_NAME",
        "COLUMN_NAME_SOURCE",
        "returnColNew(COLUMN_NAME_SOURCE,COLUMN_NAME) as col_is_new_rule",
        "returnColUpdata(COLUMN_NAME_SOURCE,COLUMN_NAME) as col_is_update_rule",
        "returnColDefault(COLUMN_NAME_SOURCE,COLUMN_NAME,COLUMN_DEFAULT_1,COLUMN_DEFAULT) as col_default_value_rule",
        "returnColIsNull(COLUMN_NAME_SOURCE,COLUMN_NAME,IS_NULLABLE_1,IS_NULLABLE) as col_is_null_rule",
        "returnColDataType(COLUMN_NAME_SOURCE,COLUMN_NAME,DATA_TYPE_1,DATA_TYPE) as col_data_type_rule",
        "case when CHARACTER_MAXIMUM_LENGTH_1 is not null and CHARACTER_MAXIMUM_LENGTH is not null then " +
          "returnColStringLength(COLUMN_NAME_SOURCE,COLUMN_NAME,CHARACTER_MAXIMUM_LENGTH_1,CHARACTER_MAXIMUM_LENGTH) else -1 end as col_string_length_rule",
        "case when NUMERIC_PRECISION_1 is not null and NUMERIC_PRECISION is not null then " +
          "returnColNumTypeLength(COLUMN_NAME_SOURCE,COLUMN_NAME,NUMERIC_PRECISION_1,NUMERIC_PRECISION) else -1 end as col_number_length_rule",
        "case when NUMERIC_SCALE_1 is not null and NUMERIC_SCALE is not null then " +
          "returnColNumTypeLength(COLUMN_NAME_SOURCE,COLUMN_NAME,NUMERIC_SCALE_1,NUMERIC_SCALE) else -1 end as col_number_scala_rule",
        "case when DATETIME_PRECISION_1 is not null and DATETIME_PRECISION is not null then " +
          "returnColNumTypeLength(COLUMN_NAME_SOURCE,COLUMN_NAME,DATETIME_PRECISION_1,DATETIME_PRECISION) else -1 end as col_time_length_rule",
        "returnColType(COLUMN_NAME_SOURCE,COLUMN_NAME,COLUMN_TYPE_1,COLUMN_TYPE) as col_type_rule",
        "returnColKey(COLUMN_NAME_SOURCE,COLUMN_NAME,COLUMN_KEY_1,COLUMN_KEY) as col_key_rule"
      )
      .where("TABLE_SCHEMA_SOURCE = 'bznprd' and TABLE_NAME in ('odr_policy_insurant_bznprd','ent_enterprise_info_bznprd','plc_policy_preserve_bznprd','odr_order_info_bznprd','odr_policy_bznprd','pdt_product_bznprd','pdt_product_sku_bznprd','odr_policy_insured_child_bznprd','odr_policy_holder_bznprd','plc_policy_preserve_insured_child_bznprd')")

    val resApi= inforSchema106Data.join(inforSchema1Data,'TABLE_NAME==='TABLE_NAME_API and 'COLUMN_NAME==='COLUMN_NAME_SOURCE,"leftouter")
      .selectExpr(
        "TABLE_SCHEMA",
        "TABLE_SCHEMA_SOURCE",
        "TABLE_NAME",
        "TABLE_NAME_SOURCE",
        "COLUMN_NAME",
        "COLUMN_NAME_SOURCE",
        "returnColNew(COLUMN_NAME_SOURCE,COLUMN_NAME) as col_is_new_rule",
        "returnColUpdata(COLUMN_NAME_SOURCE,COLUMN_NAME) as col_is_update_rule",
        "returnColDefault(COLUMN_NAME_SOURCE,COLUMN_NAME,COLUMN_DEFAULT_1,COLUMN_DEFAULT) as col_default_value_rule",
        "returnColIsNull(COLUMN_NAME_SOURCE,COLUMN_NAME,IS_NULLABLE_1,IS_NULLABLE) as col_is_null_rule",
        "returnColDataType(COLUMN_NAME_SOURCE,COLUMN_NAME,DATA_TYPE_1,DATA_TYPE) as col_data_type_rule",
        "case when CHARACTER_MAXIMUM_LENGTH_1 is not null and CHARACTER_MAXIMUM_LENGTH is not null then " +
          "returnColStringLength(COLUMN_NAME_SOURCE,COLUMN_NAME,CHARACTER_MAXIMUM_LENGTH_1,CHARACTER_MAXIMUM_LENGTH) else -1 end as col_string_length_rule",
        "case when NUMERIC_PRECISION_1 is not null and NUMERIC_PRECISION is not null then " +
          "returnColNumTypeLength(COLUMN_NAME_SOURCE,COLUMN_NAME,NUMERIC_PRECISION_1,NUMERIC_PRECISION) else -1 end as col_number_length_rule",
        "case when NUMERIC_SCALE_1 is not null and NUMERIC_SCALE is not null then " +
          "returnColNumTypeLength(COLUMN_NAME_SOURCE,COLUMN_NAME,NUMERIC_SCALE_1,NUMERIC_SCALE) else -1 end as col_number_scala_rule",
        "case when DATETIME_PRECISION_1 is not null and DATETIME_PRECISION is not null then " +
          "returnColNumTypeLength(COLUMN_NAME_SOURCE,COLUMN_NAME,DATETIME_PRECISION_1,DATETIME_PRECISION) else -1 end as col_time_length_rule",
        "returnColType(COLUMN_NAME_SOURCE,COLUMN_NAME,COLUMN_TYPE_1,COLUMN_TYPE) as col_type_rule",
        "returnColKey(COLUMN_NAME_SOURCE,COLUMN_NAME,COLUMN_KEY_1,COLUMN_KEY) as col_key_rule"
      )
      .where("TABLE_SCHEMA_SOURCE = 'api_open' and TABLE_NAME in ('open_policy_bznapi')")

    val res = resCen.unionAll(resBusi).unionAll(resManage).unionAll(resPrd).unionAll(resApi)
      .selectExpr(
        "getUUID() as id",
        "TABLE_SCHEMA",
        "TABLE_SCHEMA_SOURCE",
        "TABLE_NAME",
        "TABLE_NAME_SOURCE",
        "COLUMN_NAME",
        "COLUMN_NAME_SOURCE",
        "col_is_new_rule",
        "col_is_update_rule",
        "col_default_value_rule",
        "col_is_null_rule",
        "col_data_type_rule",
        "col_string_length_rule",
        "col_number_length_rule",
        "col_number_scala_rule",
        "col_time_length_rule",
        "col_type_rule",
        "col_key_rule",
        "date_format(now(),'yyyy-MM-dd HH:mm:ss') as dw_create_time"
      )

    res.show(2000)
  }
}
