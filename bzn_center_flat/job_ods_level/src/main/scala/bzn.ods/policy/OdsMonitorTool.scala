package bzn.ods.policy

/**
  * author:xiaoYuanRen
  * Date:2020/2/12
  * Time:10:27
  * describe: this is new class
  **/
trait OdsMonitorTool {

  /**
    * 字段范围：新增
    */
  def returnColNew(source:String,local:String) = {
    var ruleColNewId = 0
    if(local != source){
      if(local == null && source != null){
        ruleColNewId = 2
      }else{
        ruleColNewId = -1
      }
    }

    ruleColNewId
  }

  /**
    * 字段范围：更新
    */
  def returnColUpdata(source:String,local:String) = {
    var ruleColNewId = 0
    if(local != source){
      if(source == null && local != null){
        ruleColNewId = 1
      }else{
        ruleColNewId = -1
      }
    }

    ruleColNewId
  }

  /**
    * 属性范围：默认值是否一致
    */
  def returnColDefault(sourceCol:String,localCol:String,source:String,local:String) = {
    var ruleColNewId = 0

    if(sourceCol == localCol){
      if(local != source){
        ruleColNewId = 2
      }
    }else{
      ruleColNewId = -1
    }

    ruleColNewId
  }

  /**
    * 属性范围：空值
    * 允许为空改不允许 2
    * 不允许为空改允许 1
    * 与源端一致
    */
  def returnColIsNull(sourceCol:String,localCol:String,source:String,local:String) = {
    var ruleColNewId = 0
    if(sourceCol == localCol){
      if(local == "YES" && source == "NO" ){
        ruleColNewId = 2
      }else if(local == "NO" && source == "YES" ){
        ruleColNewId = 1
      }else{
        ruleColNewId = 0
      }
    }else{
      ruleColNewId = -1
    }

    ruleColNewId
  }

  /**
    * 数据类型
    */
  def returnColDataType(sourceCol:String,localCol:String,source:String,local:String) = {
    var ruleColNewId = 0
    if(sourceCol == localCol){
      if(local != source){
        ruleColNewId = 1
      }
    }else{
      ruleColNewId = -1
    }

    ruleColNewId
  }

  /**
    * 字符串值的范围
    * 扩大 1
    * 缩小 2
    * 与源端一致 0
    */
  def returnColStringLength(sourceCol:String,localCol:String,source:Long,local:Long) = {
    var ruleColNewId = 0
    if(sourceCol == localCol){
      if(local < source){
        ruleColNewId = 1
      }else if(local > source){
        ruleColNewId = 2
      }
    }else{
      ruleColNewId = -1
    }

    ruleColNewId
  }

  /**
    * 数值类型值的范围
    * 扩大 1
    * 缩小 2
    * 与源端一致 0
    */
  def returnColNumTypeLength(sourceCol:String,localCol:String,source:Long,local:Long) = {
    var ruleColNewId = 0

    if(sourceCol == localCol){
      if(local < source){
        ruleColNewId = 1
      }else if(local > source){
        ruleColNewId = 2
      }
    }else{
      ruleColNewId = -1
    }

    ruleColNewId
  }

  /**
    * 数值类型值的范围
    * 扩大 1
    * 缩小 2
    * 与源端一致 0
    */
  def returnColNumTypeScalaLength(sourceCol:String,localCol:String,source:Long,local:Long) = {
    var ruleColNewId = 0

    if(sourceCol == localCol){
      if(local < source){
        ruleColNewId = 1
      }else if(local > source){
        ruleColNewId = 2
      }
    }else{
      ruleColNewId = -1
    }

    ruleColNewId
  }

  /**
    * 日期类型的精度
    * 扩大 1
    * 缩小 2
    * 与源端一致 0
    */
  def returnColTimeTypeLength(sourceCol:String,localCol:String,source:Long,local:Long) = {
    var ruleColNewId = 0

    if(sourceCol == localCol){
      if(local < source){
        ruleColNewId = 1
      }else if(local > source){
        ruleColNewId = 2
      }
    }else{
      ruleColNewId = -1
    }

    ruleColNewId
  }

  /**
    * 索引类型（唯一主键还是多个主键）
    * 与源端不一致 1
    * 与源端一致 0
    */
  def returnColKey(sourceCol:String,localCol:String,source:String,local:String) = {
    var ruleColNewId = 0
    if(sourceCol == localCol){
      if(local != source){
        ruleColNewId = 1
      }
    }else{
      ruleColNewId = -1
    }

    ruleColNewId
  }

  /**
    * 列类型
    * 与源端不一致 1
    * 与源端一致 0
    */
  def returnColType(sourceCol:String,localCol:String,source:String,local:String) = {
    var ruleColNewId = 0
    if(sourceCol == localCol){
      if(local != source){
        ruleColNewId = 1
      }
    }else{
      ruleColNewId = -1
    }

    ruleColNewId
  }
}
