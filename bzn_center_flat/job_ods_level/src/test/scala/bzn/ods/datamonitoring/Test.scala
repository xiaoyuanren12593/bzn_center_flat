package bzn.ods.datamonitoring

object Test {
  def main(args: Array[String]): Unit = {
    println(SpaceMatching("liuxiang"))





  }


  //匹配空格
  def SpaceMatching(Temp: String): Boolean = {
    if (Temp != null) {
      "^\\s|\\s+$  ".r.findFirstIn(Temp).isDefined
    } else false
  }

  //匹配换行符
  def LinefeedMatching(Temp: String): Boolean = {
    if (Temp != null) {
      "^\\n|\\n+$".r.findFirstIn(Temp).isDefined
    } else false
  }




}
