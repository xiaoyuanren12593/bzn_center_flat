/**
  * author:xiaoYuanRen
  * Date:2019/11/8
  * Time:16:31
  * describe: this is new class
  **/
object test {
  def main (args: Array[String]): Unit = {
    val cert = "\\u0012"

    println (new String(cert.getBytes ("ISO8859-1"),"ISO8859-1"))
    println (new String(cert.getBytes ("ISO8859-1"),"utf-8"))
  }

  
}
