import Dependencies._
import sbtassembly.AssemblyPlugin.autoImport.assemblyOption

// 添加非托管依赖的jar
unmanagedBase := baseDirectory.value / "lib"
unmanagedJars in Compile := Seq.empty[sbt.Attributed[java.io.File]]

//resolvers +=
resolvers ++= Seq(
  "Restlet Repositories" at "http://maven.restlet.org",
  "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven",
  "bintray-sbt-plugins" at "http://dl.bintray.com/sbt/sbt-plugin-releases",
  "central" at "https://maven.aliyun.com/repository/central"
)

// 公共配置
val commonSettings = Seq(
  version :=  "0.1",
  scalaVersion := "2.10.4",
  //挡在java项目中写中文时，编译会报错，加上该行就行了
  javacOptions ++= Seq("-encoding", "UTF-8"),
  // the 'run' task uses all the libraries, including the ones marked with "provided".
  run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated,
  runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated
)


// 公共的 打包 配置
val commonAssemblySettings = Seq(
  //解决依赖重复的问题
  assemblyMergeStrategy in assembly := {
    case PathList(ps@_*) if ps.last endsWith ".properties" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith "Absent.class" => MergeStrategy.first
    case PathList(ps@_*) if ps.last endsWith ".xml" => MergeStrategy.first
    case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  //执行assembly的时候忽略测试
  test in assembly := {},
  //把scala本身排除在Jar中，因为spark已经包含了scala
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
)


// 主工程
lazy val bznSparkNeed = (project in file("."))
  .settings(
    libraryDependencies ++= allDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    name := "bznCenterFlat"
  )

// 事例项目
lazy val jobUtil = (project in file("job-util"))
  .settings(
    libraryDependencies ++= utilDeps)
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bzn-util.jar"
  )

// ods层模块
lazy val bznOdsLevel = (project in file("job_ods_level"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= bznOdsLevelDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bznOdsLevel.jar"
  )

// dw层模块
lazy val bznDwLevel = (project in file("job_dw_level"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= bznDwLevelDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bznDwLevel.jar"
  )

// dm层模块
lazy val bznDmLevel = (project in file("job_dm_level"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= bznDmLevelDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bznDmLevel.jar"
  )

// c端标签模块
lazy val bznCPersonLabel = (project in file("job_c_Person_label"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= bznCPersonLabelDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bznCPerson.jar"
  )

// 机器学习模块
lazy val bznMLLibGraphX = (project in file("job_mllib_graphx"))
  .dependsOn(jobUtil)
  .settings(
    libraryDependencies ++= bznMLLibGraphXDepsProvided.map(
      _.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    ).map(
      _.excludeAll(ExclusionRule(organization = "javax.servlet"))
    ))
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    //定义jar包的名字
    assemblyJarName in assembly := "bznMLLibGraphX.jar"
  )

