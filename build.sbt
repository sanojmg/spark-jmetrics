lazy val SparkVersion = "3.1.0"
lazy val FramelessVersion = "0.10.1"
val Http4sVersion = "0.21.22"
val Specs2Version = "4.2.0"
val LogbackVersion = "1.2.3"
val circeVersion = "0.13.0"

def makeColorConsole() = {
  val ansi = System.getProperty("sbt.log.noformat", "false") != "true"
  if (ansi) System.setProperty("scala.color", "true")
}

lazy val root = project.in(file(".")).
  settings(
    name := "spark-jmetrics",
    organization := "io.github.sanojmg",
    scalaVersion := "2.12.10",
    version := "0.1",
    libraryDependencies ++= Seq(
      "org.typelevel"     %% "frameless-dataset"   % FramelessVersion,
      "org.typelevel"     %% "frameless-cats"      % FramelessVersion,
      "org.apache.spark"  %% "spark-core"          % SparkVersion,
      "org.apache.spark"  %% "spark-sql"           % SparkVersion,
      "org.http4s"        %% "http4s-blaze-server" % Http4sVersion,
      "org.http4s"        %% "http4s-dsl"          % Http4sVersion,
      "org.http4s"        %% "http4s-blaze-client" % Http4sVersion,
      "org.specs2"        %% "specs2-core"         % Specs2Version     % "test",
      "ch.qos.logback"    %  "logback-classic"     % LogbackVersion,
      "org.http4s"        %% "http4s-circe"        % Http4sVersion,
      // Optional for auto-derivation of JSON codecs
      "io.circe"          %% "circe-generic"       % circeVersion,
      // Optional for string interpolation to JSON model
      "io.circe"          %% "circe-literal"       % circeVersion,
      "io.circe"          %% "circe-optics"        % circeVersion
    ),
    scalacOptions += "-Ypartial-unification",
    initialize ~= { _ => makeColorConsole() },
    initialCommands in console :=
      """
        |import org.apache.spark.{SparkConf, SparkContext}
        |import org.apache.spark.sql.SparkSession
        |import frameless.functions.aggregate._
        |import frameless.syntax._
        |
        |val conf = new SparkConf().setMaster("local[*]").setAppName("frameless-repl").set("spark.ui.enabled", "false")
        |implicit val spark = SparkSession.builder().config(conf).appName("spark-jmetrics").getOrCreate()
        |
        |import spark.implicits._
        |
        |spark.sparkContext.setLogLevel("WARN")
        |
        |import frameless.TypedDataset
      """.stripMargin,
    cleanupCommands in console :=
      """
        |spark.stop()
      """.stripMargin
  )
