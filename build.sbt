ThisBuild / scalaVersion := "2.13.18"

ThisBuild / semanticdbEnabled := true

lazy val sparkVersion = "4.1.1"

lazy val sparkJavaOptions = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "spark-devcontainer",
    version := "0.1.0-SNAPSHOT",
    libraryDependencies += ("org.apache.spark" %% "spark-sql" % sparkVersion)
      .exclude("commons-logging", "commons-logging"),
    Compile / run / fork := true,
    Compile / run / javaOptions ++= sparkJavaOptions,
    assembly / mainClass := Some("Main"),
    assembly / assemblyJarName := "app.jar",
    assembly / assemblyMergeStrategy := {
      case PathList(
            "META-INF",
            "org",
            "apache",
            "logging",
            "log4j",
            "core",
            "config",
            "plugins",
            "Log4j2Plugins.dat"
          ) =>
        MergeStrategy.discard
      case PathList("META-INF", fileName)
          if fileName.equalsIgnoreCase("MANIFEST.MF") =>
        MergeStrategy.discard
      case PathList("META-INF", "services", _*) =>
        MergeStrategy.concat
      case PathList("META-INF", fileName)
          if fileName.toLowerCase.endsWith(".sf") ||
            fileName.toLowerCase.endsWith(".dsa") ||
            fileName.toLowerCase.endsWith(".rsa") =>
        MergeStrategy.discard
      case PathList("META-INF", _*) =>
        MergeStrategy.first
      case "module-info.class" =>
        MergeStrategy.discard
      case x =>
        (assembly / assemblyMergeStrategy).value(x)
    }
  )
