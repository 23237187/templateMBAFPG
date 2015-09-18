

assemblySettings

name := "fpg"

organization := "io.prediction"

libraryDependencies ++= Seq(
  "io.prediction"    %% "core"          % pioVersion.value % "provided",
  "org.apache.spark" %% "spark-core"    % "1.5.0" ,
  "org.apache.spark" %% "spark-mllib"   % "1.5.0" ,
  "org.xerial.snappy" % "snappy-java"   % "1.1.1.7")