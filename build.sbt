ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "stream-loader-example",
    libraryDependencies ++= Seq(
      "org.apache.parquet"    % "parquet-protobuf"               % "1.13.1",
      "com.thesamet.scalapb" %% "scalapb-runtime"                % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "it.unimi.dsi"          % "fastutil"                       % "8.5.9",
      "com.adform"           %% "stream-loader-hadoop"           % "0.2.14",
      "io.micrometer"         % "micrometer-registry-prometheus" % "1.11.4",
      "io.micrometer"         % "micrometer-registry-jmx"        % "1.11.4",
      "org.scalatest"        %% "scalatest"                      % "3.2.15"                                % "test"
    )
  )

Compile / PB.targets := Seq(
  PB.gens.java -> (Compile / sourceManaged).value
)
