//
// Copyright 2016 Commonwealth Bank of Australia
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//        http://www.apache.org/licenses/LICENSE-2.0
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin.depend.versions
import sbt._
import sbt.Keys._

import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._
import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin._

object build extends Build {
  val maestroVersion = "2.20.0-20160520031836-e06bc75"

  lazy val standardSettings =
    Defaults.coreDefaultSettings ++
    uniformPublicDependencySettings ++
    strictDependencySettings ++
    Seq(
      concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),  // because thermometer tests cannot run in parallel
      scalacOptions += "-Xfatal-warnings",
      scalacOptions in (Compile, console) ~= (_.filterNot(Set("-Xfatal-warnings", "-Ywarn-unused-import"))),
      scalacOptions in (Compile, doc) ~= (_ filterNot (_ == "-Xfatal-warnings")),
      scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value,
      dependencyOverrides += "com.chuusai" %% "shapeless" % "2.2.5" //until maestro is updated
    )

  lazy val all = Project(
    id = "all"
    , base = file(".")
    , settings =
      standardSettings
        ++ uniform.project("coppersmith-all", "commbank.coppersmith.all")
        ++ Seq(
        publishArtifact := false
      )
    , aggregate = Seq(core, testProject, examples, scalding, tools)
  )

  lazy val core = Project(
    id = "core"
    , base = file("core")
    , settings =
      standardSettings
   ++ uniform.project("coppersmith-core", "commbank.coppersmith")
   ++ uniformThriftSettings
   ++ Seq(
          libraryDependencies += "org.specs2" %% "specs2-matcher-extra" % versions.specs % "test"
            exclude("org.scala-lang", "scala-compiler"),
          libraryDependencies +=  "io.argonaut" %% "argonaut" % "6.1",
          libraryDependencies ++= depend.testing(configuration = "test"),
          libraryDependencies ++= depend.omnia("maestro", maestroVersion)
      )
  ).configs( IntegrationTest )

  lazy val scalding = Project(
    id = "scalding"
    , base = file("scalding")
    , settings =
      standardSettings
        ++ uniform.project("coppersmith-scalding", "commbank.coppersmith.scalding")
        ++ uniformThriftSettings
        ++ Seq(
        libraryDependencies ++= depend.hadoopClasspath,
        libraryDependencies ++= depend.omnia("maestro-test", maestroVersion, "test"),
        libraryDependencies ++= depend.parquet()
      )
  ).dependsOn(core % "compile->compile;test->test")

  lazy val examples = Project(
    id = "examples"
    , base = file("examples")
    , settings =
      standardSettings
        ++ uniform.project("coppersmith-examples", "commbank.coppersmith.examples")
        ++ uniformThriftSettings
        ++ uniformAssemblySettings
        ++ Seq(
        watchSources <++= baseDirectory map(path => (path / "../USERGUIDE.markdown").get),
        libraryDependencies ++= depend.scalding(),
        libraryDependencies ++= depend.hadoopClasspath,
        libraryDependencies ++= depend.omnia("maestro-test", maestroVersion, "test"),
        sourceGenerators in Compile <+= (sourceManaged in Compile, streams) map { (outdir: File, s) =>
          val infile = "USERGUIDE.markdown"
          val source = io.Source.fromFile(infile)
          val fileContent = try source.mkString finally source.close()
          val sourceCode = """```scala(?s)(.*?)```""".r
          val codeFragments = (sourceCode findAllIn fileContent).matchData.map {
            _.group(1)
          }
          val fragFiles = codeFragments.zipWithIndex.map { case (frag, i) =>
            val newFile = outdir / s"userGuideFragment$i.scala"
            IO.write(newFile, frag)
            newFile
          }.toSeq

          val jobFiles = FeatureJobGenerator.gen(fragFiles).map { case (name, job) =>
            val newFile = outdir / s"${name}Job.scala"
            IO.write(newFile, job)
            newFile
          }

          fragFiles ++ jobFiles
        }
      )
  ).dependsOn(core, scalding, testProject)

  lazy val testProject = Project(
    id = "test"
    , base = file("test")
    , settings =
      standardSettings
        ++ uniform.project("coppersmith-test", "commbank.coppersmith.test")
        ++ uniformThriftSettings
        ++ Seq(
          libraryDependencies ++= depend.testing(configuration = "test"),
          libraryDependencies ++= depend.omnia("maestro-test", maestroVersion)
        )
  ).dependsOn(core)

  lazy val plugin = Project(
    id = "plugin",
    base = file("plugin"),
    settings = uniform.project("coppersmith-plugin", "commbank.coppersmith.plugin") ++ Seq(
      scalaVersion := "2.10.4",
      crossScalaVersions := Seq("2.10.4"),
      sbtPlugin := true,
      scalacOptions := Seq()
    ))

  lazy val tools = Project(
    id = "tools"
    , base = file("tools")
    , settings =
      Defaults.coreDefaultSettings
        ++ uniformDependencySettings
        ++ uniform.project("coppersmith-tools", "commbank.coppersmith.tools")
        ++ Seq(libraryDependencies ++= Seq(
             "io.github.lukehutch" % "fast-classpath-scanner" % "1.9.7",
             "org.specs2"         %% "specs2-matcher-extra"   % versions.specs % "test"
           )
        )
        ++ Seq(
          fork in Test := true,
          javaOptions in Test += {
            val files: Seq[File] = (fullClasspath in Compile).value.files
            val sbtClasspath: String = files.map(x => x.getAbsolutePath).mkString(":")
            s"-Dsbt-classpath=$sbtClasspath"
          }
        )
  ).dependsOn(core)
}
