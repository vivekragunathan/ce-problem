lazy val ce_problem =
  project
    .in(file("."))
    .settings(
      name         := "ce-problem",
      version      := "0.1.0",
      organization := "k64",
      scalaVersion := "2.13.16",
      libraryDependencies ++=
        "org.typelevel"   %% "cats-core"   % "2.13.0" ::
          "org.typelevel" %% "cats-effect" % "3.5.7" ::
          "com.beachape"  %% "enumeratum"  % "1.7.5" ::
          "org.rudogma"   %% "supertagged" % "2.0-RC2" ::
          "io.jvm.uuid"   %% "scala-uuid"  % "0.3.1" ::
          "com.chuusai"   %% "shapeless"   % "2.3.12" ::
          Nil,
      scalacOptions ++=
        "-Xsource:3" ::
          Nil
    )
    .settings(
      Test / fork               := true,
      Test / testForkedParallel := true,
      Test / javaOptions += "-DSCALACTIC_FILL_FILE_PATHNAMES=true"
    )
