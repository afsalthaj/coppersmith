object MultiwayJoinGenerator {
  val prelude =
    """/*
    | * Generated file, do not modify.
    | *
    | * File generated by https://github.com/CommBank/coppersmith/project/MultiwayJoinGenerator.scala
    | */
    |package commbank.coppersmith.generated
    |""".stripMargin


  // 1234...
  def numbersTo(level: Int): String = 1.to(level).mkString

  // S123...
  def sourceTypeParam(level: Int): String = "S" + numbersTo(level)

  // S1, S2, S3, ...
  def sourceTypeParams(level: Int): List[String] = 1.to(level).map(l => s"S$l").toList

  // J1, J2, J3, ...
  def joinTypeParams(level: Int): List[String] = 1.to(level - 1).map(l => s"J$l").toList

  // J1 : Ordering, J2 : Ordering, ...
  def joinOrderingTypeParams(level: Int): List[String] = joinTypeParams(level).map(_ + " : Ordering")

  // S12, S123, S1234, ...
  def joinedSourceParams(supportedDepth: Int, level: Int = 2): List[String] = {
    if (level > supportedDepth) Nil
    else ("S" + numbersTo(level)) :: joinedSourceParams(supportedDepth, level + 1)
  }

  def permute[T](values: List[T], level: Int, acc: List[T] = Nil): List[List[T]] =
    if (level == 0) List(acc)
    else values.flatMap(v => permute(values, level - 1, v :: acc))

  case class NameType(name: String, typ: String)

  def generateJoined(supportedDepth: Int): String = prelude + s"""
    |import commbank.coppersmith.FeatureSource
    |
    |${genJoined(supportedDepth)()}
    |""".stripMargin


  def genJoined(supportedDepth: Int)(level: Int = supportedDepth): String = {
    if (level == 1) {
      ""
    } else {
      def joinFunctions(l: Int, limit: Int = 1, acc: List[NameType] = Nil): List[NameType] =
        if (l == limit) {
          acc
        } else {
          //                       eg, s123j3                        eg,  S123 => J3
          val soFar = NameType(s"s${numbersTo(l - 1)}j${l - 1}", s"S${numbersTo(l - 1)} => J${l - 1}")
          //                      eg, s4j3           eg,  S4 => J3
          val next = NameType(s"s${l}j${l - 1}", s"S${l} => J${l - 1}")
          joinFunctions(l - 1, limit, soFar :: next :: acc)
        }


      val srcTypeParams = sourceTypeParams(level).mkString(", ")
      val srcTypeParam = sourceTypeParam(level)
      val nextSrcTypeParams = sourceTypeParams(level + 1).mkString(", ")
      val joinOrderingTypeParams = MultiwayJoinGenerator.joinOrderingTypeParams(level).mkString(", ")
      val joinTypeParams = MultiwayJoinGenerator.joinTypeParams(level).mkString(", ")
      val nextJoinTypeParams = MultiwayJoinGenerator.joinTypeParams(level + 1).mkString(", ")
      val joinedSrcParams = joinedSourceParams(level).mkString(", ")
      val joinFunctionsAndTypes = joinFunctions(level).map(jf =>
        jf.name + ": " + jf.typ
      ).mkString(",\n      |  ")

      genJoined(supportedDepth)(level - 1) + s"""
      |
      |case class Joined${level}[
      |  ${srcTypeParams},
      |  ${joinOrderingTypeParams},
      |  ${joinedSrcParams}
      |](
      |  ${joinFunctionsAndTypes},
      |  filter: Option[${srcTypeParam} => Boolean]
      |) extends FeatureSource[${srcTypeParam}, Joined${level}[${srcTypeParams}, ${joinTypeParams}, ${joinedSrcParams}]](filter) {
      |
      |  type FS = Joined${level}[${srcTypeParams}, ${joinTypeParams}, ${joinedSrcParams}]
      |
      |  def copyWithFilter(filter: Option[${srcTypeParam} => Boolean]) = copy(filter = filter)""".stripMargin +
      {
        if (level < supportedDepth) { s"""
      |
      |  def innerJoinTo[S${level + 1}] =
      |    IncompleteJoin${level + 1}[
      |      ${nextSrcTypeParams}, ${joinTypeParams}, ${joinedSrcParams}, (${srcTypeParam}, S${level + 1})
      |    ](${joinFunctions(level).map(_.name).mkString(", ")})
      |  def leftJoinTo[S${level + 1}] =
      |    IncompleteJoin${level + 1}[
      |      ${nextSrcTypeParams}, ${joinTypeParams}, ${joinedSrcParams}, (${srcTypeParam}, Option[S${level + 1}])
      |    ](${joinFunctions(level).map(_.name).mkString(", ")})""".stripMargin
        } else ""
      } + """
      |}
      |""".stripMargin +
      {
        if (level < supportedDepth) {
          val nextJoinFunctionsAndTypes = joinFunctions(level + 1, level).map(jf =>
          jf.name + ": " + jf.typ
        ).mkString(",\n    ")

        s"""
        |
        |case class IncompleteJoin${level + 1}[${nextSrcTypeParams}, ${joinOrderingTypeParams}, ${joinedSrcParams}, ${sourceTypeParam(level + 1)}](
        |  ${joinFunctionsAndTypes}
        |) {
        |  def on[J${level} : Ordering](
        |    ${nextJoinFunctionsAndTypes}
        |  ): Joined${level + 1}[${nextSrcTypeParams}, ${nextJoinTypeParams}, ${joinedSourceParams(level + 1).mkString(", ")}] =
        |    Joined${level + 1}(${joinFunctions(level + 1).map(_.name).mkString(", ")}, None)
        |}""".stripMargin
        } else ""
      }.stripMargin
    }
  }

  sealed abstract class JoinType(val camelName: String)
  case object Inner extends JoinType("Inner")
  case object Left  extends JoinType("Left")
//  case object Right extends JoinType("Right")

  def joinTypes = List[JoinType](Inner, Left/*, Right*/)

  def generateBindings(supportedDepth: Int): String = prelude + s"""
    |import commbank.coppersmith.{DataSource, Lift}
    |
    |trait GeneratedBindings {
    |${genBindings(supportedDepth)()}
    |}
    |""".stripMargin

  def genBindings(supportedDepth: Int)(level: Int = supportedDepth): String = {
    if (level == 1) {
      ""
    } else {
      genBindings(supportedDepth)(level - 1) + "\n\n" + {

        val permutations: List[List[JoinType]] = permute(joinTypes, level - 1)
        permutations.map(permutation => {
          val srcTypeParams = sourceTypeParams(level).mkString(", ")
          val joinOrderingTypeParams = MultiwayJoinGenerator.joinOrderingTypeParams(level).mkString(", ")
          val joinTypeParams = MultiwayJoinGenerator.joinTypeParams(level).mkString(", ")
          val sources = 1.to(level).map(l => s"s${l}: DataSource[S${l}, P]").mkString(",\n    ")
          val sourceNames = 1.to(level).map(l => s"s${l}").mkString(", ")
          val joinType = permutation.map(_.camelName).mkString

          s"""
          |  def join${joinType}[${srcTypeParams}, ${joinOrderingTypeParams}, P[_] : Lift](
          |    ${sources}
          |  ) = Joined${level}${joinType}Binder[${srcTypeParams}, ${joinTypeParams}, P](${sourceNames})
          |""".stripMargin.trim
        }).mkString("\n")
      }
    }
  }

  def generateBinders(supportedDepth: Int): String = prelude + s"""
    |import commbank.coppersmith.{DataSource, Lift, SourceBinder}
    |
    |${genBinders(supportedDepth)()}
    |""".stripMargin

  def genBinders(supportedDepth: Int)(level: Int = supportedDepth): String = {

    if (level == 1) {
      ""
    } else {
      genBinders(supportedDepth)(level - 1) + "\n\n" + {

        val permutations: List[List[JoinType]] = permute(joinTypes, level - 1)
        permutations.map(permutation => {
          val srcTypeParams = sourceTypeParams(level).mkString(", ")
          val joinOrderingTypeParams = MultiwayJoinGenerator.joinOrderingTypeParams(level).mkString(", ")
          val joinTypeParams = MultiwayJoinGenerator.joinTypeParams(level).mkString(", ")
          val sources = 1.to(level).map(l => s"s${l}: DataSource[S${l}, P]").mkString(",\n  ")
          val sourceNames = 1.to(level).map(l => s"s${l}").mkString(", ")
          val loadCalls = 1.to(level).map(l => s"s${l}.load").mkString(", ")

          val joinType = permutation.map(_.camelName).mkString
          def joinedSrcParams(l: Int = 2): String =
            if (l > level) {
              ""
            } else {
              "(S1, " + permutation.zipWithIndex.take(l).map {
                case (Inner, idx) => s"S${idx + 1}"
                case (Left, idx) => s"Option[S${idx + 1}]"
              }.mkString(", ") + ")" + { if (l == level) "" else ", " + joinedSrcParams(l + 1) }
            }

          s"""
          |case class Joined${level}${joinType}Binder[${srcTypeParams}, ${joinOrderingTypeParams}, P[_] : Lift](
          |  ${sources}
          |) extends SourceBinder[${joinedSrcParams(level)}, Joined${level}[${srcTypeParams}, ${joinTypeParams}, ${joinedSrcParams()}], P] {
          |  def bind(j: Joined${level}[${srcTypeParams}, ${joinTypeParams}, ${joinedSrcParams()}]): P[${joinedSrcParams(level)}] = {
          |    implicitly[Lift[P]].liftJoin${joinType}(j)(${loadCalls})
          |  }
          |}
          """.stripMargin.trim
        }).mkString("\n")
      }
    }
  }

  def generateLift(supportedDepth: Int): String = prelude + s"""
    |trait GeneratedLift[P[_]] {
    |${genLift(supportedDepth)()}
    |}
    |""".stripMargin

  def genLift(supportedDepth: Int)(level: Int = supportedDepth): String = {
    if (level == 1) {
      ""
    } else {
      genLift(supportedDepth)(level - 1) + "\n\n" + {

        val permutations: List[List[JoinType]] = permute(joinTypes, level - 1)
        permutations.map(permutation => {
          val srcTypeParams = sourceTypeParams(level).mkString(", ")
          val joinOrderingTypeParams = MultiwayJoinGenerator.joinOrderingTypeParams(level).mkString(", ")
          val joinTypeParams = MultiwayJoinGenerator.joinTypeParams(level).mkString(", ")
          val sources = 1.to(level).map(l => s"s${l}: P[S${l}]").mkString(", ")

          val joinType = permutation.map(_.camelName).mkString
          def joinedSrcParams(l: Int = 2): String =
            if (l > level) {
              ""
            } else {
              "(S1, " + permutation.zipWithIndex.take(l).map {
                case (Inner, idx) => s"S${idx + 1}"
                case (Left, idx) => s"Option[S${idx + 1}]"
              }.mkString(", ") + ")" + { if (l == level) "" else ", " + joinedSrcParams(l + 1) }
            }

          s"""
          |  def liftJoin${joinType}[${srcTypeParams}, ${joinOrderingTypeParams}](
          |    joined${level}: Joined${level}[${srcTypeParams}, ${joinTypeParams}, ${joinedSrcParams()}]
          |  )(${sources}): P[${joinedSrcParams(level)}] = ???""".stripMargin.trim
        }).mkString("\n")
      }
    }
  }
}
