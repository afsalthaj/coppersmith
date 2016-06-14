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

package commbank.coppersmith.scalding.lift

import com.twitter.scalding._

import commbank.coppersmith._, Feature.Value
import commbank.coppersmith.scalding.ScaldingBoundFeatureSource

import shapeless._
import shapeless.ops.hlist.Prepend

import ScaldingScalazInstances.typedPipeFunctor

trait ScaldingLift extends Lift[TypedPipe] {

  def lift[S, V <: Value](f:Feature[S,V])(s: TypedPipe[S]): TypedPipe[FeatureValue[V]] = {
    s.flatMap(s => f.generate(s))
  }

  def lift[S](fs: FeatureSet[S])(s: TypedPipe[S]): TypedPipe[FeatureValue[_]] = {
    s.flatMap(s => fs.generate(s))
  }

  def liftJoinInnerInner[S1, S2, S3, J1 : Ordering, J2 : Ordering](
    joined3: Joined3[S1, S2, S3, J1, J2, (S1, S2), (S1, S2, S3)]
  )(s1: TypedPipe[S1], s2: TypedPipe[S2], s3: TypedPipe[S3]): TypedPipe[(S1, S2, S3)] = {
    s1.groupBy(joined3.s1j1).join(s2.groupBy(joined3.s2j1)).values
      .groupBy(joined3.s12j2).join(s3.groupBy(joined3.s3j2)).values
      .map { case ((s1, s2), s3) => (s1, s2, s3) }
  }
  def liftJoinLeftInner[S1, S2, S3, J1 : Ordering, J2 : Ordering](
    joined3: Joined3[S1, S2, S3, J1, J2, (S1, Option[S2]), (S1, Option[S2], S3)]
  )(s1: TypedPipe[S1], s2: TypedPipe[S2], s3: TypedPipe[S3]): TypedPipe[(S1, Option[S2], S3)] = {
    s1.groupBy(joined3.s1j1).leftJoin(s2.groupBy(joined3.s2j1)).values
      .groupBy(joined3.s12j2).join(s3.groupBy(joined3.s3j2)).values
      .map { case ((s1, s2), s3) => (s1, s2, s3) }
  }
  def liftJoinInnerLeft[S1, S2, S3, J1 : Ordering, J2 : Ordering](
    joined3: Joined3[S1, S2, S3, J1, J2, (S1, S2), (S1, S2, Option[S3])]
  )(s1: TypedPipe[S1], s2: TypedPipe[S2], s3: TypedPipe[S3]): TypedPipe[(S1, S2, Option[S3])] = {
    s1.groupBy(joined3.s1j1).join(s2.groupBy(joined3.s2j1)).values
      .groupBy(joined3.s12j2).leftJoin(s3.groupBy(joined3.s3j2)).values
      .map { case ((s1, s2), s3) => (s1, s2, s3) }
  }
  def liftJoinLeftLeft[S1, S2, S3, J1 : Ordering, J2 : Ordering](
    joined3: Joined3[S1, S2, S3, J1, J2, (S1, Option[S2]), (S1, Option[S2], Option[S3])]
  )(s1: TypedPipe[S1], s2: TypedPipe[S2], s3: TypedPipe[S3]): TypedPipe[(S1, Option[S2], Option[S3])] = {
    s1.groupBy(joined3.s1j1).leftJoin(s2.groupBy(joined3.s2j1)).values
      .groupBy(joined3.s12j2).leftJoin(s3.groupBy(joined3.s3j2)).values
      .map { case ((s1, s2), s3) => (s1, s2, s3) }
  }

  def liftJoinInnerLeftInner[S1, S2, S3, S4, J1 : Ordering, J2 : Ordering, J3 : Ordering](
    joined4: Joined4[S1, S2, S3, S4, J1, J2, J3, (S1, S2), (S1, S2, Option[S3]), (S1, S2, Option[S3], S4)]
  )(s1: TypedPipe[S1], s2: TypedPipe[S2], s3: TypedPipe[S3], s4: TypedPipe[S4]): TypedPipe[(S1, S2, Option[S3], S4)] = {
    s1.groupBy(joined4.s1j1).join(s2.groupBy(joined4.s2j1)).values
      .groupBy(joined4.s12j2).leftJoin(s3.groupBy(joined4.s3j2)).values
      .map { case ((s1, s2), s3) => (s1, s2, s3) }
      .groupBy(joined4.s123j3).join(s4.groupBy(joined4.s4j3)).values
      .map { case ((s1, s2, s3), s4) => (s1, s2, s3, s4) }
  }
  // etc

  def innerJoinNext[LeftSides <: HList, RightSide, J : Ordering, Out <: HList]
  (l: LeftSides => J, r: RightSide => J )
  (a:TypedPipe[LeftSides], b: TypedPipe[RightSide])
  (implicit pp: Prepend.Aux[LeftSides, RightSide :: HNil, Out])
  : TypedPipe[Out] = {
    a.groupBy(l).join(b.groupBy(r)).values.map { case (hl, r) => hl :+ r }
  }
  def leftJoinNext[LeftSides <: HList, RightSide, J : Ordering, Out <: HList]
  (l: LeftSides => J, r: RightSide => J )
  (a:TypedPipe[LeftSides], b: TypedPipe[RightSide])
  (implicit pp: Prepend.Aux[LeftSides, Option[RightSide] :: HNil, Out])
  : TypedPipe[Out] =
    a.groupBy(l).leftJoin(b.groupBy(r)).values.map {case (hl, r) => hl :+ r}


  def liftBinder[S, U <: FeatureSource[S, U], B <: SourceBinder[S, U, TypedPipe]](
    underlying: U,
    binder: B,
    filter: Option[S => Boolean]
  ) = ScaldingBoundFeatureSource(underlying, binder, filter)

  def liftFilter[S](p: TypedPipe[S], f: S => Boolean) = p.filter(f)
}

object scalding extends ScaldingLift
