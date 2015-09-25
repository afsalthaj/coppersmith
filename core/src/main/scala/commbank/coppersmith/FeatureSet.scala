package commbank.coppersmith

import scala.reflect.runtime.universe.TypeTag

import scalaz.syntax.std.boolean.ToBooleanOpsFromBoolean

import au.com.cba.omnia.maestro.api.Field

import Feature._

trait FeatureSet[S] extends MetadataSet[S] {
  def namespace: Feature.Namespace

  def features: Iterable[Feature[S, Value]]

  def generate(source: S): Iterable[FeatureValue[Value]] = features.flatMap(f =>
    f.generate(source)
  )
  def metadata: Iterable[Metadata[S, Value]] = {
    features.map(_.metadata)
  }
}

trait FeatureSetWithTime[S] extends FeatureSet[S] {
  /**
   * Specifies the time associated with a feature. Most of the time that will be
   * the job time, but when it depends on data, this method should be overridden.
   */
  def time(source: S, c: FeatureContext): Time = c.generationTime.getMillis
}

trait MetadataSet[S] {
  def metadata: Iterable[Metadata[S, Value]]
}

abstract class PivotFeatureSet[S : TypeTag] extends FeatureSetWithTime[S] {
  def entity(s: S): EntityId

  def pivot[V <: Value : TypeTag, FV <% V](field: Field[S, FV], humanDescription: String, featureType: Type) =
    Patterns.pivot(namespace, featureType, entity, field, humanDescription)
}

abstract class BasicFeatureSet[S : TypeTag] extends FeatureSetWithTime[S] {
  def entity(s: S): EntityId

  def basicFeature[V <: Value : TypeTag](featureName: Name, humanDescription: String, featureType: Type, value: S => V) =
    Patterns.general(namespace, featureName, humanDescription, featureType, entity, (s: S) => Some(value(s)))
}

abstract class QueryFeatureSet[S : TypeTag, V <: Value : TypeTag] extends FeatureSetWithTime[S] {
  type Filter = S => Boolean

  def featureType:  Feature.Type

  def entity(s: S): EntityId
  def value(s: S):  V

  def queryFeature(featureName: Name, humanDescription: String, filter: Filter) =
    Patterns.general(namespace, featureName, humanDescription, featureType, entity, (s: S) => filter(s).option(value(s)))
}

import scalaz.syntax.foldable1.ToFoldable1Ops
import scalaz.syntax.std.list.ToListOpsFromList
import scalaz.syntax.std.option.ToOptionIdOps

import com.twitter.algebird.{Aggregator, AveragedValue, Monoid, Semigroup}

case class AggregationFeature[S : TypeTag, SV, U, +V <: Value : TypeTag](
  name:        Name,
  description: Description,
  aggregator:  Aggregator[SV, U, V],
  view:        PartialFunction[S, SV],
  featureType: Type = Type.Continuous
) {
  import AggregationFeature.AlgebirdSemigroup
  // Note: Implementation exists here to satisfty feature signature and enable unit testing.
  // Framework should take advantage of aggregators that can run natively on the underlying plumbing.
  def toFeature(namespace: Namespace) = new Feature[(EntityId, Iterable[S]), Value](
    Metadata(namespace, name, description, featureType)
  ) {
    def generate(s: (EntityId, Iterable[S])): Option[FeatureValue[Value]] = {
      val (entity, source) = s
      val sourceView = source.toList.collect(view).toNel
      sourceView.map(nonEmptySource => {
        val value = aggregator.present(
          nonEmptySource.foldMap1(aggregator.prepare)(aggregator.semigroup.toScalaz)
        )
        FeatureValue(entity, name, value)
      })
    }
  }
}

trait AggregationFeatureSet[S] extends FeatureSet[(EntityId, Iterable[S])] {
  def entity(s: S): EntityId

  def aggregationFeatures: Iterable[AggregationFeature[S, _, _, Value]]

  def features = aggregationFeatures.map(_.toFeature(namespace))
}

object AggregationFeature {
  def avg[T](t: T => Double): Aggregator[T, AveragedValue, Double] =
    AveragedValue.aggregator.composePrepare[T](t)

  def max[T, V : Ordering](v: T => V): Aggregator[T, V, V]         = Aggregator.max[V].composePrepare[T](v)
  def min[T, V : Ordering](v: T => V): Aggregator[T, V, V]         = Aggregator.min[V].composePrepare[T](v)
  def uniqueCountBy[S, T](f : S => T): Aggregator[S, Set[T], Int]  = Aggregator.uniqueCount[T].composePrepare(f)

  // TODO: Would be surprised if this doesn't exist elsewhere
  implicit class AlgebirdSemigroup[T](s: Semigroup[T]) {
    def toScalaz = new scalaz.Semigroup[T] { def append(t1: T, t2: =>T): T = s.plus(t1, t2) }
  }
}
