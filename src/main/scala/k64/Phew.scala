package k64

import cats.Monad
import cats.effect.implicits.genTemporalOps_
import cats.effect.std.Queue
import cats.effect.{Concurrent, Resource, Sync}
import cats.implicits.*
import cats.effect.implicits.*
import cats.syntax.option.catsSyntaxOptionId

import scala.concurrent.duration.FiniteDuration
import cats.effect.kernel.GenTemporal

final case class Token(value: String) extends AnyVal

final case class Tokenized(token: Token, value: String)

object Phew {
  private def logOverflow[F[_]: Sync](tokenValue: Token)(): F[Unit] =
    Sync[F].delay(
      println(
        s"Token '${tokenValue.value}' not published because buffer is full"
      )
    )

  private def logSinkError[F[_]: Sync](
    batch: Set[Token]
  )(t: Throwable): F[Unit] =
    Sync[F].delay {
      val tokens = batch.map(tv => s"'${tv.value}'").mkString(", ")
      println(s"Some tokens may not have been published ($tokens) ::: $t")
      t.printStackTrace()
    }

  def apply[E <: Throwable, F[_]](
    batchSize: Int,
    lingerTimeout: FiniteDuration,
    tokenSink: Resource[F, Set[Tokenized] => F[Unit]]
  )(implicit G: GenTemporal[F, E]) = {
    def flush(source: Queue[F, Tokenized], sink: List[Tokenized] => F[Unit]) =
      source
        .tryTakeN(None)
        .flatMap(_.grouped(batchSize).toList.traverse_(sink))

    def pollLoop(
      source: Queue[F, Tokenized],
      sink: List[Tokenized] => F[Unit]
    ) =
      source
        .tryTakeN(batchSize.some)
        .timeoutTo(lingerTimeout, source.tryTakeN(batchSize.some))
        .iterateUntil(_.nonEmpty)
        .flatMap(sink)
        .foreverM
        .onCancel(flush(source, sink))
        .start

    for {
      _ <- batchSize.pure[F].ensure(new IllegalArgumentException(s"Max batch size must be > 0, was $batchSize"))(_ > 0)
      buffer <- Queue.unbounded[F, Tokenized]
      loop <- tokenSink.use { sink =>
        // No error propagation, no cancellation (to be sure a token taken from a query is published)
        val safeSink = (sink, logSinkError _).mapN(_ handleErrorWith _).map(_.uncancelable)
        pollLoop(buffer, safeSink)
      }
    } yield () // just returning unit for now but have to return a bigger object later
  }
}
