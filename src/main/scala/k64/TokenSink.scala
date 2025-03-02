package k64

import cats.effect.implicits.*
import cats.effect.kernel.{Async, Fiber}
import cats.effect.std.Queue
import cats.effect.{Resource, Sync}
import cats.implicits.*
import cats.syntax.option.catsSyntaxOptionId

import scala.concurrent.duration.FiniteDuration

final case class Token(value: String) extends AnyVal
final case class Tokenized(token: Token, value: String)

trait TokenSink[F[_]] {

  /**
    * Publishes a token along with a corresponding raw value. Does not provide any guarantees about the
    * publishing order.
    *
    * @param token token
    * @param value raw value a token was created for
    */
  def publish(token: Token, value: String): F[Unit]

  /**
    * Closes a token sink and releases resources (if any) allocated for it.
    *
    * @return
    */
  def release: F[Unit]
}

object TokenSink {
  def apply[F[_]](
    batchSize: Int,
    lingerTimeout: FiniteDuration,
    tokenSink: Resource[F, Set[Tokenized] => F[Unit]]
  )(implicit G: Async[F]): F[TokenSink[F]] = {
    def flush(source: Queue[F, Tokenized], sink: Set[Tokenized] => F[Unit]): F[Unit] =
      source
        .tryTakeN(None)
        .flatMap(_.grouped(batchSize).map(_.toSet).toList.traverse_(sink))

    def pollLoop(
      source: Queue[F, Tokenized],
      sink: Set[Tokenized] => F[Unit]
    ): F[Fiber[F, Throwable, Unit]] =
      source
        .tryTakeN(batchSize.some)
        .timeoutTo(lingerTimeout, source.tryTakeN(batchSize.some))
        .iterateUntil(ts => ts.nonEmpty)
        .map(_.toSet)
        .flatMap(sink)
        .foreverM[Unit]
        .onCancel(flush(source, sink))
        .start

    for {
      _ <- batchSize.pure[F].ensure(new IllegalArgumentException(s"Max batch size must be > 0, was $batchSize"))(_ > 0)
      buffer <- Queue.unbounded[F, Tokenized]
      loop <- tokenSink.use { sink =>
        // No error propagation, no cancellation (to be sure a token taken from a query is published)
        val safeSink: Set[Tokenized] => F[Unit] =
          tokens =>
            sink(tokens)
              .handleErrorWith(t => logSinkError(tokens.map(_.token))(t))
              .uncancelable

        pollLoop(buffer, safeSink)
      }
    } yield {
      new TokenSink[F] {
        override def publish(token: Token, value: String): F[Unit] = {
          val t = Tokenized(token, value)
          buffer.offer(t).handleErrorWith(_ => logOverflow(t.token))
        }

        override val release: F[Unit] = loop.cancel
      }
    }
  }

  private def logOverflow[F[_]: Sync](tokenValue: Token): F[Unit] =
    Sync[F].delay(
      println(
        s"Token '${tokenValue.value}' not published because buffer is full"
      )
    )

  private def logSinkError[F[_]: Sync](batch: Set[Token])(t: Throwable): F[Unit] =
    Sync[F].delay {
      val tokens = batch.map(tv => s"'${tv.value}'").mkString(", ")
      println(s"Some tokens may not have been published ($tokens) ::: $t")
      t.printStackTrace()
    }
}
