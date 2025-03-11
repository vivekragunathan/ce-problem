import cats.Applicative
import cats.effect.implicits._
import cats.effect.kernel.{Async, Fiber}
import cats.effect.std.Queue
import cats.effect.{Resource, Sync}
import cats.implicits._

import scala.concurrent.duration.FiniteDuration

final case class Token(value: String) extends AnyVal
final case class Tokenized(token: Token, value: String)

trait TokenSink[F[_]] {
  def publish(token: Token, value: String): F[Unit]
  def release: F[Unit]
}

object TokenSink {

  implicit final class QueueOps[F[_]: Async, A](queue: Queue[F, A]) {
    def takeN(n: Int): F[List[A]] = {
      def loop(acc: List[A]): F[List[A]] =
        if (acc.size >= n) Applicative[F].pure(acc)
        else queue.take.flatMap(element => loop(acc :+ element))

      loop(List.empty[A])
    }

    def takeN(n: Int, timeout: FiniteDuration): F[List[A]] =
      Async[F]
        .race(Async[F].sleep(timeout), takeN(n))
        .flatMap {
          case Left(_) =>
            queue.tryTakeN(Some(n)) // Timeout case
          case Right(result) =>
            Applicative[F].pure(result) // takeN completed
        }
  }

  def apply[F[_]: Async](
    batchSize: Int,
    lingerTimeout: FiniteDuration,
    tokenSink: Resource[F, Set[Tokenized] => F[Unit]]
  ): F[TokenSink[F]] = {
    def flush[A](source: Queue[F, A], sink: Set[A] => F[Unit]): F[Unit] =
      source
        .tryTakeN(None)
        .flatMap { items =>
          println("**** Flushing elements from buffer ...")
          items.grouped(batchSize).map(_.toSet).toList.traverse_(sink)
        }

    def pollLoop[A](
      source: Queue[F, A],
      sink: Set[A] => F[Unit]
    ): F[Fiber[F, Throwable, Unit]] =
      source
        .takeN(batchSize, lingerTimeout)
        .iterateUntil(_.nonEmpty)
        .map(_.toSet)
        .flatMap(sink)
        .foreverM[Unit]
        .onCancel(flush(source, sink))
        .start

    for {
      _ <- batchSize.pure[F].ensure(new IllegalArgumentException(s"Max batch size must be > 0, was $batchSize"))(_ > 0)
      buffer <- Queue.bounded[F, Tokenized](batchSize * 2)
      loop <- tokenSink.use { sink =>
        pollLoop(
          buffer,
          (tokens: Set[Tokenized]) =>
            // No error propagation, no cancellation (to be sure a token taken from a query is published)
            sink(tokens)
              .handleErrorWith(t => logSinkError(tokens.map(_.token))(t))
              .uncancelable
        )
      }
    } yield {
      new TokenSink[F] {
        override def publish(token: Token, value: String): F[Unit] = {
          val t = Tokenized(token, value)
          buffer.offer(t).handleErrorWith(_ => logOverflow(t.token))
        }

        override val release: F[Unit] =
          loop.cancel.map(_ => println("Token sink released"))
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
