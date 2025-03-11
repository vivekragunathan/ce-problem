package k64

import cats.Applicative
import cats.effect.implicits._
import cats.effect.kernel.{Async, Fiber}
import cats.effect.std.Queue
import cats.effect.{Resource, Sync}
import cats.implicits._

import scala.concurrent.duration.{DurationInt, FiniteDuration}

trait BufferRes[F[_], A] {
  def publish(item: A): F[Unit]
  def release: F[Unit]
}

object BufferRes {

  implicit final class QueueOps[F[_]: Async, A](queue: Queue[F, A]) {
    def takeN(n: Int): F[List[A]] = {
      def loop(acc: List[A]): F[List[A]] =
        if (acc.size >= n) Applicative[F].pure(acc)
        else queue.take.flatMap(element => loop(acc :+ element))

      loop(List.empty[A])
    }

    def takeN2(n: Int, timeout: FiniteDuration): F[List[A]] = {
      val startTime = System.currentTimeMillis()

      def loop(remaining: Int, acc: List[A]): F[List[A]] =
        if (remaining <= 0) Applicative[F].pure(acc)
        else if ((System.currentTimeMillis() - startTime) >= timeout.toMillis) {
          // println(s"@@@@ Timed out @@@@")
          Applicative[F].pure(acc)
        } else
          queue.tryTake
            .flatMap {
              case Some(element) =>
                loop(remaining - 1, acc :+ element)
              case None =>
                loop(remaining, acc)
            }

      loop(n, List.empty[A])
    }

    def takeN3(n: Int, timeout: FiniteDuration): F[List[A]] = {
      def loop(remaining: Int, acc: List[A]): F[List[A]] =
        if (remaining <= 0) {
          println(s"\t\ttakenN ($n) ==> $acc")
          Applicative[F].pure(acc)
        } else
          queue.tryTake
            .flatMap {
              case Some(element) =>
                println(s"\t\tTaken element: $element, remaining: ${remaining - 1}, acc: $acc")
                // If an element is taken, accumulate it and continue
                loop(remaining - 1, acc :+ element)
              case None =>
                // If no element is available, continue checking
                loop(remaining, acc)
            }

      // Start the reading operation with a timeout
      val initialAcc: F[List[A]] = loop(n, List.empty[A])

      // Create a timeout effect that will return whatever has been accumulated so far
      val timeoutEffect: F[List[A]] = Async[F].sleep(timeout).flatMap(_ => initialAcc)

      println(s"@@@ taken($n) =================>")

      // Race the loop against the timeout
      Async[F].race(initialAcc, timeoutEffect).flatMap {
        case Left(result)       => Applicative[F].pure(result) // Successfully read elements
        case Right(accumulated) => Applicative[F].pure(accumulated) // Timeout occurred, return accumulated results
      }
    }

    def takeN(n: Int, timeout: FiniteDuration): F[List[A]] = {
      def readItems(acc: List[A], remaining: Int): F[List[A]] =
        if (remaining <= 0) Applicative[F].pure(acc)
        else
          queue.tryTake.flatMap {
            case Some(item) =>
              readItems(acc :+ item, remaining - 1) // Continue reading
            case None =>
              Async[F].sleep(100.millis).flatMap(_ => readItems(acc, remaining)) // Wait a bit and try again
          }

      // Start reading items and set a timeout
      val readTask = readItems(List.empty, n)

      // Timeout task that will return whatever has been read so far
      val timeoutTask = Async[F].sleep(timeout).flatMap(_ => readTask) // Return whatever has been read so far

      // Race the reading task against the timeout
      readTask.race(timeoutTask).map(_.merge) // Merge the results
    }
  }

  def apply[F[_]: Async, A](
    batchSize: Int,
    lingerTimeout: FiniteDuration,
    tokenSink: Resource[F, Set[A] => F[Unit]]
  ): F[BufferRes[F, A]] = {
    def flush(source: Queue[F, A], sink: Set[A] => F[Unit]): F[Unit] =
      source
        .tryTakeN(None)
        .flatMap { items =>
          println(s"**** Flushing ${items.size} elements from buffer ...")
          items.grouped(batchSize).map(_.toSet).toList.traverse_(sink)
        }

    def pollLoop(
      source: Queue[F, A],
      sink: Set[A] => F[Unit]
    ): F[Fiber[F, Throwable, Unit]] =
      source
        .takeN(batchSize, lingerTimeout)
        // .timeoutTo(lingerTimeout, source.tryTakeN(batchSize))
        // .takeN(batchSize, lingerTimeout)
        .iterateUntil(_.nonEmpty)
        .map(_.toSet)
        .flatMap(sink)
        .foreverM[Unit]
        .onCancel(flush(source, sink))
        .start

    for {
      _ <- batchSize.pure[F].ensure(new IllegalArgumentException(s"Max batch size must be > 0, was $batchSize"))(_ > 0)
      buffer <- Queue.bounded[F, A](batchSize * 2)
      loop <- tokenSink.use { sink =>
        pollLoop(
          buffer,
          (items: Set[A]) =>
            // No error propagation, no cancellation (to be sure a token taken from a query is published)
            sink(items)
              .handleErrorWith(t => logSinkError(items)(t))
              .uncancelable
        )
      }
    } yield {
      new BufferRes[F, A] {
        override def publish(item: A): F[Unit] =
          buffer.offer(item).handleError { _ =>
            println(s"Item '$item' not published because buffer is full")
          }

        override val release: F[Unit] =
          loop.cancel.map(_ => println("Token sink released"))
      }
    }
  }

  private def logOverflow[F[_]: Sync, A](item: A): F[Unit] =
    Sync[F].delay(
      println(s"Item '$item' not published because buffer is full")
    )

  private def logSinkError[F[_]: Sync, A](batch: Set[A])(t: Throwable): F[Unit] =
    Sync[F].delay {
      val items = batch.map(_.toString).mkString(", ")
      println(s"Some items may not have been published ($items) ::: $t")
      t.printStackTrace()
    }
}
