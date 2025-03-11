import cats.effect.implicits.monadCancelOps_
import cats.effect.{Async, Deferred, Ref}
import cats.implicits.{catsSyntaxApplicativeId, catsSyntaxMonadError, toFlatMapOps, toFunctorOps}

import scala.collection.immutable.Queue

trait SetBuffer[F[_], A] {

  def add(a: A): F[Boolean]

  def offer(a: A): F[Boolean] = add(a)

  def takeN(n: Int): F[Set[A]]

  def tryTakeN(n: Int): F[Set[A]]

  def tryTakeAll: F[Set[A]] = tryTakeN(Int.MaxValue)
}

object SetBuffer {
  def apply[F[_]: Async, A](maxSize: Int): F[SetBuffer[F, A]] = {

    final case class Take(count: Int, promise: Deferred[F, Set[A]])

    final case class State(waitingTakes: Queue[Take], takenSoFar: Set[A], maxSize: Int) {

      def add(a: A): (State, F[Boolean]) = {
        if (takenSoFar.contains(a)) this -> true.pure[F]
        else if (takenSoFar.size >= maxSize) this -> false.pure[F]
        else {
          val all = takenSoFar + a
          waitingTakes.headOption
            .filter(_.count <= all.size)
            .map { take =>
              val (taken, remaining) = all.splitAt(waitingTakes.head.count)
              copy(waitingTakes = waitingTakes.tail, takenSoFar = remaining) -> take.promise.complete(taken).as(true)
            }
            .getOrElse(copy(takenSoFar = all) -> true.pure[F])
        }
      }

      def takeN(n: Int, onCancelWaiting: Deferred[F, Set[A]] => F[Unit]): (State, F[Set[A]]) =
        if (n <= takenSoFar.size) {
          val (taken, remaining) = takenSoFar.splitAt(n)
          copy(takenSoFar = remaining) -> taken.pure[F]
        } else {
          val promise = Deferred.unsafe[F, Set[A]]
          copy(waitingTakes = waitingTakes :+ Take(n, promise)) -> promise.get.onCancel(onCancelWaiting(promise))
        }

      def tryTakeN(n: Int): (State, Set[A]) = {
        val (taken, remaining) = takenSoFar.splitAt(n)
        copy(takenSoFar = remaining) -> taken
      }

      def removeWaitingTake(promise: Deferred[F, Set[A]]): State =
        copy(waitingTakes = waitingTakes.filterNot(_.promise == promise))
    }

    object State {
      def initial(maxSize: Int): F[State] =
        maxSize
          .pure[F]
          .ensure(new IllegalArgumentException(s"Max buffer size must be > 0, was $maxSize"))(_ > 0)
          .map(State(Queue.empty, Set.empty, _))
    }

    State
      .initial(maxSize)
      .flatMap(Ref.of[F, State])
      .map { state =>
        new SetBuffer[F, A] {

          override def add(a: A): F[Boolean] = state.modify(_.add(a)).flatten.uncancelable

          override def takeN(n: Int): F[Set[A]] = {

            def onCancelWaiting(promise: Deferred[F, Set[A]]): F[Unit] = state.update(_.removeWaitingTake(promise))

            state.modify(_.takeN(n, onCancelWaiting)).flatten
          }

          override def tryTakeN(n: Int): F[Set[A]] = state.modify(_.tryTakeN(n))
        }
      }
  }
}
