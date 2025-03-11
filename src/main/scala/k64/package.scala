import cats.effect.IO

import scala.concurrent.duration.FiniteDuration

package object k64 {
  def loopWithDelay(n: Int, delay: FiniteDuration)(fn: Int => Unit): IO[Unit] =
    if (n <= 0) IO.unit
    else {
      fn(n)

      for {
        _ <- IO.sleep(delay)
        _ <- loopWithDelay(n - 1, delay)(fn)
      } yield ()
    }
}
