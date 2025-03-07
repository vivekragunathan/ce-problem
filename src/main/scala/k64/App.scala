package k64

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, IOApp, Resource}
import cats.implicits._

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object App extends IOApp.Simple {
  override def run: IO[Unit] = {

    val res: Resource[IO, Set[Tokenized] => IO[Unit]] =
      Resource.make(IO.pure(theSink _))(_ => IO.unit)

    val sinkIO: IO[TokenSink[IO]] = TokenSink[IO](5, 5.seconds, res)
    val tokenSink: TokenSink[IO]  = sinkIO.unsafeRunSync()

    publishTokens(tokenSink) *> tokenSink.release
  }

  private def publishTokens2(tokenSink: TokenSink[IO]): IO[Unit] = IO {
    for (i <- 1 to 100) {
      tokenSink
        .publish(Token(i.toString), i.toString)
        .unsafeRunSync()
    }
  }

  private def publishTokens(tokenSink: TokenSink[IO]): IO[Unit] =
    loopWithDelay(20, 1.second) { i =>
      println(s"Publishing token $i ...")
      tokenSink
        .publish(Token(i.toString), i.toString)
        .unsafeRunSync()
    }

  private def theSink(tokens: Set[Tokenized]): IO[Unit] = {
    println(s"Read ${tokens.size} elements from buffer ...")
    IO.println(tokens.mkString(">>>", ",", ""))
  }

  private def loopWithDelay(n: Int, delay: FiniteDuration)(fn: Int => Unit): IO[Unit] =
    if (n <= 0) IO.unit
    else {
      fn(n)
      for {
        _ <- IO.sleep(delay)
        _ <- loopWithDelay(n - 1, delay)(fn)
      } yield ()
    }
}
