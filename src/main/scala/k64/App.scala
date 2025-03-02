package k64

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, IOApp, Resource}
import cats.implicits._

import scala.concurrent.duration.DurationInt

object App extends IOApp.Simple {
  override def run: IO[Unit] = {
    val res: Resource[IO, Set[Tokenized] => IO[Unit]] =
      Resource.make(IO.pure(theSink _))(_ => IO.unit)

    val sinkIO: IO[TokenSink[IO]] = TokenSink[IO](100, 20.seconds, res)
    val tokenSink: TokenSink[IO]  = sinkIO.unsafeRunSync()

    IO.println("Publishing tokens ...") *>
      publishTokens(tokenSink) *>
      tokenSink.release
  }

  private def publishTokens(tokenSink: TokenSink[IO]): IO[Unit] = IO {
    for (i <- 1 to 100) {
      tokenSink.publish(Token(i.toString), i.toString)
    }
  }

  private def theSink(tokens: Set[Tokenized]): IO[Unit] =
    IO.println(tokens.mkString(","))
}
