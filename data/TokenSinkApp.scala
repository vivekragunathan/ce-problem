import cats.effect.unsafe.implicits.global
import cats.effect.{IO, IOApp, Resource}
import cats.implicits._

import scala.concurrent.duration.DurationInt

object TokenSinkApp extends IOApp.Simple {
  override def run: IO[Unit] = {

    val res: Resource[IO, Set[Tokenized] => IO[Unit]] =
      Resource.make(IO.pure(theSink _))(_ => IO.unit)

    val sinkIO: IO[TokenSink[IO]] = TokenSink[IO](5, 5.seconds, res)
    val tokenSink: TokenSink[IO]  = sinkIO.unsafeRunSync()

    publishTokens(tokenSink) *> tokenSink.release
  }

  private def publishTokens(tokenSink: TokenSink[IO]): IO[Unit] =
    loopWithDelay(20, 2.seconds) { i =>
      println(s"Publishing token $i ...")
      tokenSink
        .publish(Token(i.toString), i.toString)
        .unsafeRunSync()
    }

  private def theSink(tokens: Set[Tokenized]): IO[Unit] = {
    println(s"Read ${tokens.size} elements from buffer ...")
    IO.println(tokens.mkString(">>>", ",", ""))
  }
}
