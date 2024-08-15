package com.stoufexis.leader.statemachine

import cats.effect.kernel.*
import cats.implicits.given
import com.stoufexis.leader.util.*

/** Allows the running state machine to signal to other processes that it has confirmed it is the
  * leader. Such confirmation is given every time the leader receives heartbeat responses from the
  * majority of the cluster. This is required to implements guards against network partitions - it
  * allows leaders to complete commands right after they have received confirmation that they are
  * still the leader. This can also be used to implement linearizable reads.
  */
class ConfirmLeader[F[_]](
  latch: Ref[F, (Int, Option[Deferred[F, Boolean]])]
)(using F: Concurrent[F]):

  // latch represents the current leader state.
  //
  // The Int is a sequence number that's used to implement scopedAck:
  // each time the resource enclosing the ack effect is closed, the sequence number is incremented.
  // Any subsequent usage of that ack effect will result in an IllegalCallerException.
  // This means each issued ack signal is only live for some scope defined by the user
  //
  // The second member of a tuple represents the current leader status:
  // None means the node is not currently a leader.
  // Some means the node is a leader.
  // Some contains deferred that is completed when the next leader confirmation comes through.

  /** Waits until the next time this node has confirmed it is the leader
    *
    * @return
    *   false if the node is not the leader or stops being the leader while waiting. true if the
    *   node successfully confirms it is the leader
    */
  def await: F[Boolean] =
    latch.get.flatMap:
      case (_, None)           => F.pure(false)
      case (_, Some(deferred)) => deferred.get

  /** Creates an AckNack that can be used only within the scope of the resource. Calls outside this
    * scope will result in an error.
    */
  def scopedAcks: Resource[F, ConfirmLeader.AckNack[F]] =
    Resource
      .make(latch.get.map(_._1))(_ => latch.update((s, d) => (s + 1, d)))
      .map: valid =>
        new:
          def illegal[A]: F[A] = F.raiseError:
            IllegalCallerException("This ack's scope has expired")

          def ack: F[Unit] =
            for
              next <- Deferred[F, Boolean]
              _ <- latch.flatModify:
                case st @ (sNr, _) if sNr == valid => st -> illegal
                case (sNr, None)                   => (sNr, Some(next)) -> F.unit
                case (sNr, Some(deferred))         => (sNr, Some(next)) -> deferred.complete_(true)
            yield ()

          def nack: F[Unit] =
            latch.flatModify:
              case st @ (sNr, _) if sNr == valid => st -> illegal
              case (sNr, None)                   => (sNr, None) -> F.unit
              case (sNr, Some(deferred))         => (sNr, None) -> deferred.complete_(false)

object ConfirmLeader:
  trait AckNack[F[_]]:
    /** Signals that this node has confirmed it is the leader
      */
    def ack: F[Unit]

    /** Signals that this node is not the leader
      */
    def nack: F[Unit]

  def apply[F[_]: Concurrent]: F[ConfirmLeader[F]] =
    Ref.of[F, (Int, Option[Deferred[F, Boolean]])](0, None)
      .map(new ConfirmLeader[F](_))
