package com.stoufexis.raft.kvstore.statemachine

enum KvCommand derives CanEqual:
  case Get(keys: Set[String])
  case Update(sets: Map[String, Option[String]])
  case TransactionUpdate(gets: Map[String, Long], sets: Map[String, Option[String]])

enum KvResponse derives CanEqual:
  case Success
  case Values(values: Map[String, (String, Long)])

case class State(map: Map[String, (String, Long)]):
  def setAll(sets: Map[String, Option[String]]): State = State:
    sets.foldLeft(map): (acc, set) =>
      acc.updatedWith(set._1):
        case Some((_, r)) => set._2.map((_, r + 1))
        case None         => set._2.map((_, 0))

  def revisionsMatch(revisions: Map[String, Long]): Boolean =
    revisions.forall((k, r) => map.get(k).exists(_._2 == r))

  def getAll(keys: Set[String]): Map[String, (String, Long)] =
    keys.foldLeft(Map.empty): (acc, key) =>
      acc.updatedWith(key)(_ => map.get(key))

object StateMachine:
  import scodec.*
  import scodec.bits.*
  import scodec.codecs.*

  // Create a codec for an 8-bit unsigned int followed by an 8-bit unsigned int followed by a 16-bit unsigned int
  val firstCodec: Codec[(Int, Int, Int)] = uint8 :: uint8 :: uint16

  def apply(state: State, cmd: KvCommand): (State, KvResponse) =
    cmd match
      case KvCommand.Get(keys) =>
        (state, KvResponse.Values(state.getAll(keys)))

      case KvCommand.Update(sets) =>
        (state.setAll(sets), KvResponse.Success)

      case KvCommand.TransactionUpdate(gets, sets) if state.revisionsMatch(gets) =>
        (state.setAll(sets), KvResponse.Success)

      case KvCommand.TransactionUpdate(gets, _) =>
        (state, KvResponse.Values(state.getAll(gets.keySet)))
