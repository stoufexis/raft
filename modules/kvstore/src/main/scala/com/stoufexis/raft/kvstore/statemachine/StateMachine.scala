package com.stoufexis.raft.kvstore.statemachine

enum Command:
  case Get(keys: Set[String])
  case Update(sets: Map[String, Option[String]])
  case TransactionUpdate(gets: Map[String, BigInt], sets: Map[String, Option[String]])

enum Response:
  case Success
  case Values(values: Map[String, (String, BigInt)])

object StateMachine:
  def apply(state: State, cmd: Command): (State, Response) =
    cmd match
      case Command.Get(keys) =>
        (state, Response.Values(state.getAll(keys)))

      case Command.Update(sets) =>
        (state.setAll(sets), Response.Success)

      case Command.TransactionUpdate(gets, sets) if state.revisionsMatch(gets) =>
        (state.setAll(sets), Response.Success)

      case Command.TransactionUpdate(gets, _) =>
        (state, Response.Values(state.getAll(gets.keySet)))

  case class State(map: Map[String, (String, BigInt)]):
    def setAll(sets: Map[String, Option[String]]): State = State:
      sets.foldLeft(map): (acc, set) =>
        acc.updatedWith(set._1):
          case Some((_, r)) => set._2.map((_, r + 1))
          case None         => set._2.map((_, BigInt(0)))

    def revisionsMatch(revisions: Map[String, BigInt]): Boolean =
      revisions.forall((k, r) => map.get(k).exists(_._2 == r))

    def getAll(keys: Set[String]): Map[String, (String, BigInt)] =
      keys.foldLeft(Map.empty): (acc, key) =>
        acc.updatedWith(key)(_ => map.get(key))
