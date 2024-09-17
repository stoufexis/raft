package com.stoufexis.raft.kvstore.statemachine

object StateMachine:
  def apply(state: KvState, cmd: KvCommand): (KvState, KvResponse) =
    cmd match
      case KvCommand.Get(keys) =>
        (state, state.valuesForKeys(keys))

      case KvCommand.Update(sets) =>
        (state.setAll(sets), KvResponse.Success)

      case KvCommand.TransactionUpdate(gets, sets) if state.revisionMatches(gets) =>
        (state.setAll(sets), KvResponse.Success)

      case KvCommand.TransactionUpdate(r, _) =>
        (state, state.valuesForKeys(r.revisions.keySet))
