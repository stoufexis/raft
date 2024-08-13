package com.stoufexis.leader.model

opaque type EntityId = String

object EntityId:
  given CanEqual[EntityId, EntityId] = CanEqual.derived