package com.stoufexis.raft.model

import com.stoufexis.raft.typeclass.IntLike

opaque type SerialNr = Long

object SerialNr:
  given IntLike[SerialNr]            = IntLike.IntLikeLong
  given CanEqual[SerialNr, SerialNr] = CanEqual.derived
