package com.stoufexis.leader.model

import com.stoufexis.leader.typeclass.IntLike

opaque type Index = Int

object Index:
  given IntLike[Index]         = IntLike.IntLikeInt
  given CanEqual[Index, Index] = CanEqual.derived
