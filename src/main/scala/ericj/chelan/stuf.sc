/**
 * Created by ericj on 13/02/2014.
 */

import com.roundeights.hasher.Digest
import com.roundeights.hasher.Implicits._

val hashMe = "Some String"

// Generate a few hashes

val sha1: Digest = hashMe.sha1
val sha2: Digest = hashMe.sha1

sha1.hash
