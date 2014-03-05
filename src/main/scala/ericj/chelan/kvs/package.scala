package ericj.chelan.kvs

import com.roundeights.hasher.Implicits._


/**
 * Created by ericj on 13/02/2014.
 */
object KeyImplicits {

  implicit def String2Key(value: String) = value.sha1.hash

  implicit def Int2Key(value: Int) = value.toString.sha1.hash

}
