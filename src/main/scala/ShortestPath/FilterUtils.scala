package ShortestPath

/**
  * Utility methods for using Max Filter on edges dataset.
  */
object FilterUtils {
  /**
    * Switch filter on/off
    */
  val filter = true
  /**
    * Filter out any IDs above this filter value.
    */
  val maxFilter = 10000

  /**
    * Returns true if both user IDs of given tuple pass the filter test.
    *
    * @param nodes tuple of (userID, userID)
    * @return true if both user IDs pass the filter test, false otherwise
    */
  def passesFilterTest(nodes: (String, String)) = {
    (!filter) || (filter && nodes._1.toInt < maxFilter && nodes._2.toInt < maxFilter)
  }
}
