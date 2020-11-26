object ShortestPaths {

  /**
    * A path from original departure airport to final arrival airport.
    * @param start airport ID for original departure airport for this path
    * @param end airport ID for final arrival airport for this path
    */
  // I envision using this as a key.  We will need a Path for every combination of airports.
  // Two path keys would be equivalent if k1.start == k2.start AND k1.end == k2.end
  // TODO how to codify equivalence?  Is there like an equals method for case classes?
  case class Path(start: String, end: String)

  /**
    * A complete route for a given path from original arrival airport to final destination airport.
    * @param path Path indicating original arrival and final departure airport IDs
    * @param distance cumulative distance for this route so far
    * @param intermediates ordered list of all intermediate airport IDs for this route so far
    * @param isComplete true if the route is complete from departure to arrival; false otherwise
    */
  // Each Path would use a Route to build up the shortest path
  case class Route(path: Path, distance: Int, intermediates: List[String], isComplete: Boolean)

}
