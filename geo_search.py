import redis
from typing import List, Dict, Optional


class GeoSearchService:
    """
    Geo-search functionality using Redis geospatial indexes.
    Stores locations (e.g., stores, drivers, users) and supports nearby queries.
    """

    def __init__(self, key: str, redis_client: Optional[redis.Redis] = None) -> None:
        self.key = key  # e.g., "geo:stores:thimphu"
        self.r = redis_client or redis.Redis(
            host="127.0.0.1",
            port=6379,
            db=0,
            decode_responses=True,
        )

    def add_location(self, name: str, longitude: float, latitude: float) -> None:
        """
        Add or update a named location.
        """
        self.r.geoadd(self.key, [longitude, latitude, name])

    def distance_between(
        self,
        name1: str,
        name2: str,
        unit: str = "km",
    ) -> Optional[float]:
        """
        Compute distance between two locations in the given unit.
        Units: 'm', 'km', 'mi', 'ft'
        Returns None if one of the locations does not exist.
        """
        dist = self.r.geodist(self.key, name1, name2, unit)
        return float(dist) if dist is not None else None

    def nearby(
        self,
        longitude: float,
        latitude: float,
        radius: float,
        unit: str = "km",
        withdist: bool = True,
        withcoord: bool = True,
    ) -> List[Dict]:
        """
        Find locations within a given radius from a point.
        Uses GEOSEARCH with FROMLONLAT.
        """
        # GEOSEARCH returns list of names or tuples depending on flags;
        # redis-py uses 'geosearch' wrapper method.
        results = self.r.geosearch(
            self.key,
            longitude=longitude,
            latitude=latitude,
            radius=radius,
            unit=unit,
            withdist=withdist,
            withcoord=withcoord,
        )

        # Results structure depends on redis-py version. For decode_responses=True,
        # each row is [name, dist, [lon, lat]] if withdist and withcoord are True.
        formatted_results: List[Dict] = []
        for item in results:
            if not isinstance(item, (list, tuple)):
                # Fallback if response includes only member names.
                formatted_results.append({"name": str(item)})
                continue

            record: Dict = {"name": str(item[0]) if item else ""}
            idx = 1

            if withdist and len(item) > idx:
                record["distance"] = float(item[idx])
                idx += 1

            if withcoord and len(item) > idx:
                coord = item[idx]
                if isinstance(coord, (list, tuple)) and len(coord) >= 2:
                    record["longitude"] = float(coord[0])
                    record["latitude"] = float(coord[1])

            formatted_results.append(record)

        return formatted_results


def demo():
    service = GeoSearchService("geo:stores:thimphu")

    # Clear old data
    service.r.delete(service.key)

    # Add some demo stores in Thimphu (coordinates are illustrative)
    service.add_location("store_norzin", 89.6390, 27.4728)
    service.add_location("store_changzamtog", 89.6530, 27.4712)
    service.add_location("store_motithang", 89.6490, 27.4770)

    # Distance example
    dist = service.distance_between("store_norzin", "store_changzamtog", unit="km")
    if dist is not None:
        print(f"Distance Norzin -> Changzamtog: {dist:.2f} km")
    else:
        print("Distance Norzin -> Changzamtog: unavailable")

    # Nearby example: user standing at Norzin
    print("\nStores within 3 km of Norzin:")
    nearby_stores = service.nearby(89.6390, 27.4728, radius=3, unit="km")
    for store in nearby_stores:
        if {"latitude", "longitude", "distance"}.issubset(store.keys()):
            print(
                f" {store['name']} at ({store['latitude']:.4f}, "
                f"{store['longitude']:.4f}) - {store['distance']:.2f} km"
            )
        else:
            print(f" {store['name']}")


if __name__ == "__main__":
    demo()
