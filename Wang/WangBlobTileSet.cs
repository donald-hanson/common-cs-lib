using System.Collections.Generic;

namespace Common.Wang
{
    public static class WangBlobTileSet
    {
        private static readonly Dictionary<int, WangBlobTile> _tilesByIndex = new Dictionary<int, WangBlobTile>();

        static WangBlobTileSet()
        {
            var indexes = new[]
            {
                0,
                1, 4, 16, 64,
                5, 20, 80, 65,
                7, 28, 112, 193,
                17, 68,
                21, 84, 81, 69,
                23, 92, 113, 197,
                29, 116, 209, 71,
                31, 124, 241, 199,
                85,
                87, 93, 117, 213,
                95, 125, 245, 215,
                119, 221,
                127, 253, 247, 223,
                255
            };

            foreach (var index in indexes)
            {
                var tile = new WangBlobTile(index, true);
                _tilesByIndex.Add(index, tile);
            }
        }

        public static IReadOnlyList<WangBlobTile> GetPossibleMatches(WangBlobTile north, WangBlobTile south, WangBlobTile east, WangBlobTile west, in Coordinate position, Size size)
        {
            List<WangBlobTile> result = new List<WangBlobTile>();

            foreach (var tile in _tilesByIndex.Values)
            {
                if (!MatchesCoordinates(tile, position, size))
                    continue;

                if (!MatchTile(north, tile, WangDirection.North) ||
                    !MatchTile(east, tile, WangDirection.East) ||
                    !MatchTile(south, tile, WangDirection.South) ||
                    !MatchTile(west, tile, WangDirection.West))
                    continue;
                result.Add(tile);
            }

            return result;
        }

        private static bool MatchesCoordinates(in WangBlobTile newTile, in Coordinate position, in Size size)
        {
            var x = position.X;
            var y = position.Y;
            var w = size.Width;
            var h = size.Height;

            if (x == 0)
            {
                if (newTile.NorthWest || newTile.West || newTile.SouthWest)
                {
                    return false;
                }
            }
            if (x == w - 1)
            {
                if (newTile.NorthEast || newTile.East || newTile.SouthEast)
                {
                    return false;
                }
            }
            if (y == 0)
            {
                if (newTile.NorthWest || newTile.North || newTile.NorthEast)
                {
                    return false;
                }
            }
            if (y == h - 1)
            {
                if (newTile.SouthWest || newTile.South || newTile.SouthEast)
                {
                    return false;
                }
            }

            return true;
        }

        private static bool MatchTile(WangBlobTile current, WangBlobTile next, WangDirection direction)
        {
            if (current.IsNull)
            {
                return true;
            }

            if (direction == WangDirection.North)
            {
                if (current.SouthWest != next.NorthWest)
                {
                    return false;
                }

                if (current.South != next.North)
                {
                    return false;
                }
                
                if (current.SouthEast != next.NorthEast)
                {
                    return false;
                }
            }
            else if (direction == WangDirection.East)
            {
                if (current.NorthWest != next.NorthEast)
                {
                    return false;
                }

                if (current.West != next.East)
                {
                    return false;
                }

                if (current.SouthWest != next.SouthEast)
                {
                    return false;
                }
            }
            else if (direction == WangDirection.South)
            {
                if (current.NorthWest != next.SouthWest)
                {
                    return false;
                }

                if (current.North != next.South)
                {
                    return false;
                }

                if (current.NorthEast != next.SouthEast)
                {
                    return false;
                }
            }
            else if (direction == WangDirection.West)
            {
                if (current.NorthEast != next.NorthWest)
                {
                    return false;
                }

                if (current.East != next.West)
                {
                    return false;
                }

                if (current.SouthEast != next.SouthWest)
                {
                    return false;
                }
            }

            return true;
        }
    }
}