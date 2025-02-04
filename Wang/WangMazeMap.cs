using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Common.Wang
{
    public class WangMazeMap : WangMap<WangBlobTile>
    {
        private readonly int _randomThreshold;
        private readonly bool _generateRooms;

        public WangMazeMap(int width, int height, int seed, int randomThreshold = 10, bool generateRooms = false) : base(width, height, seed)
        {
            _randomThreshold = randomThreshold;
            _generateRooms = generateRooms;
        }

        protected override WangBlobTile InvalidTile()
        {
            return WangBlobTile.Null;
        }

        protected override WangBlobTile CreateTile(Coordinate position)
        {
            return new WangBlobTile(0, false);
        }

        public override void Generate()
        {
            List<Coordinate> stack = new List<Coordinate>();

            var x = GetRandomNext(Width);
            var y = GetRandomNext(Height);

            Coordinate start = new Coordinate(x, y);
            stack.Add(start);

            Generate(stack);
        }

        private int SelectNextIndex(ICollection visited)
        {
            if (GetRandomNext(100) < _randomThreshold)
            {
                return GetRandomNext(visited.Count);
            }

            return visited.Count - 1;
        }

        private void Generate(List<Coordinate> cells)
        {
            while (true)
            {
                var startIndex = SelectNextIndex(cells);
                var startPosition = cells[startIndex];
                var start = GetTileAt(startPosition.X, startPosition.Y);

                var north = GetNeighborTile(startPosition, WangDirection.North);
                var south = GetNeighborTile(startPosition, WangDirection.South);
                var east = GetNeighborTile(startPosition, WangDirection.East);
                var west = GetNeighborTile(startPosition, WangDirection.West);

                var neighbors = new[] {north, south, east, west};
                neighbors = neighbors.Where(t => !t.Item1.IsNull && t.Item1.Index == 0).ToArray();

                if (neighbors.Length == 0)
                {
                    cells.RemoveAt(startIndex);
                    if (cells.Count > 0)
                        continue;
                    return;
                }

                var neighborIndex = GetRandomNext(neighbors.Length);
                var (neighbor, position, direction) = neighbors[neighborIndex];

                switch (direction)
                {
                    case WangDirection.North:
                        start.North = true;
                        neighbor.South = true;
                        ApplySouthEastCornerRule(position, neighbor);
                        ApplySouthWestCornerRule(position, neighbor);
                        break;
                    case WangDirection.East:
                        start.East = true;
                        neighbor.West = true;
                        ApplyNorthWestCornerRule(position, neighbor);
                        ApplySouthWestCornerRule(position, neighbor);
                        break;
                    case WangDirection.South:
                        start.South = true;
                        neighbor.North = true;
                        ApplyNorthEastCornerRule(position, neighbor);
                        ApplyNorthWestCornerRule(position, neighbor);
                        break;
                    case WangDirection.West:
                        start.West = true;
                        neighbor.East = true;
                        ApplyNorthEastCornerRule(position, neighbor);
                        ApplySouthEastCornerRule(position, neighbor);
                        break;
                }

                cells.Add(position);
            }
        }

        private WangBlobTile EnsureNotNullTile(WangBlobTile input, Coordinate pos)
        {
            if (input.IsNull || input.ReadOnly)
            {
                int index = input.Index;
                if (index < 0)
                {
                    index = 0;
                }
                var tile = new WangBlobTile(index, false);
                ReplaceTileAt(pos, tile);
                return tile;
            }

            return input;
        }

        private void ApplySouthWestCornerRule(Coordinate neighborPosition, WangBlobTile neighbor)
        {
            if (!_generateRooms)
            {
                return;
            }

            var (west, westPos, _) = GetNeighborTile(neighborPosition, WangDirection.West);
            var (southWest, southWestPos, _) = GetNeighborTile(neighborPosition, WangDirection.SouthWest);
            var (south, southPos, _) = GetNeighborTile(neighborPosition, WangDirection.South);

            var edges = 0;
            if (west.East || neighbor.West)
            {
                edges++;
            }

            if (west.South || southWest.North)
            {
                edges++;
            }

            if (southWest.East || south.West)
            {
                edges++;
            }

            if (south.North || neighbor.South)
            {
                edges++;
            }

            if (edges >= 3)
            {
                west = EnsureNotNullTile(west, westPos);
                southWest = EnsureNotNullTile(southWest, southWestPos);
                south = EnsureNotNullTile(south, southPos);

                neighbor.West = true;
                neighbor.SouthWest = true;
                neighbor.South = true;

                west.East = true;
                west.SouthEast = true;
                west.South = true;

                southWest.North = true;
                southWest.NorthEast = true;
                southWest.East = true;

                south.NorthWest = true;
                south.West = true;
                south.North = true;
            }
        }
        
        private void ApplySouthEastCornerRule(Coordinate neighborPosition, WangBlobTile neighbor)
        {
            if (!_generateRooms)
            {
                return;
            }

            var (east, eastPos, _) = GetNeighborTile(neighborPosition, WangDirection.East);
            var (southEast, southEastPos, _) = GetNeighborTile(neighborPosition, WangDirection.SouthEast);
            var (south, southPos, _) = GetNeighborTile(neighborPosition, WangDirection.South);

            var edges = 0;
            if (east.West || neighbor.East)
            {
                edges++;
            }

            if (east.South || southEast.North)
            {
                edges++;
            }

            if (southEast.West || south.East)
            {
                edges++;
            }

            if (south.North || neighbor.South)
            {
                edges++;
            }

            if (edges >= 3)
            {
                east = EnsureNotNullTile(east, eastPos);
                southEast = EnsureNotNullTile(southEast, southEastPos);
                south = EnsureNotNullTile(south, southPos);

                neighbor.East = true;
                neighbor.SouthEast = true;
                neighbor.South = true;

                east.West = true;
                east.SouthWest = true;
                east.South = true;

                southEast.North = true;
                southEast.NorthWest = true;
                southEast.West = true;

                south.North = true;
                south.NorthEast = true;
                south.East = true;
            }
        }

        private void ApplyNorthEastCornerRule(Coordinate neighborPosition, WangBlobTile neighbor)
        {
            if (!_generateRooms)
            {
                return;
            }

            var (east, eastPos, _) = GetNeighborTile(neighborPosition, WangDirection.East);
            var (northEast, northEastPos, _) = GetNeighborTile(neighborPosition, WangDirection.NorthEast);
            var (north, northPos, _) = GetNeighborTile(neighborPosition, WangDirection.North);

            var edges = 0;
            if (east.West || neighbor.East)
            {
                edges++;
            }

            if (east.North || northEast.South)
            {
                edges++;
            }

            if (northEast.West || north.East)
            {
                edges++;
            }

            if (north.South || neighbor.North)
            {
                edges++;
            }

            if (edges >= 3)
            {
                east = EnsureNotNullTile(east, eastPos);
                northEast = EnsureNotNullTile(northEast, northEastPos);
                north = EnsureNotNullTile(north, northPos);

                neighbor.NorthEast = true;
                neighbor.East = true;
                neighbor.North = true;

                east.West = true;
                east.NorthWest = true;
                east.North = true;

                northEast.South = true;
                northEast.SouthWest = true;
                northEast.West = true;

                north.East = true;
                north.SouthEast = true;
                north.South = true;
            }
        }

        private void ApplyNorthWestCornerRule(Coordinate neighborPosition, WangBlobTile neighbor)
        {
            if (!_generateRooms)
            {
                return;
            }

            var (west, westPos, _) = GetNeighborTile(neighborPosition, WangDirection.West);
            var (northWest, northWestPos, _) = GetNeighborTile(neighborPosition, WangDirection.NorthWest);
            var (north, northPos, _) = GetNeighborTile(neighborPosition, WangDirection.North);

            var edges = 0;
            if (west.East || neighbor.West)
            {
                edges++;
            }

            if (west.North || northWest.South)
            {
                edges++;
            }

            if (northWest.East || north.West)
            {
                edges++;
            }

            if (north.South || neighbor.North)
            {
                edges++;
            }

            if (edges >= 3)
            {
                west = EnsureNotNullTile(west, westPos);
                northWest = EnsureNotNullTile(northWest, northWestPos);
                north = EnsureNotNullTile(north, northPos);

                neighbor.West = true;
                neighbor.NorthWest = true;
                neighbor.North = true;

                west.East = true;
                west.NorthEast = true;
                west.North = true;

                northWest.South = true;
                northWest.SouthEast = true;
                northWest.East = true;

                north.West = true;
                north.SouthWest = true;
                north.South = true;
            }
        }

    }
}
