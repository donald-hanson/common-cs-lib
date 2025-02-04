namespace Common.Wang
{
    public class WangBlobMap : WangMap<WangBlobTile>
    {
        public WangBlobMap(int width, int height, int seed) : base(width, height, seed)
        {
        }

        protected override WangBlobTile InvalidTile()
        {
            return WangBlobTile.Null;
        }

        protected override WangBlobTile CreateTile(Coordinate position)
        {
            return WangBlobTile.Null;
        }

        public override void Generate()
        {
            for (var x = 0; x < Width; x++)
            {
                for (var y = 0; y < Height; y++)
                {
                    PlaceTile(new Coordinate(x, y));
                }
            }
        }

        private void PlaceTile(in Coordinate position)
        {
            var (west, _, _) = GetNeighborTile(position, WangDirection.West);
            var (east, _, _) = GetNeighborTile(position, WangDirection.East);
            var (north, _, _) = GetNeighborTile(position, WangDirection.North);
            var (south, _, _) = GetNeighborTile(position, WangDirection.South);

            var possibleTiles = WangBlobTileSet.GetPossibleMatches(north, south, east, west, position, new Size(Width, Height));

            if (possibleTiles.Count > 0)
            {
                var index = GetRandomNext(possibleTiles.Count);
                var nextTile = possibleTiles[index];
                ReplaceTileAt(position, nextTile);
            }
        }
    }
}