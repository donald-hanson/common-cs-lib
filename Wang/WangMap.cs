using System;

namespace Common.Wang
{
    public interface IWangMap<out T>
    {
        T GetTileAt(int x, int y);
        public void Generate();
    }

    public abstract class WangMap<T> : IWangMap<T> where T : WangTile
    {
        private readonly int _width;
        private readonly int _height;
        private readonly Random _random;
        private readonly T[] _tiles;

        public int Width => _width;
        public int Height => _height;

        protected WangMap(int width, int height, int seed)
        {
            _width = width;
            _height = height;
            _random = new Random(seed);
            _tiles = new T[_width * _height];
        }

        public T GetTileAt(int x, int y)
        {
            if (x < 0 || y < 0 || x > _width - 1 || y > _height - 1)
            {
                return InvalidTile();
            }

            var offset = GetTileOffset(x, y);
            var tile = _tiles[offset];
            if (tile == null)
            {
                tile = CreateTile(x, y);
                _tiles[offset] = tile;
            }

            return tile;
        }
        
        protected void ReplaceTileAt(Coordinate position, T tile)
        {
            var x = position.X;
            var y = position.Y;

            if (x < 0 || y < 0 || x > _width - 1 || y > _height - 1)
            {
                return;
            }

            var offset = GetTileOffset(x, y);
            _tiles[offset] = tile;
        }

        protected (T, Coordinate, WangDirection) GetNeighborTile(Coordinate position, WangDirection direction)
        {
            var neighbor = GetNeighborCoordinate(position, direction);
            return (GetTileAt(neighbor.X, neighbor.Y), neighbor, direction);
        }

        protected Coordinate GetNeighborCoordinate(Coordinate position, WangDirection direction)
        {
            int x = position.X;
            int y = position.Y;

            return direction switch
            {
                WangDirection.North => new Coordinate(x, y - 1),
                WangDirection.NorthEast => new Coordinate(x + 1, y - 1),
                WangDirection.East => new Coordinate(x + 1, y),
                WangDirection.SouthEast => new Coordinate(x + 1, y + 1),
                WangDirection.South => new Coordinate(x, y + 1),
                WangDirection.SouthWest => new Coordinate(x - 1, y + 1),
                WangDirection.West => new Coordinate(x - 1, y),
                WangDirection.NorthWest => new Coordinate(x - 1, y - 1),
                _ => throw new ArgumentOutOfRangeException(nameof(direction), direction, null)
            };
        }

        private int GetTileOffset(int x, int y)
        {
            return x + y * _width;
        }

        private T CreateTile(int x, int y)
        {
            return CreateTile(new Coordinate(x, y));
        }

        protected int GetRandomNext(int maxValue)
        {
            return _random.Next(maxValue);
        }

        protected abstract T InvalidTile();

        protected abstract T CreateTile(Coordinate position);

        public abstract void Generate();
    }
}