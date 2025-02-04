using System;
using System.Collections.Generic;

namespace Common.Wang
{
    public class WangBlobTile : WangTile
    {
        public static readonly WangBlobTile Null = new WangBlobTile(-1, true);

        private static readonly IDictionary<int, Tuple<int, int>> ReverseIndexMap = new Dictionary<int, Tuple<int, int>>();

        static WangBlobTile()
        {
            IDictionary<int, List<int>> indexMap = new Dictionary<int, List<int>>
            {
                {0, new List<int>()},
                {1, new List<int> {4, 16, 64}},
                {5, new List<int> {20, 80, 65}},
                {7, new List<int> {28, 112, 193}},
                {17, new List<int> {68}},
                {21, new List<int> {84, 81, 69}},
                {23, new List<int> {92, 113, 197}},
                {29, new List<int> {116, 209, 71}},
                {31, new List<int> {124, 241, 199}},
                {85, new List<int>()},
                {87, new List<int> {93, 117, 213}},
                {95, new List<int> {125, 245, 215}},
                {119, new List<int> {221}},
                {127, new List<int> {253, 247, 223}},
                {255, new List<int>()},
            };
            
            foreach (var kvp in indexMap)
            {
                
                var rootIndex = kvp.Key;
                
                ReverseIndexMap.Add(rootIndex, new Tuple<int, int>(rootIndex, 0));
                
                for (var i = 0; i < kvp.Value.Count; i++)
                {
                    var childIndex = kvp.Value[i];
                    ReverseIndexMap.Add(childIndex, new Tuple<int, int>(rootIndex, i + 1));
                }
            }
        }

        public WangBlobTile() : this(0, false)
        {

        }

        public WangBlobTile(int index, bool readOnly)
        {
            Index = index;
            ReadOnly = readOnly;
        }

        public bool IsNull => Index == -1;
        
        public int Index { get; private set; }

        public int RootIndex
        {
            get
            {
                var p = ReverseIndexMap[Index];
                return p.Item1;
            }
        }
        public int Rotation 
        {
            get
            {
                var p = ReverseIndexMap[Index];
                return p.Item2;
            }}
        public bool ReadOnly { get; }
        
        public bool NorthWest
        {
            get => HasFlag(128);
            set => SetFlag(128, value);
        }

        public bool North
        {
            get => HasFlag(1);
            set => SetFlag(1, value);
        }

        public bool NorthEast
        {
            get => HasFlag(2);
            set => SetFlag(2, value);
        }

        public bool East
        {
            get => HasFlag(4);
            set => SetFlag(4, value);
        }

        public bool SouthEast
        {
            get => HasFlag(8);
            set => SetFlag(8, value);
        }

        public bool South
        {
            get => HasFlag(16);
            set => SetFlag(16, value);
        }

        public bool SouthWest
        {
            get => HasFlag(32);
            set => SetFlag(32, value);
        }

        public bool West
        {
            get => HasFlag(64);
            set => SetFlag(64, value);
        }

        private bool HasFlag(int value)
        {
            if (IsNull)
            {
                return false;
            }

            return (Index & value) == value;
        }

        private void SetFlag(int value, bool set)
        {
            if (IsNull || ReadOnly)
            {
                throw new InvalidOperationException("Attempt to modify a null or readonly tile");
            }

            if (set)
            {
                Index |= value;
            }
            else
            {
                Index ^= value;
            }
        }

        public override string ToString()
        {
            return string.Format("Index: {0} IsNull: {1} ReadOnly: {2}", Index, IsNull, ReadOnly);
        }
    }
}