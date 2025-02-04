namespace Common.Wang
{
    public class Size
    {
        public readonly int Width;
        public readonly int Height;

        public Size(int width, int height)
        {
            Width = width;
            Height = height;
        }

        public override string ToString()
        {
            return string.Format("Width: {0} Height: {1}", Width, Height);
        }
    }
}