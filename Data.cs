using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MyData
{
    public struct Tick
    {
        public ulong Id;
        public string Code;
        public uint Time;
        public double Price;
    }
    public struct Tick2
    {
        public ulong Id;
        public uint Time;
        public double Price;
        public int Volume;
    }
    public struct Bar
    {
        public uint Time;
        public double Open;
        public double High;
        public double Low;
        public double Close;
        public int Volume;
        public uint BuyVolume;
        public uint SellVolume;
        public ulong OpenTickId;
        public ulong CloseTickId;
    }
    public struct Bar2
    {
        public string Code;
        public uint Time;
        public double Open;
        public double High;
        public double Low;
        public double Close;
        public int Volume;
    }
}
