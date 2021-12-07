using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Ecng.Collections;
using System.Globalization;

namespace MyData
{
    public static class Tools
    {
        public delegate void StringDelegate(string text);

        //  int unixTime = (int)(DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalSeconds;
        //  DateTime pDate = (new DateTime(1970, 1, 1, 0, 0, 0, 0)).AddSeconds(timestamp);

        public static DateTime ConvertFromUnixTimestamp(uint timestamp)
        {
            //DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0);
            //return origin.AddSeconds(timestamp);
            return (new DateTime(1970, 1, 1, 0, 0, 0, 0)).AddSeconds(timestamp);
        }

        public static uint ConvertToUnixTimestamp(DateTime date)
        {
            //DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0);
            //TimeSpan diff = date - origin;
            return (uint)Math.Floor((date - (new DateTime(1970, 1, 1, 0, 0, 0, 0))).TotalSeconds);
        }

        // The unsafe keyword allows pointers to be used within the following method:
        public static unsafe void Copy(byte[] src, int srcIndex, byte[] dst, int dstIndex, int count)
        {
            if (src == null || srcIndex < 0 ||
                dst == null || dstIndex < 0 || count < 0)
            {
                throw new System.ArgumentException();
            }

            int srcLen = src.Length;
            int dstLen = dst.Length;
            if (srcLen - srcIndex < count || dstLen - dstIndex < count)
            {
                throw new System.ArgumentException();
            }

            // The following fixed statement pins the location of the src and dst objects
            // in memory so that they will not be moved by garbage collection.
            fixed (byte* pSrc = src, pDst = dst)
            {
                byte* ps = pSrc;
                byte* pd = pDst;

                // Loop over the count in blocks of 4 bytes, copying an integer (4 bytes) at a time:
                for (int i = 0; i < count / 4; i++)
                {
                    *((int*)pd) = *((int*)ps);
                    pd += 4;
                    ps += 4;
                }

                // Complete the copy by moving any bytes that weren't moved in blocks of 4:
                for (int i = 0; i < count % 4; i++)
                {
                    *pd = *ps;
                    pd++;
                    ps++;
                }
            }
        }

        public static object DeepClone(object obj)
        {
            object objResult = null;
            using (MemoryStream ms = new MemoryStream())
            {
                BinaryFormatter bf = new BinaryFormatter();
                bf.Serialize(ms, obj);

                ms.Position = 0;
                objResult = bf.Deserialize(ms);
            }
            return objResult;
        }

        public static Dictionary<uint, double> LoadFromBinaryFile(string filename)
        {
            Dictionary<uint, double> data = null;
            try
            {                
                using (FileStream fs = new FileStream(filename, FileMode.Open))
                {
                    BinaryReader br = new BinaryReader(fs);
                    int FILETYPE = br.ReadInt32();
                    data = new Dictionary<uint, double>();
                    int count = br.ReadInt32();
                    for (int i = 0; i < count; i++)
                    {
                        uint key = br.ReadUInt32();
                        double value = br.ReadDouble();
                        data.Add(key, value);
                    }
                    br.Close();
                    return data;
                }
            }
            catch (Exception e)
            {
                return data;
            }
        }        
        public static bool SaveToBinaryFile(string filename, Dictionary<int, double> data)
        {
            try
            {
                int FILETYPE = 1;
                using (FileStream fs = new FileStream(filename, FileMode.Create))
                {
                    BinaryWriter bw = new BinaryWriter(fs);
                    bw.Write(FILETYPE);
                    bw.Write(data.Count);
                    foreach(KeyValuePair<int,double> kvp in data)
                    {
                        bw.Write(kvp.Key);
                        bw.Write(kvp.Value);
                    }
                    bw.Flush();
                    bw.Close();
                    return true;
                }
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public static Dictionary<uint, double> LoadTrajectoryFromCsv(StreamReader reader)
        {
            Dictionary<uint, double> data = new Dictionary<uint, double>();
            uint idx;
            decimal rsum;
            double rsumDouble;
            while (!reader.EndOfStream)
            {
                string line = reader.ReadLine();
                if (!String.IsNullOrWhiteSpace(line))
                {
                    string[] values = line.Split(';');
                    idx = Convert.ToUInt32(values[0]);
                    rsum = decimal.Parse(values[1], NumberStyles.Float, new CultureInfo("en-US"));
                    rsumDouble = (double)rsum;
                    data.Add(idx, rsumDouble);
                }
            }
            return data;
        }
        public static bool SaveToTextFile(string filename, Dictionary<int, double> data)
        {
            try
            {
                using (FileStream fs = new FileStream(filename, FileMode.Create))
                {
                    StreamWriter sw = new StreamWriter(fs);
                    foreach (KeyValuePair<int, double> kvp in data)
                    {
                        sw.WriteLine(kvp.Key.ToString() + ";" + kvp.Value.ToString());
                    }
                    sw.Flush();
                    sw.Close();
                    return true;
                }
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public class Position
        {
            public double Amount { get; private set; }
            public double AveragePrice { get; private set; }
            public double RPL { get; private set; }

            public Position()
            {
                RPL = 0;
                Amount = 0;
                AveragePrice = 0;
            }

            public void AddTrade(double size, double price)
            {
                double[] info = CalculatePosition(Amount, AveragePrice, size, price);
                Amount = info[0];
                AveragePrice = info[1];
                RPL += info[2];
            }

            public double CloseAndGetPL(double price)
            {
                AddTrade(-Amount, price);
                return RPL;
            }

            public double FPL(double price)
            {
                double[] info = CalculatePosition(Amount, AveragePrice, -Amount, price);
                return info[2];
            }

            // возвращает [ новыйразмерпозиции, новаяучценапозиции, реализованныйПУ ]
            private double[] CalculatePosition(double amount, double amountprice, double delta, double deltaprice)
            {
                if (amount == 0) return new double[] { delta, deltaprice, 0 }; // если начальная позиция была 0
                if (delta == 0) return new double[] { amount, amountprice, 0 }; // дельта = 0, wtf? на всякий
                var newamount = amount + delta; // новая позиция
                if (((amount > 0) && (delta > 0)) || ((amount < 0) && (delta < 0)))
                { // позиция увеличилась
                    var newprice = (1.0 * amountprice * amount + 1.0 * deltaprice * delta) / newamount;
                    return new double[] { newamount, newprice, 0 };
                }
                if (newamount == 0)
                { // закрылись в 0
                    var deltaPL = -delta * (deltaprice - amountprice);
                    return new double[] { newamount, 0, deltaPL };
                }
                if (Math.Abs(delta) < Math.Abs(amount))
                {  // позиция уменьшилась, но не переворачивалась
                    var deltaPL = -delta * (deltaprice - amountprice);
                    return new double[] { newamount, amountprice, deltaPL };
                }
                if (Math.Abs(delta) > Math.Abs(amount))
                {  // позиция перевернулась
                    var deltaPL = amount * (deltaprice - amountprice);
                    return new double[] { newamount, deltaprice, deltaPL };
                }
                return new double[] { 0, 0, 0 };
            } // function CalculatePosition
        }

    }

}
