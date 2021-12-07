using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Ecng.Collections;
using System.Runtime.Serialization.Formatters.Binary;
using System.Data.SQLite;

namespace MyData
{    
    public class TickData
    {
        // для разбора строки
        int INDEX_DATE = 0;
        int INDEX_TIME = 1;
        int INDEX_PRICE = 2;
        int INDEX_VOLUME = 3;
        int INDEX_ID = 4;

        public Tools.StringDelegate AddToLog;
        public DateTime LastDataUpdate;
        public uint StartTickId = 500000000;
        int TIMEFRAME = 60;

        SynchronizedLinkedList<Tick> Ticks; // двузсвязный список тиков
        //SynchronizedSet<uint> TicksIds; // id тиков
        Queue<List<Tick>> AddingTicks; // очередь на добавление тиков     
        Queue<Tick> AddingTicksSeparately; // очередь на добавление тиков     

        SynchronizedList<Tick> NextTicks;
        SynchronizedList<Tick> PrevTicks;

        public ulong FirstTickId = ulong.MaxValue, LastTickId = 0;
        public uint FirstTickTime = uint.MaxValue, LastTickTime = 0;
        public double LastTickPrice = 0;

        public long TickCount = 0;
        public long TickAddedRightCount = 0;
        public long TickAddedLeftCount = 0;
        public long TickAddedInsideCount = 0;
        public bool IsFilterSamePrices = true;
        public bool IsUseCache = true;
        public string CacheDir = "";
        bool IsProcessNewTicks = false;

        public long Bytes = 0;

        System.Threading.Timer Timer_1s;

        SynchronizedOrderedDictionary<uint, Bar> Bars_1M;

        TickStorage Storage;

        // методы
        public TickData(TickStorage storage)
        {
            //NextTicks = new SynchronizedList<Tick>(500000);
            //PrevTicks = new SynchronizedList<Tick>(5000000);

            Storage = storage;

           // Ticks = new SynchronizedLinkedList<Tick>();
            //TicksIds = new SynchronizedSet<uint>();
            AddingTicks = new Queue<List<Tick>>();
            AddingTicksSeparately = new Queue<Tick>();
            //Bars_1M = new SynchronizedOrderedDictionary<uint, Bar>(delegate(uint id1, uint id2) { return (id1 == id2) ? 0 : ((id1 > id2) ? 1 : -1); });
            Timer_1s = new System.Threading.Timer(Timer_Tick, null, TimeSpan.Zero, TimeSpan.FromMilliseconds(100));
        }

        public int[] GetLastTickData()
        {
            return Storage.GetLastTickData();
        }

        public List<Tick> GetTicks()
        {
            lock (Ticks.SyncRoot) return Ticks.ToList();                        
        }
        public List<Tick> GetTicks(uint start, uint end)
        {
            lock (Ticks.SyncRoot) return Ticks.Where(t => ((end >= t.Time)&&(t.Time >= start))).ToList();
        }
        public List<Tick> GetTicks(DateTime dtstart, DateTime dtend)
        {
            uint end = Tools.ConvertToUnixTimestamp(dtend);
            uint start = Tools.ConvertToUnixTimestamp(dtstart);
            lock (Ticks.SyncRoot) return Ticks.Where(t => ((end >= t.Time) && (t.Time >= start))).ToList();
        }
        public static List<Tick> FilterTicks(List<Tick> ticks, double minStep, bool isOnePerSec = true, bool addLast = false)
        {
            try
            {
                List<Tick> filtered = new List<Tick>();
                double lastprice = 0;
                uint lastutime = 0;

                for (int i = 0; i < ticks.Count; i++)
                {
                    Tick tick = ticks[i];

                    /*if (i>0 && tick.Time-ticks[i-1].Time >= 5 && tick.Time - ticks[i - 1].Time < 200)
                    {
                        Console.WriteLine("Дыра в тиках " + ticks[i - 1].Time + "-" + tick.Time);
                    }*/
                    
                    DateTime now = Tools.ConvertFromUnixTimestamp(tick.Time);
                    
                    if (isOnePerSec && lastutime == tick.Time) continue;
                 
                    if (Math.Abs(lastprice - tick.Price) >= minStep)
                    {
                        filtered.Add(tick);
                        lastprice = tick.Price;
                        lastutime = tick.Time;
                    }
                }

                if (addLast)
                {
                    Tick lastTick = ticks.Last<Tick>();
                    Tick lastFiltered = filtered.Last<Tick>();

                    if (lastTick.Price != lastFiltered.Price)
                    {
                        filtered.Add(lastTick);
                    }
                }
                return filtered;
            }
            catch (Exception ex)
            {
                //AddToLog("FilterTicks:" + ex.Message);                
                return null;
            }
        }

        public List<Bar> GetBars(DateTime start, DateTime end)
        {
            uint dt_start = Tools.ConvertToUnixTimestamp(start);
            uint dt_end = Tools.ConvertToUnixTimestamp(end);

            List<Tick> ticks = GetTicks(dt_start, dt_end);
            AddTicksToBars(ticks);

            List<Bar> bars1, bars2;
            bars2 = new List<Bar>(10000);
            lock (Bars_1M.SyncRoot) { bars1 = Bars_1M.Values.ToList(); };
            foreach (Bar bar in bars1)
            {
                if ((bar.Time >= dt_start) && (bar.Time <= dt_end)) bars2.Add(bar);
            }
            return bars2;
        }
        public List<Bar> GetBars()
        {
            List<Tick> ticks = GetTicks();
            AddTicksToBars(ticks);

            List<Bar> bars;
            lock (Bars_1M.SyncRoot) { bars = Bars_1M.Values.ToList(); };
            return bars;
        }

        // добавление тика
        public void AddTick(Tick tick)
        {
            List<Tick> tickList = new List<Tick>();
            tickList.Add(tick);
            AddingTicks.Enqueue(tickList);
            //AddingTicksSeparately.Enqueue(tick);
            //ProcessNewTicks();

        }
        public void AddTicks(List<Tick> ticks)
        {
            if (ticks.Count == 0) return;
            AddingTicks.Enqueue(ticks);
            //ProcessNewTicks();
        }

        private void Timer_Tick(Object state)
        {
            try
            {
                ProcessNewTicks();
            }
            catch (Exception e)
            {
                IsProcessNewTicks = false;

                //AddToLog(e.Message);
            }
        }
        void ProcessNewTicks()
        {
            if (AddingTicks.Count == 0)
            {
                return;
            }
            if (IsProcessNewTicks) return;
            
            IsProcessNewTicks = true;
            try
            {
                while (AddingTicks.Count > 0)
                {
                    List<Tick> listTicks = AddingTicks.Dequeue();

                    if (listTicks != null && listTicks.Count > 0)
                    {
                        using (SQLiteTransaction mytransaction = Storage._connection.BeginTransaction())
                        {
                            try
                            {
                                foreach (Tick t in listTicks)
                                {
                                    Storage.StoreTick(t);
                                }

                                mytransaction.Commit();
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("SQLite transaction commit: " + e.Message);
                                mytransaction.Commit();
                            }
                        }
                    }
                }
            } catch (Exception err)
            {
                Console.WriteLine("Proc err.: " + err.Message);
            }
            IsProcessNewTicks = false;
        }

       /* void ProcessNewTicks()
        {
            if (AddingTicksSeparately.Count== 0) return;
            if (IsProcessNewTicks) return;

            IsProcessNewTicks = true;
            List<Tick> tmpQueue = new List<Tick>();
            using (SQLiteTransaction mytransaction = Storage._connection.BeginTransaction())
            {
                while (AddingTicksSeparately.Count > 0)
                {
                    Tick t = AddingTicksSeparately.Dequeue();
                    Storage.StoreTick(t);
                }
                mytransaction.Commit();
            }

            IsProcessNewTicks = false;
        }*/


        void AddTicksToBars(List<Tick> ticks)
        {            
            foreach (Tick t in ticks)
            {
                uint timeindex = (uint)(Math.Floor((double)t.Time / TIMEFRAME) * TIMEFRAME);
                if (!Bars_1M.ContainsKey(timeindex))
                {
                    Bar bar = new Bar();
                    bar.Time = timeindex;
                    bar.OpenTickId = t.Id;
                    bar.CloseTickId = t.Id;
                    bar.Close = t.Price;
                    bar.Open = t.Price;
                    bar.High = t.Price;
                    bar.Low = t.Price;
                    //bar.Volume = t.Volume;
                    Bars_1M.Add(timeindex, bar);
                }
                else
                {
                    double price = t.Price;
                    Bar bar = Bars_1M[timeindex];
                    bar.High = (price > bar.High) ? price : bar.High;
                    bar.Low = (price < bar.Low) ? price : bar.Low;
                    if (t.Id > bar.CloseTickId)
                    {
                        bar.CloseTickId = t.Id;
                        bar.Close = price;
                    }
                    else if (t.Id < bar.OpenTickId)
                    {
                        bar.OpenTickId = t.Id;
                        bar.Open = price;
                    }
                    Bars_1M[timeindex] = bar;
                }
            }
        }
        void AddTickToBars(Tick t)
        {
                uint timeindex = (uint)(Math.Floor((double)t.Time / TIMEFRAME) * TIMEFRAME);
                if (!Bars_1M.ContainsKey(timeindex))
                {
                    Bar bar = new Bar();
                    bar.Time = timeindex;
                    bar.OpenTickId = t.Id;
                    bar.CloseTickId = t.Id;
                    bar.Close = t.Price;
                    bar.Open = t.Price;
                    bar.High = t.Price;
                    bar.Low = t.Price;
                    //bar.Volume = t.Volume;
                    Bars_1M.Add(timeindex, bar);
                }
                else
                {
                    double price = t.Price;
                    Bar bar = Bars_1M[timeindex];
                    bar.High = (price > bar.High) ? price : bar.High;
                    bar.Low = (price < bar.Low) ? price : bar.Low;
                    if (t.Id > bar.CloseTickId)
                    {
                        bar.CloseTickId = t.Id;
                        bar.Close = price;
                    }
                    else if (t.Id < bar.OpenTickId)
                    {
                        bar.OpenTickId = t.Id;
                        bar.Open = price;
                    }
                    Bars_1M[timeindex] = bar;
            }
        }

        public void LoadOneFile(string filename)
        {
            System.Threading.Thread thread = new System.Threading.Thread(unused => ThreadLoadOneFile(filename));
            thread.Start();
        }
        // чтение файла с тиками,  формат должен быть дд.мм.гг, чч:мм:cc, цена, объем, tickID
        private void ThreadLoadOneFile(string filename) 
        {
            List<Tick> tickList;
            if (IsUseCache) // попробуем прочитать из бинарного кэша
            {
                tickList = LoadBinCache(filename);
                if (tickList.Count > 0)
                {
                    //AddToLog("Используем кэш файла " + filename + ": загружено " + tickList.Count + " тиков.");
                    AddTicks(tickList);
                    return;
                }
            }

            string line;
            string[] strArr;
            char[] charArr = new char[] { ';' };
            int counter = 0;
            int year = 0, month = 0, day = 0, hour = 0, min = 0, sec = 0;
            float price = 0; int volume = 0; uint tickId = 0;
            FileStream fs;
            StreamReader sr = null;

            System.Globalization.NumberStyles numstyle = System.Globalization.NumberStyles.Float;
            System.Globalization.NumberFormatInfo nfi;
            nfi = new System.Globalization.NumberFormatInfo();
            nfi.NumberDecimalSeparator = ".";

            tickList = new List<Tick>(500000);

            //AddToLog("Читаем файл " + filename);

            try
            {
                fs = new FileStream(filename, FileMode.Open);
                sr = new StreamReader(fs);
                line = sr.ReadLine(); // первая строка не нужна
                while (sr.EndOfStream != true)  // framework 2.0
                {
                    line = sr.ReadLine();
                    strArr = line.Split(charArr);
                    if (strArr.Length != 5) continue; // формат отличается

                    bool checkOK = true;
                    // разбор даты и времени
                    string str_date = strArr[INDEX_DATE];
                    string str_time = strArr[INDEX_TIME];
                    if (str_date.Length != 8) continue; // если длина отличается от дд.мм.гг
                    if (str_time.Length != 8) continue; // если время отличается от чч:мм:cc
                    // дд.мм.гг
                    // 01234567
                    checkOK = checkOK && int.TryParse(str_date.Substring(6, 2), numstyle, nfi, out year);
                    checkOK = checkOK && int.TryParse(str_date.Substring(3, 2), numstyle, nfi, out month);
                    checkOK = checkOK && int.TryParse(str_date.Substring(0, 2), numstyle, nfi, out day);
                    // чч:мм:сс
                    // 01234567
                    checkOK = checkOK && int.TryParse(str_time.Substring(0, 2), numstyle, nfi, out hour);
                    checkOK = checkOK && int.TryParse(str_time.Substring(3, 2), numstyle, nfi, out min);
                    checkOK = checkOK && int.TryParse(str_time.Substring(6, 2), numstyle, nfi, out sec);

                    uint unixdatetime = Tools.ConvertToUnixTimestamp(new DateTime(year+2000, month, day, hour, min, sec));
                    if (unixdatetime > LastTickTime) LastTickTime = unixdatetime;
                    if (unixdatetime < FirstTickTime) FirstTickTime = unixdatetime;

                    checkOK = checkOK && float.TryParse(strArr[INDEX_PRICE], numstyle, nfi, out price);
                    checkOK = checkOK && int.TryParse(strArr[INDEX_VOLUME], numstyle, nfi, out volume);
                    checkOK = checkOK && uint.TryParse(strArr[INDEX_ID], numstyle, nfi, out tickId);
                    if (!checkOK) continue;

                    Tick tick = new Tick();
                    tick.Id = tickId - StartTickId;
                    tick.Price = price;
                    //tick.Volume = volume;
                    tick.Time = unixdatetime;
                    //AddTick(tick);
                    tickList.Add(tick);
                    counter++;
                } // while
                sr.Close();                
                //AddToLog("Загружено " + counter + " тиков.");
                tickList.Reverse();
                SaveBinCache(filename, tickList);
                AddTicks(tickList);
            }
            catch (Exception e)
            {
                //AddToLog("Проблема при загрузке файла: " + e.Message);
                if (sr != null) sr.Close();
            }
        }
        private void SaveBinCache(string fullFilename, List<Tick> ticks)
        {
            string filename = Path.GetFileName(fullFilename);
            filename = filename.Substring(0, filename.Length - 4) + ".bin";

            FileStream fs = new FileStream(CacheDir + filename, FileMode.Create);
            BinaryWriter bw = new BinaryWriter(fs);

            int count = ticks.Count;
            bw.Write(count);
            for (int i = 0; i < count; i++)
            {
                Tick t = ticks[i];
                bw.Write(t.Id);
                bw.Write(t.Time);
                bw.Write(t.Price);                
            }
            bw.Flush();
            bw.Close();
        }
        private List<Tick> LoadBinCache(string fullFilename)
        {
            List<Tick> ticks = new List<Tick>(500000);
            try
            {
                string filename = Path.GetFileName(fullFilename);
                filename = filename.Substring(0, filename.Length - 4) + ".bin";

                using (FileStream fs = new FileStream(CacheDir + filename, FileMode.Open))
                {
                    BinaryReader br = new BinaryReader(fs);
                    int count = br.ReadInt32();
                    for (int i = 0; i < count; i++)
                    {
                        Tick t = new Tick();
                        t.Id = br.ReadUInt32();
                        t.Time = br.ReadUInt32();
                        t.Price = br.ReadSingle();
                        ticks.Add(t);
                    }
                    fs.Close();
                }
            }
            catch (Exception e)
            {
               // AddToLog("LoadBinCache: " + e.Message);
            }
            return ticks;
        }

        public void LoadFilesFromDir(string dirpath)
        {
            System.Threading.Thread thread = new System.Threading.Thread(unused => ThreadLoadFilesFromDir(dirpath));
            thread.Start();
        }
        public void ThreadLoadFilesFromDir(string dirpath)
        {
            string[] filePaths = Directory.GetFiles(dirpath, "*.*");
            filePaths = filePaths.Reverse().ToArray();
            foreach (string filename in filePaths)
            {
                ThreadLoadOneFile(filename);
            }

            
        }
    }
}
