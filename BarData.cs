using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Ecng.Collections;
using System.IO;

namespace MyData
{
    public class BarData
    {
        // для разбора строки
        int INDEX_DATE = 0;
        int INDEX_TIME = 1;
        int INDEX_OPEN = 2;
        int INDEX_HIGH = 3;
        int INDEX_LOW  = 4;
        int INDEX_CLOSE = 5;
        int INDEX_VOLUME = 6;

        public Tools.StringDelegate AddToLog;

        public Dictionary<string, SortedDictionary<uint, Bar>> Storage;

        public BarData()
        {
            Storage = new Dictionary<string, SortedDictionary<uint, Bar>>();
        }

        public void LoadOneFile(string ticker, string filename)
        {            
            System.Threading.Thread thread = new System.Threading.Thread(unused => ThreadLoadOneFile(ticker, filename));
            thread.Start();
        }
        public void ThreadLoadOneFile(string ticker, string filename)
        {
            if (!(Storage.ContainsKey(ticker))) Storage.Add(ticker, new SortedDictionary<uint, Bar>());

            int counter = 0; int linescounter = 0;
            char[] charArr = new char[] { ';' };
            FileStream fs;
            StreamReader sr = null;
            int year = 0, month = 0, day = 0, hour = 0, min = 0, sec = 0;

            System.Globalization.NumberStyles numstyle = System.Globalization.NumberStyles.Float;
            System.Globalization.NumberFormatInfo nfi;
            nfi = new System.Globalization.NumberFormatInfo();
            nfi.NumberDecimalSeparator = ".";

            AddToLog("Читаем данные " + ticker + " из файла " + filename);

            try
            {
                fs = new FileStream(filename, FileMode.Open);
                sr = new StreamReader(fs);
                string line = sr.ReadLine(); // первая строка не нужна
                while (sr.EndOfStream != true)  // framework 2.0
                {
                    line = sr.ReadLine();
                    linescounter++;
                    string[] strArr = line.Split(charArr);
                    if (strArr.Length != 7) continue;

                    //начали разбор
                    Bar bar = new Bar();
                    bool checkOK = true;
                    checkOK = checkOK && double.TryParse(strArr[INDEX_OPEN], numstyle, nfi, out bar.Open);
                    checkOK = checkOK && double.TryParse(strArr[INDEX_HIGH], numstyle, nfi, out bar.High);
                    checkOK = checkOK && double.TryParse(strArr[INDEX_LOW], numstyle, nfi, out bar.Low);
                    checkOK = checkOK && double.TryParse(strArr[INDEX_CLOSE], numstyle, nfi, out bar.Close);
                    //checkOK = checkOK && int.TryParse(strArr[INDEX_VOLUME], numstyle, nfi, out bar.Volume);
                    if (!checkOK) continue;
                    
                    // разбор даты и времени
                    string str_date = strArr[INDEX_DATE];
                    string str_time = strArr[INDEX_TIME];
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
                    if (!checkOK) continue;
                    

                    DateTime dt = new DateTime(year+2000, month, day, hour, min, sec);
                    uint unixdatetime = Tools.ConvertToUnixTimestamp(dt);
                    bar.Time = unixdatetime;

                    if (!(Storage[ticker].ContainsKey(unixdatetime)))
                        { Storage[ticker].Add(unixdatetime, bar); } else { Storage[ticker][unixdatetime] = bar; }

                    counter++;
                } // while
                sr.Close();
                AddToLog("Загружено " + counter + " баров.");
            }
            catch (Exception e)
            {
                AddToLog("Проблема при загрузке файла: " + e.Message + " line=" + linescounter);
                if (sr != null) sr.Close();
            }

            /*
            // проверка
            foreach (KeyValuePair<string, SortedDictionary<uint, Bar>> kvp in Storage)
            {
                AddToLog(kvp.Key + ":" + kvp.Value.Count);
                foreach (KeyValuePair<uint, Bar> kvp2 in kvp.Value)
                {
                    DateTime dt = Tools.ConvertFromUnixTimestamp(kvp2.Key);
                    AddToLog(dt.ToString("yyyyMMdd HHmmss")+"; " + kvp2.Value.Open+"; " + kvp2.Value.High+"; " + kvp2.Value.Low+"; " + kvp2.Value.Close+"; " + kvp2.Value.Volume);
                }
            }
             */
        }

        public void LoadAllFiles(string dirname)
        {
            System.Threading.Thread thread = new System.Threading.Thread(unused => ThreadLoadAllFiles(dirname));
            thread.Start();
        }
        public void ThreadLoadAllFiles(string dirname)
        {
            try
            {
                string[] filePaths = Directory.GetFiles(dirname, "*.*");
                foreach (string filename in filePaths)
                {
                    string ticker = filename.Substring(0, filename.IndexOf("_"));
                    ticker = ticker.Substring(ticker.LastIndexOf("//"));
                    ThreadLoadOneFile(ticker, filename);
                }
            }
            catch (Exception e) { AddToLog("ThreadLoadAllFiles: " + e.Message); }
        }

        public SortedDictionary<uint, List<Bar2>> GetMergedData()
        {
            SortedDictionary<uint, List<Bar2>> data = new SortedDictionary<uint, List<Bar2>>();
            List<string> tickers = Storage.Keys.ToList();
            int tickercount = tickers.Count;

            for (int i = 0; i < tickercount; i++)
            {
                SortedDictionary<uint, Bar> bars = Storage[tickers[i]];
                foreach (KeyValuePair<uint, Bar> kvp in bars) // сливаем бары
                {
                    uint time = kvp.Key;
                    Bar oldbar = kvp.Value;
                    Bar2 b = new Bar2();
                    b.Code = tickers[i];
                    b.Time = time;
                    b.Close = oldbar.Close;
                    b.Open = oldbar.Open;
                    b.High = oldbar.High;
                    b.Low = oldbar.Low;
                    b.Volume = oldbar.Volume;

                    if (!data.ContainsKey(time)) data.Add(time, new List<Bar2>());
                    data[time].Add(b);
                }
            } // for

            // тепень нужно выбросить точки, где количество баров != кол-во тикеров, т.к. есть пропуски
            List<uint> datetimes = data.Keys.ToList();
            foreach (uint datetime in datetimes)
            {
                int num = data[datetime].Count;
                if (num < tickercount)
                {
                    data[datetime].Clear();
                    data.Remove(datetime);
                }
            }//foreach

            return data;
        }

        public List<List<double>> CalcGapsStat(string ticker)
        {
            Dictionary<uint, List<MyData.Bar>> daylyBars = new Dictionary<uint, List<MyData.Bar>>();
            List<MyData.Bar> Bars = Storage[ticker].Values.ToList();
            Dictionary<uint, double> daylyVola = new Dictionary<uint, double>();

            List<double> Day1Gaps = new List<double>();
            List<double> Day3Gaps = new List<double>();
            List<List<double>> Results = new List<List<double>>();

            // распихиваем всё по дневным наборам
            foreach (MyData.Bar bar in Bars)
            {
                DateTime dt = Tools.ConvertFromUnixTimestamp(bar.Time);
                dt = new DateTime(dt.Year, dt.Month, dt.Day);
                uint dayIndex = Tools.ConvertToUnixTimestamp(dt);
                if (!daylyBars.ContainsKey(dayIndex)) daylyBars.Add(dayIndex, new List<MyData.Bar>());
                daylyBars[dayIndex].Add(bar);
            }
            // считаем волу дневных наборов
            foreach (KeyValuePair<uint, List<MyData.Bar>> kvp in daylyBars)
            {
                double r2sum = CalcR2S(kvp.Value);
                daylyVola.Add(kvp.Key, r2sum);
            }

            int count = Bars.Count;
            for (int i = 1; i < count; i++)
            {
                DateTime dt1 = Tools.ConvertFromUnixTimestamp(Bars[i-1].Time);
                DateTime dt2 = Tools.ConvertFromUnixTimestamp(Bars[i].Time);
                int dayDiff = dt2.DayOfYear - dt1.DayOfYear;
                if (dayDiff>0)
                {
                    DateTime dt = new DateTime(dt1.Year, dt1.Month, dt1.Day);
                    uint dayIndex = Tools.ConvertToUnixTimestamp(dt);
                    double r2sum = (daylyVola.ContainsKey(dayIndex)) ? daylyVola[dayIndex] : 0;
                    if (r2sum == 0) continue;
                    string dayOfWeek = dt.DayOfWeek.ToString();
                    int intDayOfWeek = (int) dt.DayOfWeek;
                    if (!((intDayOfWeek > 0) && (intDayOfWeek < 6))) continue;
                    double gap = Math.Pow(Math.Log(Bars[i].Close / Bars[i - 1].Close), 2);
                    if ((dayDiff==1)||(dayDiff==3))
                        AddToLog(dt1.ToString("yyyy-MM-dd") + "; " + dayOfWeek + "; " + intDayOfWeek + "; " + dayDiff + "; " + r2sum.ToString("0.0000000000") + "; " + gap.ToString("0.0000000000"));
                    if (dayDiff == 1) Day1Gaps.Add(gap / r2sum);
                    if (dayDiff == 3) Day3Gaps.Add(gap / r2sum);
                }
            }
            Results.Add(Day1Gaps);
            Results.Add(Day3Gaps);
            return Results;
        }

        public double CalcR2S(List<MyData.Bar> bars, bool useFirstBar = false, bool useFirstDayBar = true, bool useBarGaps = false)
        {
            int count = bars.Count;
            int start = (useFirstBar) ? 0 : 1;
            double sum = 0;
            double lastprice = 0;
            for (int i = start; i < count; i++)
            {
                MyData.Bar bar = bars[i];

                if ((useBarGaps) && (lastprice > 0))
                {
                    sum += Math.Pow(Math.Log(bar.Open / lastprice), 2);
                    lastprice = bar.Close;
                }

                if ((!useFirstDayBar)&&(i > 0))
                {
                    MyData.Bar prevbar = bars[i];
                    DateTime dtbar = Tools.ConvertFromUnixTimestamp(bar.Time);
                    DateTime dtprevbar = Tools.ConvertFromUnixTimestamp(prevbar.Time);
                    if (dtbar.DayOfYear != dtprevbar.DayOfYear) continue;
                }
               
                sum += Math.Pow(Math.Log(bar.High / bar.Low), 2);
                double range = bar.High - bar.Low;
                if (range == 0) continue;
                double openRangeL = 1 - (bar.Open - bar.Low) / range;
                double openRangeH = 1 - (bar.High - bar.Open) / range;
                double closeRangeL = 1 - (bar.Close - bar.Low) / range;
                double closeRangeH = 1 - (bar.High - bar.Close) / range;
                sum += Math.Pow(Math.Log(bar.Open / bar.Low), 2) * openRangeL + Math.Pow(Math.Log(bar.High / bar.Open), 2) * openRangeH;
                sum += Math.Pow(Math.Log(bar.Close / bar.Low), 2) * closeRangeL + Math.Pow(Math.Log(bar.High / bar.Close), 2) * closeRangeH;
            }
            return sum * 0.295836321;
        }

        public static double CalcR2S(List<MyData.Bar> bars, int startIndex, int endIndex, bool useFirstBar = false, bool useFirstDayBar = true, bool useBarGaps = false)
        {
            //int count = bars.Count;
            int start = (useFirstBar) ? startIndex : startIndex+1;
            double sum = 0;
            double lastprice = 0;
            for (int i = start; i <= endIndex; i++)
            {
                MyData.Bar bar = bars[i];

                if ((!useFirstDayBar) && (i > 0))
                {
                    MyData.Bar prevbar = bars[i-1];
                    DateTime dtbar = Tools.ConvertFromUnixTimestamp(bar.Time);
                    DateTime dtprevbar = Tools.ConvertFromUnixTimestamp(prevbar.Time);
                    if (dtbar.DayOfYear != dtprevbar.DayOfYear) continue;
                }

                if ((useBarGaps) && (lastprice > 0)) sum += Math.Pow(Math.Log(bar.Open / lastprice), 2);
                lastprice = bar.Close;
                sum += Math.Pow(Math.Log(bar.High / bar.Low), 2);
                double range = bar.High - bar.Low;
                if (range == 0) continue;
                double openRangeL = 1 - (bar.Open - bar.Low) / range;
                double openRangeH = 1 - (bar.High - bar.Open) / range;
                double closeRangeL = 1 - (bar.Close - bar.Low) / range;
                double closeRangeH = 1 - (bar.High - bar.Close) / range;
                sum += Math.Pow(Math.Log(bar.Open / bar.Low), 2) * openRangeL + Math.Pow(Math.Log(bar.High / bar.Open), 2) * openRangeH;
                sum += Math.Pow(Math.Log(bar.Close / bar.Low), 2) * closeRangeL + Math.Pow(Math.Log(bar.High / bar.Close), 2) * closeRangeH;
                
            }
            return sum * 0.295836321;
        }
 
    }
}
