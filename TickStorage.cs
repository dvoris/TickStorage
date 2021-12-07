using System.Data.SQLite;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace MyData
{
    public class TickStorage
    {
        public string FileName;

        public SQLiteConnection _connection;

        uint LastTickTime;

        public TickStorage()
        {
            bool isNewDb = false;
            string dbFileName = "livevol.db";
            FileName = "./" + dbFileName;
            if (!File.Exists(FileName)) { 
                SQLiteConnection.CreateFile(FileName);
                isNewDb = true;
            };

            SQLiteConnectionStringBuilder connBuilder = new SQLiteConnectionStringBuilder();
            connBuilder.DataSource = FileName;
            //Set page size to NTFS cluster size = 4096 bytes
            connBuilder.PageSize = 4096;
            connBuilder.JournalMode = SQLiteJournalModeEnum.Memory;
            connBuilder.Pooling = true;
            connBuilder.SyncMode = SynchronizationModes.Off;

            _connection = new SQLiteConnection(connBuilder.ToString() + "");
            _connection.Open();

            if (isNewDb)
            {
                CreateTables();
            }
        }

        public int[] GetLastTickData()
        {
            int[] results = { 0, 0 };
            using (SQLiteCommand cmd = _connection.CreateCommand())
            {
                cmd.CommandText = "SELECT MAX(Tstamp), COUNT(*) FROM 'ticks';";
                SQLiteDataReader r = cmd.ExecuteReader();
                while (r.Read())
                {
                    if (r[0].GetType() != typeof(DBNull))
                    {
                        results[0] = Convert.ToInt32(r[0]);
                        results[1] = Convert.ToInt32(r[1]);
                    }
                }

            }
            return results;
        }

        public void AddToArchive()
        {
            SQLiteCommand cmd = _connection.CreateCommand();

            cmd.CommandText = "ATTACH 'archive.db' AS ARCHIVE";
            int retval = 0;

            DateTime now = DateTime.Now;

            if (now.DayOfWeek == DayOfWeek.Monday)
            {
                now = now.AddDays(-5);
            } else
            {
                now = now.AddDays(-3);
            }
            

            uint keepFromStamp = Tools.ConvertToUnixTimestamp(now);
            try
            {
                retval = cmd.ExecuteNonQuery();
            } catch(Exception)
            {
                Console.WriteLine("Import error on attach");
                cmd.Dispose();
                _connection.Close();
                return;
            }

            cmd.CommandText = "INSERT OR IGNORE INTO ARCHIVE.ticks SELECT * FROM ticks";
            retval = 0;

            try
            {
                retval = cmd.ExecuteNonQuery();
            } catch
            {
                Console.WriteLine("Import error on paste");
                cmd.Dispose();
                _connection.Close();
                return;
            }

            if (retval > 0)
            {
                cmd.CommandText = "DELETE FROM ticks WHERE Tstamp < " + keepFromStamp;

                try
                {
                    retval = cmd.ExecuteNonQuery();
                }
                catch
                {
                    Console.WriteLine("Import error on delete with stamp "+ keepFromStamp);
                    cmd.Dispose();
                    _connection.Close();
                    return;
                }
                
                cmd.CommandText = "VACUUM;";

                try
                {
                    retval = cmd.ExecuteNonQuery();
                }
                catch
                {
                    Console.WriteLine("Import error on vacuum");
                    cmd.Dispose();
                    _connection.Close();
                    return;
                }
                finally
                {
                    cmd.Dispose();
                }

            } else
            {
                cmd.Dispose();
            }

            _connection.Close();
        }

        public void CreateTables()
        {
            try
            {
                string sql = "CREATE TABLE 'ticks' ('Oid' INTEGER UNIQUE NOT NULL PRIMARY KEY, 'Tstamp' INTEGER, 'Price' REAL, 'Code' TEXT);";
                new SQLiteCommand(sql, _connection).ExecuteNonQuery();
                sql = "CREATE INDEX 'code_idx' ON 'ticks'  ( 'Code' ASC);";
                new SQLiteCommand(sql, _connection).ExecuteNonQuery();

                sql = "CREATE INDEX 'tstamp_idx' ON 'ticks'  ('Tstamp' ASC);";
                new SQLiteCommand(sql, _connection).ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR SQLITE: " + ex.Message);
            }
        }

        public void StoreTick(Tick tick)
        {
            /*if (tick.Time < LastTickTime)
            {
                return;
            }*/
            
            string sql = "INSERT OR IGNORE INTO 'ticks' ('Oid', 'Tstamp', 'Code', 'Price') VALUES (@1, @2, @3, @4);";
            SQLiteCommand command = new SQLiteCommand(sql, _connection);
            command.Parameters.AddWithValue("@1", tick.Id);
            command.Parameters.AddWithValue("@2", tick.Time);
            command.Parameters.AddWithValue("@3", tick.Code.Substring(0,2));
            command.Parameters.AddWithValue("@4", tick.Price);
            command.ExecuteNonQuery();

                
        }
        
        public void Close()
        {
            if (_connection != null)
            {
                _connection.Close();
            }
        }

        public List<Tick> PrepareTicks(string ticker, DateTime from, DateTime till)
        {
            SortedList<ulong, Tick> tickList = new SortedList<ulong, Tick>();

            uint fromStamp = Tools.ConvertToUnixTimestamp(from);
            uint tillStamp = Tools.ConvertToUnixTimestamp(till);
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            using (SQLiteCommand cmd = _connection.CreateCommand())
            {
                cmd.CommandText = "SELECT * FROM 'ticks' WHERE Code='" + ticker + "' AND Tstamp >=" + fromStamp + " AND Tstamp <=" + tillStamp + " ORDER BY Oid ASC;";
                SQLiteDataReader r = cmd.ExecuteReader();
                while(r.Read())
                {
                    Tick t = new Tick();
                    t.Id = Convert.ToUInt64(r["Oid"]);
                    t.Time = Convert.ToUInt32(r["Tstamp"]);
                    t.Price = Convert.ToDouble(r["Price"]);
                    t.Code = ticker;
                    if (tickList.ContainsKey(t.Id)) {
                        tickList[t.Id] = t;
                    } else
                    {
                        tickList.Add(t.Id, t);
                    }
                }
            }
            stopwatch.Stop();
            Console.WriteLine ("Select query elapsed " + stopwatch.ElapsedMilliseconds + " milliseconds");



            return tickList.Values.ToList();
        }
    }
}
