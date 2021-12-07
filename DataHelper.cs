using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace MyData
{
    public class DataHelper
    {
        public DataHelper()
        {

        }
        
        public static void StoreTick(Tick tick)
        {
            SqliteFactory factory = (SqliteFactory)DbProviderFactories.GetFactory("System.Data.SQLite");
            using (SqliteConnection connection = (SqliteConnection)factory.CreateConnection())
            {
                connection.ConnectionString = "Data Source = ./livevol.db";
                connection.Open();
            };
        }
    }
}
