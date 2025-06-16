using Microsoft.Data.SqlClient;
using System.Text.RegularExpressions;

static string GetSqlConnection()
{
    var connectionString = "Server=127.0.0.1,2017;User Id=sa;Password=AMNDev@2017;TrustServerCertificate=True;Initial Catalog=ADM_SPAREPART_LIVE;Connection Timeout=3600;";
    return connectionString;
}

static bool ExecuteSQL(string sql)
{
    var connectionString = GetSqlConnection();
    using var sqlConnection = new SqlConnection(connectionString);
    sqlConnection.Open();
    using var command = new SqlCommand(sql, sqlConnection);
    command.CommandTimeout = 3600;
    var executionResult = command.ExecuteNonQuery();
    return executionResult >= 0;
}

static int GetRowCount(string tableName)
{
    var connectionString = GetSqlConnection();
    using var sqlConnection = new SqlConnection(connectionString);
    sqlConnection.Open();
    using var command = new SqlCommand($"SELECT COUNT(*) as counter FROM {tableName}", sqlConnection);
    command.CommandTimeout = 3600;
    var executionResult = command.ExecuteReader();
    var dataTable = new System.Data.DataTable();
    dataTable.Load(executionResult);
    if (dataTable.Rows.Count > 0)
    {
        return Convert.ToInt32(dataTable.Rows[0][0]);
    }

    return 0;
}

var files = Directory.GetFiles(Path.Combine(Directory.GetCurrentDirectory(), "sql"), "*.sql", SearchOption.AllDirectories);
var successFileDir = Path.Combine(Directory.GetCurrentDirectory(), "success");
if (!Directory.Exists(successFileDir))
{
    Directory.CreateDirectory(successFileDir);
}

while (files.Count() > 0)
{
    foreach (var file in files)
    {
        Console.WriteLine($"Executing SQL file: {file}");
        try
        {
            var sql = File.ReadAllText(file);
            var regexUse = new Regex(@"^\s*(?:USE)\s+\[(\S+)\]\s*$", RegexOptions.IgnoreCase | RegexOptions.Multiline);
            var regexGo = new Regex(@"^\s*GO\s*$", RegexOptions.IgnoreCase | RegexOptions.Multiline);
            var regexTableName = new Regex(@"\[dbo\]\.\[\S+\]+", RegexOptions.IgnoreCase | RegexOptions.Multiline);
            var regexMatch = regexTableName.Matches(sql);
            var tableName = string.Empty;
            if (regexTableName.IsMatch(sql))
            {
                try
                {
                    tableName = regexMatch[0].Value;
                    Console.WriteLine($"Table name found: {tableName}");
                    ExecuteSQL("DELETE FROM " + tableName);
                }
                catch
                {
                    Console.WriteLine("Failed to delete data from the table.");
                }
            }

            while (regexUse.IsMatch(sql))
            {
                sql = regexUse.Replace(sql, string.Empty, 1);
            }

            while (regexGo.IsMatch(sql))
            {
                sql = regexGo.Replace(sql, string.Empty, 1);
            }

            sql = sql.Trim();

            var sqlSplits = sql.Split("INSERT [dbo].", StringSplitOptions.RemoveEmptyEntries)
                .Select(s => "INSERT [dbo]." + s.Trim() + ";")
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .ToList();

            var sqlList = new List<string>();
            var index = 0;
            var maxExecutionRow = 1000; // seconds
            foreach (var sqlRow in sqlSplits)
            {
                if (sqlList.Count >= maxExecutionRow)
                {
                    Console.WriteLine($"Executing batch index {tableName}: {index - maxExecutionRow} until {index} SQL commands.");
                    var sqlBatch = string.Join(" ", sqlList);
                    var executionResult = ExecuteSQL(sqlBatch);
                    sqlList.Clear();
                }

                sqlList.Add(sqlRow);
                index++;
            }

            if (sqlList.Count > 0)
            {
                Console.WriteLine($"Executing batch index {tableName}: {index - sqlList.Count} until {index} SQL commands.");
                var sqlBatch = string.Join(" ", sqlList);
                var executionResult = ExecuteSQL(sqlBatch);
                sqlList.Clear();
            }

            var totalRowCount = GetRowCount(tableName);
            Console.WriteLine($"Total rows in {tableName}: {totalRowCount}");
            Console.WriteLine($"Expected Row in {tableName}: {sqlSplits.Count}.");
            if (totalRowCount == sqlSplits.Count)
            {
                Console.WriteLine($"SQL file {file} executed successfully.");
                var targetOutputFile = Path.Combine(successFileDir, Path.GetFileName(file));
                File.Move(file, targetOutputFile, true);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error executing SQL file {file}: {ex.Message}");
            continue;
        }
    }

    Console.WriteLine($"Execution completed.");
}