using MySql.Data.MySqlClient;
using Spectre.Console;

class Program
{
    static async Task Main(string[] args)
    {

        AnsiConsole.Write(new FigletText("MySQL Switcheroo").Color(Color.Yellow));
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("This tool allows you to transfer tables and data from one database to another.");

        var table = new Table();
        table.AddColumn("Feature");
        table.AddColumn("Description");

        table.AddRow("Source Database Connection", "Collects connection information for the source database.");
        table.AddRow("Destination Database Connection", "Collects connection information for the destination database.");
        table.AddRow("Table Selection", "Allows selection of tables to be transferred.");
        table.AddRow("Column Selection", "Selects columns to be transferred for the chosen tables.");
        table.AddRow("Database and Table Validation", "Performs necessary validations on source and destination databases.");
        table.AddRow("Data Transfer", "Transfers data from selected tables and columns to the destination database.");

        AnsiConsole.Write(table);


        var sourceConnectionInfo = PromptForConnectionInfo("Connection information for Source DB:");
        var sourceConnectionString = BuildConnectionString(sourceConnectionInfo);

        var selectedTables = new List<string>();

        if (!(await CheckDatabaseExistsAsync(sourceConnectionString, sourceConnectionInfo["Database"])))
        {
            AnsiConsole.MarkupLine($"[red]Source database '{sourceConnectionInfo["Database"]}' not found.[/]");
            return;
        }

        try
        {
            using (var sourceConnection = new MySqlConnection(sourceConnectionString))
            {
                ConnectToDatabase(sourceConnection);
                selectedTables = SelectTables(sourceConnection);
                await SelectColumnsForTablesAsync(sourceConnection, selectedTables);
            }

            var targetConnectionInfo = PromptForConnectionInfo("\nConnection information for Destination DB:");
            var targetConnectionString = BuildConnectionString(targetConnectionInfo);

            if (!(await CheckDatabaseExistsAsync(targetConnectionString, targetConnectionInfo["Database"])))
            {
                AnsiConsole.MarkupLine($"[red]Destination database '{targetConnectionInfo["Database"]}' not found.[/]");
                var createDb = AnsiConsole.Confirm("Would you like to create the destination database?");

                if (createDb)
                {
                    try
                    {
                        await CreateDatabaseAsync(targetConnectionString, targetConnectionInfo["Database"]);
                        AnsiConsole.MarkupLine($"[green]'{targetConnectionInfo["Database"]}' database successfully created.[/]");

                        using (var targetConnection = new MySqlConnection(targetConnectionString))
                        {
                            ConnectToDatabase(targetConnection);
                            await CheckAndReportTablePresenceAsync(targetConnection, selectedTables, sourceConnectionString);
                        }
                    }
                    catch (Exception ex)
                    {
                        AnsiConsole.MarkupLine($"[red]Database creation error: {ex.Message}[/]");

                        return;
                    }
                }
                else
                {
                    return;
                }
            }
            else
            {
                using (var targetConnection = new MySqlConnection(targetConnectionString))
                {
                    ConnectToDatabase(targetConnection);
                    await CheckAndReportTablePresenceAsync(targetConnection, selectedTables, sourceConnectionString);
                }
            }


        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Hata: {ex.Message}[/]");
        }

        Console.Read();
    }

    static Dictionary<string, string> PromptForConnectionInfo(string message)
    {
        AnsiConsole.WriteLine(message);
        var info = new Dictionary<string, string>
        {
            {"Host", AnsiConsole.Ask<string>("Host: ")},
            {"Database", AnsiConsole.Ask<string>("Database: ")},
            {"Port", AnsiConsole.Ask<string>("Port: ","3306")},
            {"Username", AnsiConsole.Ask<string>("Username: ")},
            {"Password", AnsiConsole.Prompt(new TextPrompt<string>("Password: ").Secret())}
        };

        return info;
    }

    static string BuildConnectionString(Dictionary<string, string> connectionInfo)
    {
        return $"Server={connectionInfo["Host"]}; database={connectionInfo["Database"]}; port={connectionInfo["Port"]}; User Id={connectionInfo["Username"]}; password={connectionInfo["Password"]};";
    }

    static void ConnectToDatabase(MySqlConnection connection)
    {
        connection.Open();
        AnsiConsole.MarkupLine("[green]Connection successful.[/]");
    }

    static List<string> SelectTables(MySqlConnection connection)
    {
        var tables = new List<string>();
        using (var command = new MySqlCommand("SHOW TABLES;", connection))
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                tables.Add(reader.GetString(0));
            }
        }

        return AnsiConsole.Prompt(
             new MultiSelectionPrompt<string>()
                 .Title("Please select the tables you want to transfer:")
                 .Required()
                 .PageSize(15)
                 .AddChoices(tables));
    }

    static async Task SelectColumnsForTablesAsync(MySqlConnection connection, List<string> selectedTables)
    {
        foreach (var tableName in selectedTables)
        {
            AnsiConsole.MarkupLine($"[yellow]{tableName}[/] table columns:");
            var columns = new List<string>();

            await using (var columnCommand = new MySqlCommand($"SHOW COLUMNS FROM `{tableName}`;", connection))
            await using (var columnReader = await columnCommand.ExecuteReaderAsync())
            {
                while (await columnReader.ReadAsync())
                {
                    columns.Add(columnReader.GetString(0));
                }
            }

            var selectedColumnNames = AnsiConsole.Prompt(
                new MultiSelectionPrompt<string>()
                    .Title($"[green]{tableName}[/] table - select the columns you wish to transfer:")
                    .PageSize(15)
                    .Required()
                    .AddChoices(columns));

            AnsiConsole.MarkupLine($"[green]{tableName}[/] selected columns: [yellow]{string.Join(", ", selectedColumnNames)}[/]");
        }
    }

    static async ValueTask<bool> CheckDatabaseExistsAsync(string connectionString, string databaseName)
    {
        try
        {
            await using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await using (var cmd = new MySqlCommand($"SHOW DATABASES LIKE '{databaseName}';", connection))
                {
                    await using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        return await reader.ReadAsync();
                    }
                }
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Error checking database: {ex.Message}[/]");
            return false;
        }
    }

    static async Task CreateDatabaseAsync(string connectionString, string databaseName)
    {
        var builder = new MySqlConnectionStringBuilder(connectionString) { Database = "" };

        await using (var connection = new MySqlConnection(builder.ConnectionString))
        {
            await connection.OpenAsync();

            var query = $"CREATE DATABASE IF NOT EXISTS `{databaseName}`;";
            await using (var cmd = new MySqlCommand(query, connection))
            {
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }

    static async Task CreateTablesAsync(MySqlConnection targetConnection, List<string> missingTables, string sourceConnectionString)
    {
        foreach (var tableName in missingTables)
        {
            try
            {
                await using (var sourceConnection = new MySqlConnection(sourceConnectionString))
                {
                    await sourceConnection.OpenAsync();
                    var createTableCommandText = $"SHOW CREATE TABLE `{tableName}`;";
                    await using (var cmd = new MySqlCommand(createTableCommandText, sourceConnection))
                    await using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            var createTableScript = reader.GetString(1);
                            await using (var targetCmd = new MySqlCommand(createTableScript, targetConnection))
                            {
                                await targetCmd.ExecuteNonQueryAsync();
                                AnsiConsole.MarkupLine($"[green]{tableName}[/] table successfully created in the target database.");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]Failed to create {tableName} table in the target database: {ex.Message}[/]");
            }
        }
    }


    static async Task CheckAndReportTablePresenceAsync(MySqlConnection targetConnection, List<string> selectedTableNames, string sourceConnectionString)
    {
        var missingTables = new List<string>();

        await targetConnection.OpenAsync();
        AnsiConsole.MarkupLine("[green]Target connection successful.[/]");

        foreach (var tableName in selectedTableNames)
        {
            await using (var cmd = new MySqlCommand($"SHOW TABLES LIKE '{tableName}';", targetConnection))
            await using (var reader = await cmd.ExecuteReaderAsync())
            {
                if (await reader.ReadAsync())
                {
                    AnsiConsole.MarkupLine($"[green]{tableName}[/] table exists in the target database.");
                }
                else
                {
                    missingTables.Add(tableName);
                    AnsiConsole.MarkupLine($"[red]{tableName}[/] table does not exist in the target database.");
                }
            }
        }

        if (missingTables.Any())
        {
            var createTables = AnsiConsole.Confirm("Would you like to create the missing tables in the target database?");
            if (createTables)
            {
                await CreateTablesAsync(targetConnection, missingTables, sourceConnectionString);
            }
        }
    }

    static async Task TransferDataAsync(string sourceConnectionString, string targetConnectionString, List<string> selectedTables, Dictionary<string, List<string>> selectedColumns)
    {
        await using var sourceConnection = new MySqlConnection(sourceConnectionString);
        await sourceConnection.OpenAsync();

        await using var targetConnection = new MySqlConnection(targetConnectionString);
        await targetConnection.OpenAsync();

        foreach (var tableName in selectedTables)
        {
            var columnList = string.Join(", ", selectedColumns[tableName]);
            var query = $"SELECT {columnList} FROM {tableName};";

            await using var command = new MySqlCommand(query, sourceConnection);
            await using var reader = await command.ExecuteReaderAsync();

            var insertQuery = PrepareInsertQuery(tableName, selectedColumns[tableName]);
            await using var insertCommand = new MySqlCommand(insertQuery, targetConnection);

            await using var transaction = await targetConnection.BeginTransactionAsync();
            insertCommand.Transaction = transaction;

            while (await reader.ReadAsync())
            {
                for (int i = 0; i < selectedColumns[tableName].Count; i++)
                {
                    insertCommand.Parameters.AddWithValue($"@{selectedColumns[tableName][i]}", reader.GetValue(i));
                }

                await insertCommand.ExecuteNonQueryAsync();
                insertCommand.Parameters.Clear();
            }

            await transaction.CommitAsync();
        }
    }

    private static string PrepareInsertQuery(string tableName, List<string> columns)
    {
        var columnList = string.Join(", ", columns);
        var valuesList = "@" + string.Join(", @", columns);
        return $"INSERT INTO {tableName} ({columnList}) VALUES ({valuesList});";
    }
}

