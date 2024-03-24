using MySql.Data.MySqlClient;
using Spectre.Console;

class Program
{
    static void Main(string[] args)
    {

        AnsiConsole.Write(new FigletText("MySQL Switcheroo").Color(Color.Yellow));
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("Bu araç, bir veritabanındaki tabloları ve verileri başka bir veritabanına aktarmanızı sağlar.");

        var table = new Table();
        table.AddColumn("Özellik");
        table.AddColumn("Açıklama");

        table.AddRow("Kaynak Veritabanı Bağlantısı", "Kaynak veritabanı için bağlantı bilgilerini toplar.");
        table.AddRow("Hedef Veritabanı Bağlantısı", "Hedef veritabanı için bağlantı bilgilerini toplar.");
        table.AddRow("Tablo Seçimi", "Aktarılacak tabloların seçimini yapar.");
        table.AddRow("Kolon Seçimi", "Seçilen tablolar için aktarılacak kolonları seçer.");
        table.AddRow("Veritabanı ve Tablo Kontrolü", "Kaynak ve hedef veritabanlarında gerekli kontrolleri yapar.");
        table.AddRow("Veri Aktarımı", "Seçilen tablo ve kolonlardaki verileri hedef veritabanına aktarır.");

        AnsiConsole.Write(table);


        var sourceConnectionInfo = PromptForConnectionInfo("Kaynak DB için bağlantı bilgileri:");
        var sourceConnectionString = BuildConnectionString(sourceConnectionInfo);

        var selectedTables = new List<string>();

        if (!CheckDatabaseExists(sourceConnectionString, sourceConnectionInfo["Database"]))
        {
            AnsiConsole.MarkupLine($"[red]Kaynak veritabanı '{sourceConnectionInfo["Database"]}' bulunamadı.[/]");
            return;
        }

        try
        {
            using (var sourceConnection = new MySqlConnection(sourceConnectionString))
            {
                ConnectToDatabase(sourceConnection);
                selectedTables = SelectTables(sourceConnection);
                SelectColumnsForTables(sourceConnection, selectedTables);
            }

            var targetConnectionInfo = PromptForConnectionInfo("\nHedef DB için bağlantı bilgileri:");
            var targetConnectionString = BuildConnectionString(targetConnectionInfo);

            if (!CheckDatabaseExists(targetConnectionString, targetConnectionInfo["Database"]))
            {
                AnsiConsole.MarkupLine($"[red]Hedef veritabanı '{targetConnectionInfo["Database"]}' bulunamadı.[/]");
                var createDb = AnsiConsole.Confirm("Hedef veritabanını oluşturmak ister misiniz?");
                if (createDb)
                {
                    try
                    {
                        CreateDatabase(targetConnectionString, targetConnectionInfo["Database"]);
                        AnsiConsole.MarkupLine($"[green]'{targetConnectionInfo["Database"]}' veritabanı başarıyla oluşturuldu.[/]");
                        using (var targetConnection = new MySqlConnection(targetConnectionString))
                        {
                            ConnectToDatabase(targetConnection);
                            CheckAndReportTablePresence(targetConnection, selectedTables, sourceConnectionString);
                        }
                    }
                    catch (Exception ex)
                    {
                        AnsiConsole.MarkupLine($"[red]Veritabanı oluşturma hatası: {ex.Message}[/]");
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
                    CheckAndReportTablePresence(targetConnection, selectedTables, sourceConnectionString);
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
            {"Port", AnsiConsole.Ask<string>("Port: ")},
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
        AnsiConsole.MarkupLine("[green]Bağlantı başarılı.[/]");
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
                .Title("Lütfen aktarmak istediğiniz tabloları seçin:")
                .PageSize(15)
                .AddChoices(tables));
    }

    static void SelectColumnsForTables(MySqlConnection connection, List<string> selectedTables)
    {
        foreach (var tableName in selectedTables)
        {
            AnsiConsole.MarkupLine($"[yellow]{tableName}[/] tablosu için kolonlar:");
            var columns = new List<string>();

            using (var columnCommand = new MySqlCommand($"SHOW COLUMNS FROM {tableName};", connection))
            using (var columnReader = columnCommand.ExecuteReader())
            {
                while (columnReader.Read())
                {
                    columns.Add(columnReader.GetString(0));
                }
            }

            var selectedColumnNames = AnsiConsole.Prompt(
                new MultiSelectionPrompt<string>()
                    .Title($"[green]{tableName}[/] tablosundan aktarmak istediğiniz kolonları seçin:")
                    .PageSize(15)
                    .AddChoices(columns));

            AnsiConsole.MarkupLine($"[green]{tableName}[/] tablosundan seçilen kolonlar: [yellow]{string.Join(", ", selectedColumnNames)}[/]");
        }
    }

    static bool CheckDatabaseExists(string connectionString, string databaseName)
    {
        try
        {
            using (var connection = new MySqlConnection(connectionString))
            {
                connection.Open();
                using (var cmd = new MySqlCommand($"SHOW DATABASES LIKE '{databaseName}';", connection))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        return reader.HasRows;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Veritabanı kontrolünde hata: {ex.Message}[/]");
            return false;
        }
    }

    static void CreateDatabase(string connectionString, string databaseName)
    {
        var builder = new MySqlConnectionStringBuilder(connectionString)
        {
            Database = ""
        };

        using (var connection = new MySqlConnection(builder.ConnectionString))
        {
            connection.Open();
            using (var cmd = new MySqlCommand($"CREATE DATABASE IF NOT EXISTS `{databaseName}`;", connection))
            {
                cmd.ExecuteNonQuery();
            }
        }
    }

    static void CreateTables(MySqlConnection targetConnection, List<string> missingTables, string sourceConnectionString)
    {
        foreach (var tableName in missingTables)
        {
            // Kaynak veritabanından tablonun CREATE TABLE komutunu almak için bağlantı açılır
            using (var sourceConnection = new MySqlConnection(sourceConnectionString))
            {
                sourceConnection.Open();
                var createTableCommandText = $"SHOW CREATE TABLE {tableName};";
                using (var cmd = new MySqlCommand(createTableCommandText, sourceConnection))
                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        var createTableScript = reader.GetString(1);
                        // Hedef veritabanında tabloyu oluştur
                        using (var targetCmd = new MySqlCommand(createTableScript, targetConnection))
                        {
                            targetCmd.ExecuteNonQuery();
                            AnsiConsole.MarkupLine($"[green]{tableName}[/] tablosu hedef veritabanında başarıyla oluşturuldu.");
                        }
                    }
                }
            }
        }
    }


    static void CheckAndReportTablePresence(MySqlConnection targetConnection, List<string> selectedTableNames, string sourceConnectionString)
    {
        var missingTables = new List<string>();

        targetConnection.Open();
        AnsiConsole.MarkupLine("[green]Hedef bağlantı başarılı.[/]");

        foreach (var tableName in selectedTableNames)
        {
            using (var cmd = new MySqlCommand($"SHOW TABLES LIKE '{tableName}';", targetConnection))
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    AnsiConsole.MarkupLine($"[green]{tableName}[/] tablosu hedef veritabanında mevcut.");
                }
                else
                {
                    missingTables.Add(tableName);
                    AnsiConsole.MarkupLine($"[red]{tableName}[/] tablosu hedef veritabanında bulunamadı.");
                }
            }
        }

        if (missingTables.Any())
        {
            var createTables = AnsiConsole.Confirm("Eksik tabloları hedef veritabanında oluşturmak ister misiniz?");
            if (createTables)
            {
                CreateTables(targetConnection, missingTables, sourceConnectionString);
            }
        }
    }

}

