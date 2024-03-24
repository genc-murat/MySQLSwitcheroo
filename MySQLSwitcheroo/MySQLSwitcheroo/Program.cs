using MySql.Data.MySqlClient;
using Spectre.Console;

class Program
{
    static void Main(string[] args)
    {
        var sourceConnectionInfo = PromptForConnectionInfo("Kaynak DB için bağlantı bilgileri:");
        var sourceConnectionString = BuildConnectionString(sourceConnectionInfo);

        var selectedTables = new List<string>();

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

            using (var targetConnection = new MySqlConnection(targetConnectionString))
            {
                ConnectToDatabase(targetConnection);
                CheckAndReportTablePresence(targetConnection, selectedTables);
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
                .PageSize(10)
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
                    .PageSize(10)
                    .AddChoices(columns));

            AnsiConsole.MarkupLine($"[green]{tableName}[/] tablosundan seçilen kolonlar: [yellow]{string.Join(", ", selectedColumnNames)}[/]");
        }
    }

    static void CheckAndReportTablePresence(MySqlConnection targetConnection, List<string> selectedTableNames)
    {
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
                    AnsiConsole.MarkupLine($"[red]{tableName}[/] tablosu hedef veritabanında bulunamadı.");
                }
            }
        }
    }
}

