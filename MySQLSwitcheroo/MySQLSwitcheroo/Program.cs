using MySql.Data.MySqlClient;
using Spectre.Console;

class Program
{
    static void Main(string[] args)
    {
        AnsiConsole.WriteLine("Kaynak DB için bağlantı bilgileri:");
        var sourceHost = AnsiConsole.Ask<string>("Kaynak Host: ");
        var sourceDatabase = AnsiConsole.Ask<string>("Kaynak Database: ");
        var sourcePort = AnsiConsole.Ask<string>("Kaynak Port: ");
        var sourceUsername = AnsiConsole.Ask<string>("Kaynak Username: ");
        var sourcePassword = AnsiConsole.Prompt(
            new TextPrompt<string>("Kaynak Password: ").Secret());

        string sourceConnectionString = $"Server={sourceHost}; database={sourceDatabase}; port={sourcePort}; User Id={sourceUsername}; password={sourcePassword};";

        List<string> selectedTableNames = [];

        try
        {
            using (var connection = new MySqlConnection(sourceConnectionString))
            {
                connection.Open();
                AnsiConsole.MarkupLine("[green]Bağlantı başarılı.[/]");

                var tables = new List<string>();

                // Tabloları al
                using (var command = new MySqlCommand("SHOW TABLES;", connection))
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        tables.Add(reader.GetString(0));
                    }
                }

                 selectedTableNames = AnsiConsole.Prompt(
                    new MultiSelectionPrompt<string>()
                        .Title("Lütfen aktarmak istediğiniz tabloları seçin:")
                        .PageSize(10)
                        .MoreChoicesText("[grey](Yukarı ve aşağı hareket ettirerek daha fazla tablo görebilirsiniz)[/]")
                        .InstructionsText("[grey](Space tuşu ile seçim yapın, Enter ile onaylayın)[/]")
                        .AddChoices(tables));

                foreach (var tableName in selectedTableNames)
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
                            .MoreChoicesText("[grey](Yukarı ve aşağı hareket ettirerek daha fazla kolon görebilirsiniz)[/]")
                            .InstructionsText("[grey](Space tuşu ile seçim yapın, Enter ile onaylayın)[/]")
                            .AddChoices(columns));

                    AnsiConsole.MarkupLine($"[green]{tableName}[/] tablosundan seçilen kolonlar: [yellow]{string.Join(", ", selectedColumnNames)}[/]");
                }
            }

            AnsiConsole.WriteLine("\nHedef DB için bağlantı bilgileri:");
            var targetHost = AnsiConsole.Ask<string>("Hedef Host: ");
            var targetDatabase = AnsiConsole.Ask<string>("Hedef Database: ");
            var targetPort = AnsiConsole.Ask<string>("Hedef Port: ");
            var targetUsername = AnsiConsole.Ask<string>("Hedef Username: ");
            var targetPassword = AnsiConsole.Prompt(
                new TextPrompt<string>("Hedef Password: ").Secret());

            string targetConnectionString = $"Server={targetHost}; database={targetDatabase}; port={targetPort}; User Id={targetUsername}; password={targetPassword};";

            using (var targetConnection = new MySqlConnection(targetConnectionString))
            {
                targetConnection.Open();
                AnsiConsole.MarkupLine("[green]Hedef bağlantı başarılı.[/]");

                bool dbExists = false;
                using (var cmd = new MySqlCommand($"SHOW DATABASES LIKE '{targetDatabase}';", targetConnection))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            dbExists = true;
                        }
                    }
                }

                if (!dbExists)
                {
                    AnsiConsole.MarkupLine($"[red]Hedef veritabanı '{targetDatabase}' bulunamadı.[/]");
                    return;
                }

                targetConnection.ChangeDatabase(targetDatabase);

                List<string> existingTables = new List<string>();
                using (var cmd = new MySqlCommand("SHOW TABLES;", targetConnection))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            existingTables.Add(reader.GetString(0));
                        }
                    }
                }

                foreach (var tableName in selectedTableNames)
                {
                    if (!existingTables.Contains(tableName))
                    {
                        AnsiConsole.MarkupLine($"[yellow]{tableName}[/] tablosu hedef veritabanında bulunamadı.");
                   
                    }
                    else
                    {
                        AnsiConsole.MarkupLine($"[green]{tableName}[/] tablosu hedef veritabanında mevcut.");
                       
                    }
                }
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]Bağlantı hatası: {ex.Message}[/]");
        }

        Console.Read();
    }
}
